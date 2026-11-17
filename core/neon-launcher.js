const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');

const dataDir = path.resolve(process.env.FARM_DATA_DIR || path.join(__dirname, 'data'));
const databaseUrl = process.env.NEON_DATABASE_URL || process.env.DATABASE_URL;
const ignoredTopLevelDirs = new Set(['logs']);
const retryDelayMs = 5000;
let pool;
let child;
let watcher;
let syncTimer;
let retryTimer;
let fallbackInterval;
let syncInProgress = false;
let syncRequested = false;
let stopping = false;
let knownHashes = new Map();

function log(message, error) {
    const suffix = error ? `: ${error.message || String(error)}` : '';
    console.log(`[neon-file-sync] ${message}${suffix}`);
}

function isManagedRelativePath(relativePath) {
    if (typeof relativePath !== 'string' || !relativePath || relativePath.includes('\\')) return false;
    const parts = relativePath.split('/');
    if (parts.some(part => !part || part === '.' || part === '..')) return false;
    if (ignoredTopLevelDirs.has(parts[0])) return false;
    if (parts.at(-1).endsWith('.tmp')) return false;
    return true;
}

function toRelativePath(filePath) {
    const relativePath = path.relative(dataDir, filePath).split(path.sep).join('/');
    return isManagedRelativePath(relativePath) ? relativePath : null;
}

function walkFiles(dir, files = []) {
    let entries;
    try {
        entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
        return files;
    }

    for (const entry of entries) {
        const targetPath = path.join(dir, entry.name);
        if (entry.isSymbolicLink()) continue;
        if (entry.isDirectory()) {
            walkFiles(targetPath, files);
            continue;
        }
        if (!entry.isFile() || !toRelativePath(targetPath)) continue;
        files.push(targetPath);
    }
    return files;
}

function resolveStoredPath(relativePath) {
    if (!isManagedRelativePath(relativePath)) return null;
    const resolved = path.resolve(dataDir, ...relativePath.split('/'));
    return resolved.startsWith(dataDir + path.sep) ? resolved : null;
}

function writeRestoredFile(relativePath, content) {
    const targetPath = resolveStoredPath(relativePath);
    if (!targetPath) return false;
    fs.mkdirSync(path.dirname(targetPath), { recursive: true, mode: 0o700 });
    const temporaryPath = `${targetPath}.${process.pid}.restore.tmp`;
    fs.writeFileSync(temporaryPath, content, { mode: 0o600 });
    fs.renameSync(temporaryPath, targetPath);
    return true;
}

async function getPool() {
    if (!databaseUrl) throw new Error('NEON_DATABASE_URL is required for Render deployment');
    if (!pool) {
        const { Pool } = require('/app/neon-deps/node_modules/pg');
        pool = new Pool({
            connectionString: databaseUrl,
            max: 1,
            idleTimeoutMillis: 10000,
            connectionTimeoutMillis: 10000,
            application_name: 'qq-farm-file-sync'
        });
    }
    return pool;
}

async function ensureTable() {
    const db = await getPool();
    await db.query(`
        CREATE TABLE IF NOT EXISTS qq_farm_runtime_files (
            relative_path text PRIMARY KEY,
            content bytea NOT NULL,
            sha256 text NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
    `);
}

async function restoreFromDatabase() {
    fs.mkdirSync(dataDir, { recursive: true, mode: 0o700 });
    await ensureTable();
    const db = await getPool();
    const result = await db.query(
        'SELECT relative_path, content, sha256 FROM qq_farm_runtime_files'
    );

    if (result.rows.length === 0) {
        log('No saved runtime files found; the first file scan will initialize Neon.');
        return;
    }

    for (const row of result.rows) {
        const relativePath = row.relative_path;
        if (!isManagedRelativePath(relativePath)) {
            log(`Skipped invalid stored path: ${relativePath}`);
            continue;
        }
        const content = Buffer.from(row.content);
        writeRestoredFile(relativePath, content);
        knownHashes.set(relativePath, row.sha256 || crypto.createHash('sha256').update(content).digest('hex'));
    }
    log(`Restored ${knownHashes.size} runtime files from Neon.`);
}

async function syncSnapshot() {
    if (syncInProgress) {
        syncRequested = true;
        return;
    }
    syncInProgress = true;
    try {
        const db = await getPool();
        const currentPaths = new Set();
        const changedRows = [];

        for (const filePath of walkFiles(dataDir)) {
            const relativePath = toRelativePath(filePath);
            if (!relativePath) continue;
            currentPaths.add(relativePath);
            let content;
            try {
                content = fs.readFileSync(filePath);
            } catch (error) {
                if (error.code !== 'ENOENT') log(`Could not read ${relativePath}`, error);
                continue;
            }
            const sha256 = crypto.createHash('sha256').update(content).digest('hex');
            if (knownHashes.get(relativePath) === sha256) continue;
            changedRows.push({ relativePath, content, sha256 });
        }

        if (changedRows.length > 0) {
            await db.query('BEGIN');
            try {
                for (const row of changedRows) {
                    await db.query(
                        `INSERT INTO qq_farm_runtime_files (relative_path, content, sha256, updated_at)
                         VALUES ($1, $2, $3, now())
                         ON CONFLICT (relative_path) DO UPDATE SET
                           content = EXCLUDED.content,
                           sha256 = EXCLUDED.sha256,
                           updated_at = now()`,
                        [row.relativePath, row.content, row.sha256]
                    );
                }
                await db.query('COMMIT');
                for (const row of changedRows) knownHashes.set(row.relativePath, row.sha256);
                log(`Saved ${changedRows.length} changed runtime file(s) to Neon.`);
            } catch (error) {
                await db.query('ROLLBACK').catch(() => {});
                throw error;
            }
        }

        const removedPaths = [...knownHashes.keys()].filter(relativePath => !currentPaths.has(relativePath));
        if (removedPaths.length > 0) {
            await db.query(
                'DELETE FROM qq_farm_runtime_files WHERE relative_path = ANY($1::text[])',
                [removedPaths]
            );
            for (const relativePath of removedPaths) knownHashes.delete(relativePath);
            log(`Removed ${removedPaths.length} deleted runtime file(s) from Neon.`);
        }
    } catch (error) {
        log('Runtime file sync failed; changes will be retried.', error);
        if (!stopping && !retryTimer) {
            retryTimer = setTimeout(() => {
                retryTimer = null;
                scheduleSync(0);
            }, retryDelayMs);
        }
    } finally {
        syncInProgress = false;
        if (syncRequested) {
            syncRequested = false;
            if (!stopping) scheduleSync(0);
        }
    }
}

function scheduleSync(delay = 250) {
    if (stopping) return;
    clearTimeout(syncTimer);
    syncTimer = setTimeout(() => {
        syncTimer = null;
        void syncSnapshot();
    }, delay);
}

async function waitForActiveSync() {
    while (syncInProgress) {
        await new Promise(resolve => setTimeout(resolve, 25));
    }
}

function waitForChildExit(timeoutMs = 12000) {
    if (!child || child.exitCode !== null) return Promise.resolve();
    return new Promise(resolve => {
        const timeout = setTimeout(() => {
            if (child && child.exitCode === null) child.kill('SIGKILL');
            resolve();
        }, timeoutMs);
        child.once('exit', () => {
            clearTimeout(timeout);
            resolve();
        });
    });
}

async function shutdown(signal, exitCode = 0) {
    if (stopping) return;
    stopping = true;
    clearTimeout(syncTimer);
    clearTimeout(retryTimer);
    if (watcher) watcher.close();
    if (fallbackInterval) clearInterval(fallbackInterval);
    log(`Received ${signal}; stopping the application and saving runtime data.`);
    if (child && child.exitCode === null) child.kill(signal);
    await waitForChildExit();
    await waitForActiveSync();
    await syncSnapshot();
    await waitForActiveSync();
    if (pool) await pool.end().catch(error => log('Database pool close failed', error));
    process.exit(exitCode);
}

async function main() {
    await restoreFromDatabase();

    try {
        watcher = fs.watch(dataDir, { recursive: true }, () => scheduleSync());
        watcher.on('error', error => {
            log('Filesystem watcher failed; periodic scanning is enabled.', error);
            if (!fallbackInterval) fallbackInterval = setInterval(() => scheduleSync(0), 10000);
        });
    } catch (error) {
        log('Filesystem watcher unavailable; periodic scanning is enabled.', error);
        fallbackInterval = setInterval(() => scheduleSync(0), 10000);
    }

    await syncSnapshot();
    child = spawn(process.execPath, [path.join(__dirname, 'client.js')], {
        cwd: __dirname,
        env: process.env,
        stdio: 'inherit'
    });
    child.on('error', error => {
        log('Could not start the application process', error);
        void shutdown('SIGTERM', 1);
    });
    child.on('exit', (code, signal) => {
        if (!stopping) void shutdown(signal || 'exit', code || 0);
    });
    process.on('SIGTERM', () => void shutdown('SIGTERM'));
    process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch(async error => {
    log('Startup failed', error);
    if (pool) await pool.end().catch(() => {});
    process.exit(1);
});
