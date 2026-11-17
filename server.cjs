const express = require('express');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const os = require('os');
const pino = require('pino');
const crypto = require('crypto');
const Database = require('better-sqlite3');
const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    delay
} = require('@whiskeysockets/baileys');

const app = express();
const PORT = process.env.PORT || 3000;
const API_SECRET_TOKEN = process.env.API_SECRET_TOKEN || 'marketing_engine_secret_2026';
const ADMIN_MASTER_PIN = process.env.ADMIN_MASTER_PIN || 'adminpass2026';

const BASE_TENANTS_DIR = path.join(__dirname, 'tenants');
const LICENSES_FILE = path.join(__dirname, 'admin_licenses.json');
const DB_FILE = path.join(__dirname, 'engine_data.sqlite');

if (!fs.existsSync(BASE_TENANTS_DIR)) fs.mkdirSync(BASE_TENANTS_DIR, { recursive: true });

// Zero-Crash Process Guard
process.on('uncaughtException', (err) => {
    console.error('[CRITICAL UNCAUGHT EXCEPTION]:', err);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error('[CRITICAL UNHANDLED REJECTION]:', reason);
});

// In-Memory Brute-Force Rate Limiter for Master Admin
const adminLoginAttempts = new Map();
function checkAdminBruteForce(clientIp) {
    const record = adminLoginAttempts.get(clientIp);
    if (!record) return true;
    if (Date.now() < record.lockedUntil) return false;
    if (Date.now() - record.firstAttempt > 15 * 60 * 1000) {
        adminLoginAttempts.delete(clientIp);
        return true;
    }
    return record.count < 5;
}
function recordFailedAdminAttempt(clientIp) {
    const record = adminLoginAttempts.get(clientIp) || { count: 0, firstAttempt: Date.now(), lockedUntil: 0 };
    record.count++;
    if (record.count >= 5) {
        record.lockedUntil = Date.now() + 15 * 60 * 1000;
    }
    adminLoginAttempts.set(clientIp, record);
}
function clearAdminAttempt(clientIp) {
    adminLoginAttempts.delete(clientIp);
}

// ============================================================================
// ULTRA-FAST SQLITE DATABASE INITIALIZATION (WAL MODE)
// ============================================================================
const db = new Database(DB_FILE);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

db.exec(`
    CREATE TABLE IF NOT EXISTS licenses (
        sessionId TEXT PRIMARY KEY,
        phone TEXT,
        status TEXT,
        plan TEXT,
        tenantToken TEXT,
        validUntil INTEGER,
        notes TEXT,
        createdAt INTEGER,
        purchasedAt INTEGER
    );

    CREATE TABLE IF NOT EXISTS customer_groups (
        sessionId TEXT PRIMARY KEY,
        groupsJson TEXT,
        updatedAt INTEGER
    );

    CREATE TABLE IF NOT EXISTS welcome_rules (
        sessionId TEXT PRIMARY KEY,
        configJson TEXT,
        updatedAt INTEGER
    );

    CREATE TABLE IF NOT EXISTS scheduled_jobs (
        jobId TEXT PRIMARY KEY,
        sessionId TEXT,
        status TEXT,
        scheduledForEpoch INTEGER,
        jobDataJson TEXT
    );

    CREATE TABLE IF NOT EXISTS campaign_history (
        campaignId TEXT PRIMARY KEY,
        sessionId TEXT,
        timestamp TEXT,
        dataJson TEXT,
        createdAt INTEGER
    );

    CREATE TABLE IF NOT EXISTS global_settings (
        key TEXT PRIMARY KEY,
        value TEXT,
        updatedAt INTEGER
    );
`);

// Safe Zero-Loss Schema Migration
try { db.exec('ALTER TABLE licenses ADD COLUMN createdAt INTEGER;'); } catch (e) {}
try { db.exec('ALTER TABLE licenses ADD COLUMN purchasedAt INTEGER;'); } catch (e) {}

// Prepared Statements for Sub-millisecond Execution
const stmtGetLicense = db.prepare('SELECT * FROM licenses WHERE sessionId = ?');
const stmtGetAllLicensesSorted = db.prepare('SELECT * FROM licenses ORDER BY COALESCE(createdAt, validUntil) DESC');
const stmtUpsertLicense = db.prepare(`
    INSERT INTO licenses (sessionId, phone, status, plan, tenantToken, validUntil, notes, createdAt, purchasedAt)
    VALUES (@sessionId, @phone, @status, @plan, @tenantToken, @validUntil, @notes, @createdAt, @purchasedAt)
    ON CONFLICT(sessionId) DO UPDATE SET
        phone = excluded.phone,
        status = excluded.status,
        plan = excluded.plan,
        tenantToken = excluded.tenantToken,
        validUntil = excluded.validUntil,
        notes = excluded.notes,
        createdAt = COALESCE(licenses.createdAt, excluded.createdAt),
        purchasedAt = COALESCE(excluded.purchasedAt, licenses.purchasedAt)
`);
const stmtDeleteLicense = db.prepare('DELETE FROM licenses WHERE sessionId = ?');

const stmtGetGroups = db.prepare('SELECT groupsJson FROM customer_groups WHERE sessionId = ?');
const stmtSaveGroups = db.prepare(`
    INSERT INTO customer_groups (sessionId, groupsJson, updatedAt)
    VALUES (?, ?, ?)
    ON CONFLICT(sessionId) DO UPDATE SET
        groupsJson = excluded.groupsJson,
        updatedAt = excluded.updatedAt
`);

const stmtGetWelcome = db.prepare('SELECT configJson FROM welcome_rules WHERE sessionId = ?');
const stmtSaveWelcome = db.prepare(`
    INSERT INTO welcome_rules (sessionId, configJson, updatedAt)
    VALUES (?, ?, ?)
    ON CONFLICT(sessionId) DO UPDATE SET
        configJson = excluded.configJson,
        updatedAt = excluded.updatedAt
`);

const stmtGetScheduledJobs = db.prepare('SELECT * FROM scheduled_jobs WHERE sessionId = ?');
const stmtGetAllDueJobs = db.prepare('SELECT * FROM scheduled_jobs WHERE status = ? AND scheduledForEpoch <= ?');
const stmtUpsertScheduledJob = db.prepare(`
    INSERT INTO scheduled_jobs (jobId, sessionId, status, scheduledForEpoch, jobDataJson)
    VALUES (@jobId, @sessionId, @status, @scheduledForEpoch, @jobDataJson)
    ON CONFLICT(jobId) DO UPDATE SET
        status = excluded.status,
        jobDataJson = excluded.jobDataJson
`);
const stmtDeleteScheduledJob = db.prepare('DELETE FROM scheduled_jobs WHERE jobId = ?');
const stmtDeleteAllScheduledJobsForSession = db.prepare('DELETE FROM scheduled_jobs WHERE sessionId = ?');

const stmtGetCampaignHistory = db.prepare('SELECT dataJson FROM campaign_history WHERE sessionId = ? ORDER BY createdAt DESC LIMIT 3');
const stmtUpsertCampaignHistory = db.prepare(`
    INSERT INTO campaign_history (campaignId, sessionId, timestamp, dataJson, createdAt)
    VALUES (@campaignId, @sessionId, @timestamp, @dataJson, @createdAt)
    ON CONFLICT(campaignId) DO UPDATE SET
        dataJson = excluded.dataJson
`);

const stmtGetSetting = db.prepare('SELECT value FROM global_settings WHERE key = ?');
const stmtSaveSetting = db.prepare(`
    INSERT INTO global_settings (key, value, updatedAt)
    VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET
        value = excluded.value,
        updatedAt = excluded.updatedAt
`);

// ============================================================================
// TENANT DISK PATHS & ATOMIC FILE RECOVERY
// ============================================================================
function getTenantPaths(tenantId) {
    const cleanTenant = String(tenantId || 'unpaired_user').replace(/[^a-zA-Z0-9_-]/g, '_');
    const tenantDir = path.join(BASE_TENANTS_DIR, `tenant_${cleanTenant}`);
    const sessionDir = path.join(tenantDir, 'session');
    const mediaVaultDir = path.join(tenantDir, 'media_vault');
    const logsFile = path.join(tenantDir, 'delivery_logs.json');
    const campaignStateFile = path.join(tenantDir, 'active_campaign_state.json');
    const campaignHistoryFile = path.join(tenantDir, 'campaign_history.json');
    const groupsBackupFile = path.join(tenantDir, 'customer_groups.json');
    const welcomeRulesFile = path.join(tenantDir, 'welcome_rules.json');
    const scheduledJobsFile = path.join(tenantDir, 'scheduled_jobs.json');

    if (!fs.existsSync(sessionDir)) fs.mkdirSync(sessionDir, { recursive: true });
    if (!fs.existsSync(mediaVaultDir)) fs.mkdirSync(mediaVaultDir, { recursive: true });

    return {
        cleanTenant,
        tenantDir,
        sessionDir,
        mediaVaultDir,
        logsFile,
        campaignStateFile,
        campaignHistoryFile,
        groupsBackupFile,
        welcomeRulesFile,
        scheduledJobsFile
    };
}

function safeReadJsonSync(filePath, fallbackValue) {
    if (!fs.existsSync(filePath)) return fallbackValue;
    try {
        const raw = fs.readFileSync(filePath, 'utf8');
        if (!raw || raw.trim() === '') return fallbackValue;
        return JSON.parse(raw);
    } catch (e) {
        return fallbackValue;
    }
}

function safeWriteJsonSync(filePath, data) {
    const tempPath = filePath + '.tmp';
    try {
        fs.writeFileSync(tempPath, JSON.stringify(data, null, 2), 'utf8');
        fs.renameSync(tempPath, filePath);
    } catch (e) {
        if (fs.existsSync(tempPath)) {
            try { fs.unlinkSync(tempPath); } catch (err) {}
        }
    }
}

// Seamless Auto-Migration from Legacy Files with One-Time Permanent Guard
function migrateLegacyJsonFiles() {
    try {
        const migrationCheck = stmtGetSetting.get('legacy_json_migrated');
        if (migrationCheck && migrationCheck.value === 'true') {
            return; // Migration already complete. Never re-import old JSON duplicates on restart!
        }

        if (fs.existsSync(LICENSES_FILE)) {
            const raw = fs.readFileSync(LICENSES_FILE, 'utf8');
            if (raw.trim()) {
                const licObj = JSON.parse(raw);
                const insertManyLic = db.transaction((licenses) => {
                    for (const [sid, item] of Object.entries(licenses)) {
                        const nowEpoch = Date.now();
                        stmtUpsertLicense.run({
                            sessionId: sid,
                            phone: item.phone || sid,
                            status: item.status || 'ACTIVE',
                            plan: item.plan || 'Monthly (₹150)',
                            tenantToken: item.tenantToken || crypto.randomBytes(16).toString('hex'),
                            validUntil: item.validUntil || (nowEpoch + 30 * 24 * 60 * 60 * 1000),
                            notes: item.notes || 'Auto-Migrated',
                            createdAt: item.createdAt || nowEpoch,
                            purchasedAt: item.purchasedAt || nowEpoch
                        });
                    }
                });
                insertManyLic(licObj);
            }
        }

        if (fs.existsSync(BASE_TENANTS_DIR)) {
            const tenantFolders = fs.readdirSync(BASE_TENANTS_DIR);
            for (const d of tenantFolders) {
                if (d.startsWith('tenant_')) {
                    const sid = d.replace('tenant_', '');
                    const tenantDir = path.join(BASE_TENANTS_DIR, d);
                    
                    const grpFile = path.join(tenantDir, 'customer_groups.json');
                    if (fs.existsSync(grpFile)) {
                        const rawGrp = fs.readFileSync(grpFile, 'utf8');
                        if (rawGrp && rawGrp.trim() !== '') {
                            stmtSaveGroups.run(sid, rawGrp, Date.now());
                        }
                    }

                    const histFile = path.join(tenantDir, 'campaign_history.json');
                    if (fs.existsSync(histFile)) {
                        const rawHist = fs.readFileSync(histFile, 'utf8');
                        if (rawHist && rawHist.trim() !== '') {
                            try {
                                const parsedHist = JSON.parse(rawHist);
                                if (Array.isArray(parsedHist)) {
                                    parsedHist.forEach(item => {
                                        stmtUpsertCampaignHistory.run({
                                            campaignId: item.id || ('mig_' + Date.now()),
                                            sessionId: sid,
                                            timestamp: item.timestamp || new Date().toLocaleString(),
                                            dataJson: JSON.stringify(item),
                                            createdAt: Date.now()
                                        });
                                    });
                                }
                            } catch (e) {}
                        }
                    }

                    const welFile = path.join(tenantDir, 'welcome_rules.json');
                    if (fs.existsSync(welFile)) {
                        const rawWel = fs.readFileSync(welFile, 'utf8');
                        if (rawWel && rawWel.trim() !== '') {
                            stmtSaveWelcome.run(sid, rawWel, Date.now());
                        }
                    }
                }
            }
        }
        stmtSaveSetting.run('legacy_json_migrated', 'true', Date.now());
        console.log('[AUTO-MIGRATION SUCCESS] Synchronized to SQLite and permanently flagged.');
    } catch (e) {
        console.error('[MIGRATION ERROR]', e);
    }
}
migrateLegacyJsonFiles();

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const tenantId = req.body?.sessionId || req.query?.sessionId || 'unpaired_user';
        const { mediaVaultDir } = getTenantPaths(tenantId);
        cb(null, mediaVaultDir);
    },
    filename: (req, file, cb) => {
        const ext = path.extname(file.originalname).toLowerCase() || '';
        cb(null, `vault_${Date.now()}_${crypto.randomBytes(4).toString('hex')}${ext}`);
    }
});

const upload = multer({
    storage,
    limits: { fileSize: 15 * 1024 * 1024, files: 12 },
    fileFilter: (req, file, cb) => {
        const allowedMimes = ['image/jpeg', 'image/png', 'image/webp', 'video/mp4', 'application/pdf'];
        const ext = path.extname(file.originalname).toLowerCase();
        const allowedExts = ['.jpg', '.jpeg', '.png', '.webp', '.mp4', '.pdf'];
        if (allowedMimes.includes(file.mimetype) && allowedExts.includes(ext)) {
            cb(null, true);
        } else {
            cb(new Error('INVALID_FILE_TYPE'));
        }
    }
});

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'www')));

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization, ngrok-skip-browser-warning');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

const authenticate = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    let token = null;

    if (!authHeader) {
        if (req.query.token) token = req.query.token;
    } else {
        token = authHeader.replace(/^Bearer\s+/i, '').trim();
    }

    const sessionId = req.body?.sessionId || req.params?.sessionId || req.query?.sessionId || 'unpaired_user';
    const lic = stmtGetLicense.get(sessionId);

    const isMasterToken = (token === API_SECRET_TOKEN || token === 'marketing_engine_secret_2026');
    const isTenantToken = lic && lic.tenantToken && token === lic.tenantToken;

    if (!isMasterToken && !isTenantToken) {
        return res.status(401).json({ error: 'Unauthorized: Invalid token.' });
    }

    if (lic) {
        if (lic.status !== 'ACTIVE') {
            return res.status(403).json({ error: 'License Revoked: Remote Kill-Switch activated.' });
        }
        if (Date.now() > lic.validUntil) {
            return res.status(403).json({ error: 'License Expired: Please renew subscription.' });
        }
    }

    next();
};

const sessions = new Map();
const welcomeCooldownCache = new Map();

setInterval(() => {
    const now = Date.now();
    for (const [key, timestamp] of welcomeCooldownCache.entries()) {
        if (now - timestamp > 24 * 60 * 60 * 1000) {
            welcomeCooldownCache.delete(key);
        }
    }
}, 60 * 60 * 1000);

function sanitizeToValidWhatsAppJid(rawInput) {
    let clean = String(rawInput || '').replace(/[^0-9]/g, '');
    if (!clean) return null;
    if (clean.length === 11 && clean.startsWith('0')) clean = clean.substring(1);
    if (clean.length === 10) clean = '91' + clean;
    if (clean.length < 10 || clean.length > 15) return null;
    return clean;
}

function injectZeroWidthRandomizers(text) {
    if (!text || typeof text !== 'string') return text || '';
    const lines = text.split('\n');
    const randomizedLines = lines.map(line => {
        const words = line.split(' ');
        const randomizedWords = words.map(word => {
            const hasProtectedPattern = /[0-9+\@]|http|www|\.com/i.test(word);
            if (!hasProtectedPattern && Math.random() > 0.45) {
                const padCount = Math.floor(Math.random() * 2) + 1;
                return word + '\u200B'.repeat(padCount);
            }
            return word;
        });
        const tailPad = '\u200B'.repeat(Math.floor(Math.random() * 3) + 1);
        return randomizedWords.join(' ') + tailPad;
    });
    return randomizedLines.join('\n');
}

function buildRandomizedMediaBuffer(filePath, mimeType) {
    const rawBuffer = fs.readFileSync(filePath);
    if (mimeType && mimeType.startsWith('image/')) {
        const randomByteLength = Math.floor(Math.random() * 9) + 4;
        const randomTail = crypto.randomBytes(randomByteLength);
        return Buffer.concat([rawBuffer, randomTail]);
    }
    return rawBuffer;
}

function purgeCampaignMediaItems(mediaItems) {
    if (!Array.isArray(mediaItems)) return;
    mediaItems.forEach(item => {
        const targetPath = item?.path || item?.filePath;
        if (targetPath && fs.existsSync(targetPath)) {
            try { fs.unlinkSync(targetPath); } catch (e) {}
        }
    });
}

function cleanOrphanedMediaVaultFiles(tenantId) {
    try {
        const { mediaVaultDir, campaignStateFile } = getTenantPaths(tenantId);
        if (!fs.existsSync(mediaVaultDir)) return;

        const protectedFiles = new Set();
        const resolvedVault = path.resolve(mediaVaultDir);

        const protectPath = (p) => {
            if (!p) return;
            const resolved = path.resolve(p);
            if (resolved.startsWith(resolvedVault)) {
                protectedFiles.add(resolved);
            }
        };

        const welcomeRow = stmtGetWelcome.get(tenantId);
        if (welcomeRow && welcomeRow.configJson) {
            try {
                const rules = JSON.parse(welcomeRow.configJson);
                (rules.sequence || []).forEach(s => protectPath(s.filePath));
            } catch (e) {}
        }

        const scheduledRows = stmtGetScheduledJobs.all(tenantId);
        scheduledRows.forEach(r => {
            try {
                const j = JSON.parse(r.jobDataJson);
                (j.sequence || []).forEach(s => protectPath(s.filePath));
                (j.mediaItems || []).forEach(m => protectPath(m.path));
            } catch (e) {}
        });

        const active = safeReadJsonSync(campaignStateFile, {});
        (active.queue?.sequence || []).forEach(s => protectPath(s.filePath));
        (active.queue?.mediaItems || []).forEach(m => protectPath(m.path));

        const session = sessions.get(tenantId);
        if (session && session.queue) {
            (session.queue.sequence || []).forEach(s => protectPath(s.filePath));
            (session.queue.mediaItems || []).forEach(m => protectPath(m.path));
        }

        const vaultFiles = fs.readdirSync(mediaVaultDir);
        vaultFiles.forEach(file => {
            const fullPath = path.resolve(path.join(mediaVaultDir, file));
            if (fullPath.startsWith(resolvedVault) && !protectedFiles.has(fullPath)) {
                try { fs.unlinkSync(fullPath); } catch (e) {}
            }
        });
    } catch (e) {}
}

function pruneStaleSessionKeys(tenantId) {
    try {
        const { sessionDir } = getTenantPaths(tenantId);
        if (!fs.existsSync(sessionDir)) return;
        const files = fs.readdirSync(sessionDir);
        const now = Date.now();
        const maxAgeMs = 7 * 24 * 60 * 60 * 1000;

        files.forEach(file => {
            if (file === 'creds.json') return;
            if (file.startsWith('sender-key-') || file.startsWith('pre-key-')) {
                const fullPath = path.join(sessionDir, file);
                try {
                    const stat = fs.statSync(fullPath);
                    if (now - stat.mtimeMs > maxAgeMs) {
                        fs.unlinkSync(fullPath);
                    }
                } catch (e) {}
            }
        });
    } catch (e) {}
}

function getFolderSizeInBytes(folderPath) {
    let total = 0;
    if (!fs.existsSync(folderPath)) return 0;
    try {
        const files = fs.readdirSync(folderPath);
        for (const file of files) {
            const p = path.join(folderPath, file);
            try {
                const stat = fs.statSync(p);
                if (stat.isDirectory()) {
                    total += getFolderSizeInBytes(p);
                } else {
                    total += stat.size;
                }
            } catch (e) {}
        }
    } catch (e) {}
    return total;
}

function loadCampaignHistory(tenantId) {
    try {
        const rows = stmtGetCampaignHistory.all(tenantId);
        if (rows && rows.length > 0) {
            return rows.map(r => JSON.parse(r.dataJson));
        }
    } catch (e) {}

    const { campaignHistoryFile } = getTenantPaths(tenantId);
    return safeReadJsonSync(campaignHistoryFile, []);
}

function upsertCampaignHistory(tenantId, queueObj) {
    if (!queueObj || !queueObj.campaignId) return;
    const { logsFile, campaignHistoryFile } = getTenantPaths(tenantId);
    const isFinished = queueObj.state === 'STOPPED' && !queueObj.isRunning;

    const filteredRecipients = (queueObj.recipients || [])
        .filter(r => r.status === 'FAILED' || r.status === 'PENDING')
        .map(r => ({
            name: r.name || 'Unsaved',
            phone: r.phone,
            status: r.status
        }));

    const snapshot = {
        id: queueObj.campaignId,
        timestamp: queueObj.timestamp || new Date().toLocaleString(),
        state: queueObj.state,
        isRunning: queueObj.isRunning,
        total: queueObj.total || 0,
        sent: queueObj.sent || 0,
        failed: queueObj.failed || 0,
        pending: queueObj.remaining || 0,
        recipients: isFinished ? filteredRecipients : (queueObj.recipients || []).map(r => ({
            name: r.name || 'Unsaved',
            phone: r.phone,
            status: r.status || 'PENDING'
        }))
    };

    stmtUpsertCampaignHistory.run({
        campaignId: snapshot.id,
        sessionId: tenantId,
        timestamp: snapshot.timestamp,
        dataJson: JSON.stringify(snapshot),
        createdAt: Date.now()
    });

    let diskHist = safeReadJsonSync(campaignHistoryFile, []);
    const existingIdx = diskHist.findIndex(h => h.id === snapshot.id);
    if (existingIdx >= 0) diskHist[existingIdx] = snapshot;
    else diskHist.unshift(snapshot);
    if (diskHist.length > 3) diskHist = diskHist.slice(0, 3);
    safeWriteJsonSync(campaignHistoryFile, diskHist);

    if (isFinished && fs.existsSync(logsFile)) {
        try { fs.unlinkSync(logsFile); } catch (e) {}
    }
}

function saveActiveCampaignState(tenantId, state) {
    const { campaignStateFile } = getTenantPaths(tenantId);
    safeWriteJsonSync(campaignStateFile, state);
    if (state && state.queue) {
        upsertCampaignHistory(tenantId, state.queue);
    }
}

function clearActiveCampaignState(tenantId) {
    const { campaignStateFile } = getTenantPaths(tenantId);
    if (fs.existsSync(campaignStateFile)) {
        try { fs.unlinkSync(campaignStateFile); } catch (e) {}
    }
}

// ============================================================================
// LAZY-SOCKET SESSION & IDLE SLEEP MANAGER
// ============================================================================
function touchSessionActivity(sessionId) {
    const session = sessions.get(sessionId);
    if (session) {
        session.lastActive = Date.now();
    }
}

function getOrCreateSession(sessionId) {
    if (!sessions.has(sessionId)) {
        sessions.set(sessionId, {
            sock: null,
            status: 'DISCONNECTED',
            phone: null,
            sessionId: sessionId,
            isExecutingPriorityTask: false,
            isDispatchLoopActive: false,
            lastActive: Date.now(),
            queue: {
                isRunning: false,
                state: 'STOPPED',
                campaignId: null,
                timestamp: null,
                total: 0,
                sent: 0,
                failed: 0,
                remaining: 0,
                currentIndex: 0,
                recipients: [],
                sequence: [],
                message: '',
                mediaItems: []
            }
        });
    }
    const sess = sessions.get(sessionId);
    sess.lastActive = Date.now();
    return sess;
}

function putSocketToSleep(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.sock) return;

    if (session.queue?.isRunning || session.isDispatchLoopActive || session.isExecutingPriorityTask) {
        return;
    }

    try {
        session.sock.ev.removeAllListeners('connection.update');
        session.sock.ev.removeAllListeners('creds.update');
        session.sock.ev.removeAllListeners('messages.upsert');
        session.sock.end(undefined);
    } catch (e) {}

    session.sock = null;
    session.status = 'SLEEPING';
}

function wipeSessionData(sessionId) {
    const { sessionDir } = getTenantPaths(sessionId);
    if (sessions.has(sessionId)) {
        const session = sessions.get(sessionId);
        try {
            if (session.sock) {
                session.sock.ev.removeAllListeners('connection.update');
                session.sock.ev.removeAllListeners('creds.update');
                session.sock.ev.removeAllListeners('messages.upsert');
                session.sock.end(undefined);
            }
        } catch (e) {}
        sessions.delete(sessionId);
    }
    if (fs.existsSync(sessionDir)) {
        try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch (e) {}
    }
}

function safeResetTenantSession(sessionId) {
    const { sessionDir } = getTenantPaths(sessionId);
    if (sessions.has(sessionId)) {
        const session = sessions.get(sessionId);
        try {
            if (session.sock) {
                session.sock.ev.removeAllListeners('connection.update');
                session.sock.ev.removeAllListeners('creds.update');
                session.sock.ev.removeAllListeners('messages.upsert');
                session.sock.end(undefined);
            }
        } catch (e) {}
        session.sock = null;
        session.status = 'DISCONNECTED';
    }
    if (fs.existsSync(sessionDir)) {
        try {
            const files = fs.readdirSync(sessionDir);
            files.forEach(f => {
                try { fs.unlinkSync(path.join(sessionDir, f)); } catch (err) {}
            });
        } catch (e) {}
    }
}

async function initSessionSocket(sessionId) {
    const { sessionDir } = getTenantPaths(sessionId);
    const session = getOrCreateSession(sessionId);

    if (session.sock) {
        try {
            session.sock.ev.removeAllListeners('connection.update');
            session.sock.ev.removeAllListeners('creds.update');
            session.sock.ev.removeAllListeners('messages.upsert');
            session.sock.end(undefined);
        } catch (e) {}
        session.sock = null;
    }

    const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
    const sock = makeWASocket({
        auth: state,
        logger: pino({ level: 'silent' }),
        printQRInTerminal: false,
        keepAliveIntervalMs: 30000,
        browser: ['Ubuntu', 'Chrome', '20.0.04']
    });

    session.sock = sock;
    session.lastActive = Date.now();
    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('messages.upsert', async (m) => {
        try {
            if (m.type !== 'notify') return;
            const msgs = m.messages || [];
            const nowSec = Math.floor(Date.now() / 1000);

            // Guard: Safe-Pause Auto-Responder if Subscription Expired
            const licCheck = stmtGetLicense.get(sessionId);
            if (licCheck) {
                if (licCheck.status !== 'ACTIVE' || Date.now() > licCheck.validUntil) {
                    return; // Silent bypass: no welcome message sent when expired
                }
            }

            for (const msg of msgs) {
                if (!msg || !msg.message || msg.key.fromMe) continue;

                const msgTimestamp = msg.messageTimestamp || nowSec;
                if (nowSec - msgTimestamp > 60) continue;

                const remoteJid = msg.key.remoteJid || '';
                if (remoteJid === 'status@broadcast' || remoteJid.endsWith('@g.us')) continue;
                if (!remoteJid.endsWith('@s.whatsapp.net') && !remoteJid.endsWith('@lid')) continue;

                if (msg.message.reactionMessage || msg.message.protocolMessage || msg.message.stickerMessage) continue;

                const welcomeRow = stmtGetWelcome.get(sessionId);
                if (!welcomeRow || !welcomeRow.configJson) continue;

                const rules = JSON.parse(welcomeRow.configJson);
                if (!rules.enabled || !rules.sequence || rules.sequence.length === 0) continue;

                const senderKey = remoteJid.split('@')[0];
                const cacheKey = `${sessionId}:${senderKey}`;
                const lastSent = welcomeCooldownCache.get(cacheKey) || 0;
                const cooldownPeriod = (rules.cooldownHours || 24) * 60 * 60 * 1000;

                if (Date.now() - lastSent < cooldownPeriod) continue;
                welcomeCooldownCache.set(cacheKey, Date.now());

                session.isExecutingPriorityTask = true;
                touchSessionActivity(sessionId);

                try {
                    for (const step of rules.sequence) {
                        await delay(1800);
                        try { await sock.sendPresenceUpdate('composing', remoteJid); } catch (e) {}
                        await delay(1000);

                        if (step.type === 'text' && step.content) {
                            await sock.sendMessage(remoteJid, { text: injectZeroWidthRandomizers(step.content) });
                        } else if ((step.type === 'image' || step.type === 'video' || step.type === 'media') && step.filePath && fs.existsSync(step.filePath)) {
                            const mime = step.mimetype || '';
                            const caption = step.caption ? injectZeroWidthRandomizers(step.caption) : '';
                            const randomizedBuffer = buildRandomizedMediaBuffer(step.filePath, mime);

                            if (mime.startsWith('image/') || step.type === 'image') {
                                await sock.sendMessage(remoteJid, { image: randomizedBuffer, caption });
                            } else if (mime.startsWith('video/') || step.type === 'video') {
                                await sock.sendMessage(remoteJid, { video: randomizedBuffer, caption });
                            } else {
                                await sock.sendMessage(remoteJid, { document: randomizedBuffer, mimetype: mime, fileName: path.basename(step.filePath), caption });
                            }
                        }
                        try { await sock.sendPresenceUpdate('paused', remoteJid); } catch (e) {}
                    }
                } finally {
                    session.isExecutingPriorityTask = false;
                    touchSessionActivity(sessionId);
                }
            }
        } catch (err) {
            session.isExecutingPriorityTask = false;
            console.error('[WELCOME ERROR]', err);
        }
    });

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;
        touchSessionActivity(sessionId);

        if (connection === 'close') {
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const isLoggedOut = statusCode === DisconnectReason.loggedOut || statusCode === 401 || statusCode === 403;
            session.status = 'DISCONNECTED';

            if (session.queue && session.queue.isRunning) {
                session.queue.state = 'PAUSED';
                session.queue.isRunning = false;
                saveActiveCampaignState(sessionId, { sessionId, queue: session.queue });
            }

            if (isLoggedOut) {
                session.phone = null;
                wipeSessionData(sessionId);
            } else {
                setTimeout(() => { if (sessions.has(sessionId)) initSessionSocket(sessionId); }, 3000);
            }
        } else if (connection === 'open') {
            session.status = 'CONNECTED';
            const rawId = sock.user?.id || '';
            session.phone = rawId.split(':')[0] || rawId.split('@')[0];

            let lic = stmtGetLicense.get(sessionId);
            const nowTime = Date.now();
            if (!lic) {
                const tenantToken = crypto.randomBytes(16).toString('hex');
                stmtUpsertLicense.run({
                    sessionId,
                    phone: session.phone,
                    status: 'ACTIVE',
                    plan: 'Monthly (₹150)',
                    tenantToken,
                    validUntil: nowTime + (30 * 24 * 60 * 60 * 1000),
                    notes: 'Auto-registered Device',
                    createdAt: nowTime,
                    purchasedAt: nowTime
                });
            } else {
                stmtUpsertLicense.run({
                    sessionId,
                    phone: session.phone,
                    status: lic.status,
                    plan: lic.plan,
                    tenantToken: lic.tenantToken || crypto.randomBytes(16).toString('hex'),
                    validUntil: lic.validUntil,
                    notes: lic.notes || '',
                    createdAt: lic.createdAt || nowTime,
                    purchasedAt: lic.purchasedAt || nowTime
                });
            }
            resumeInFlightCrashCampaign(sessionId);
        }
    });

    return session;
}

async function ensureActiveSocket(sessionId) {
    const session = getOrCreateSession(sessionId);
    touchSessionActivity(sessionId);
    if (!session.sock || session.status !== 'CONNECTED') {
        const { sessionDir } = getTenantPaths(sessionId);
        if (fs.existsSync(path.join(sessionDir, 'creds.json'))) {
            await initSessionSocket(sessionId);
            let attempts = 0;
            while (session.status !== 'CONNECTED' && attempts < 15) {
                await delay(500);
                attempts++;
            }
        }
    }
    return session;
}

function resumeInFlightCrashCampaign(sessionId) {
    const { campaignStateFile } = getTenantPaths(sessionId);
    if (!fs.existsSync(campaignStateFile)) return;
    try {
        const saved = safeReadJsonSync(campaignStateFile, null);
        if (saved && saved.sessionId === sessionId && saved.queue?.remaining > 0 && (saved.queue?.state === 'RUNNING' || saved.queue?.state === 'PAUSED')) {
            const session = sessions.get(sessionId);
            session.queue = saved.queue;
            if (saved.queue.state === 'RUNNING') {
                session.queue.isRunning = true;
                executeDispatchQueue(sessionId);
            } else {
                session.queue.isRunning = false;
            }
        }
    } catch (e) {}
}

async function executeDispatchQueue(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.queue) return;
    if (session.isDispatchLoopActive) return;

    session.isDispatchLoopActive = true;
    const q = session.queue;

    try {
        while (q.currentIndex < q.recipients.length) {
            touchSessionActivity(sessionId);

            if (q.state === 'STOPPED') {
                q.isRunning = false;
                purgeCampaignMediaItems(q.sequence || q.mediaItems);
                q.sequence = [];
                q.mediaItems = [];
                upsertCampaignHistory(sessionId, q);
                clearActiveCampaignState(sessionId);
                cleanOrphanedMediaVaultFiles(sessionId);
                session.isDispatchLoopActive = false;
                return;
            }

            if (q.state === 'PAUSED') {
                q.isRunning = false;
                saveActiveCampaignState(sessionId, { sessionId, queue: q });
                session.isDispatchLoopActive = false;
                return;
            }

            while (session.isExecutingPriorityTask) {
                await delay(1000);
            }

            const item = q.recipients[q.currentIndex];
            const rawPhone = typeof item === 'object' ? item.phone : item;
            const clean = sanitizeToValidWhatsAppJid(rawPhone);

            if (!clean || clean.length < 10) {
                q.failed++;
                q.remaining = Math.max(0, q.total - q.sent - q.failed);
                if (typeof q.recipients[q.currentIndex] === 'object') {
                    q.recipients[q.currentIndex].status = 'FAILED';
                }
                q.currentIndex++;
                saveActiveCampaignState(sessionId, { sessionId, queue: q });
                continue;
            }

            const targetJid = clean + '@s.whatsapp.net';

            try {
                const [lookup] = await session.sock.onWhatsApp(targetJid);
                if (!lookup?.exists) {
                    throw new Error('NOT_ON_WHATSAPP');
                }

                if (q.state === 'PAUSED' || q.state === 'STOPPED') continue;

                if (Array.isArray(q.sequence) && q.sequence.length > 0) {
                    for (const step of q.sequence) {
                        if (q.state === 'PAUSED' || q.state === 'STOPPED') break;

                        await session.sock.sendPresenceUpdate('composing', lookup.jid);
                        await delay(Math.floor(Math.random() * (900 - 500 + 1)) + 500);

                        if (step.type === 'text' && step.content) {
                            await session.sock.sendMessage(lookup.jid, { text: injectZeroWidthRandomizers(step.content) });
                        } else if ((step.type === 'image' || step.type === 'video') && step.filePath && fs.existsSync(step.filePath)) {
                            const mime = step.mimetype || '';
                            const caption = step.caption ? injectZeroWidthRandomizers(step.caption) : '';
                            const randomizedBuffer = buildRandomizedMediaBuffer(step.filePath, mime);

                            if (mime.startsWith('image/') || step.type === 'image') {
                                await session.sock.sendMessage(lookup.jid, { image: randomizedBuffer, caption });
                            } else if (mime.startsWith('video/') || step.type === 'video') {
                                await session.sock.sendMessage(lookup.jid, { video: randomizedBuffer, caption });
                            }
                        }
                    }
                } else {
                    await session.sock.sendPresenceUpdate('composing', lookup.jid);
                    await delay(Math.floor(Math.random() * (900 - 500 + 1)) + 500);

                    const resolvedMessage = q.message ? injectZeroWidthRandomizers(q.message) : '';

                    if (q.mediaItems && q.mediaItems.length > 0) {
                        for (let mIdx = 0; mIdx < q.mediaItems.length; mIdx++) {
                            const media = q.mediaItems[mIdx];
                            if (fs.existsSync(media.path)) {
                                const randomizedBuffer = buildRandomizedMediaBuffer(media.path, media.mimetype);
                                const mime = media.mimetype || '';
                                const captionText = media.caption
                                    ? injectZeroWidthRandomizers(media.caption)
                                    : (mIdx === 0 ? resolvedMessage : '');

                                if (mime.startsWith('image/')) {
                                    await session.sock.sendMessage(lookup.jid, { image: randomizedBuffer, caption: captionText });
                                } else if (mime.startsWith('video/')) {
                                    await session.sock.sendMessage(lookup.jid, { video: randomizedBuffer, caption: captionText });
                                } else {
                                    await session.sock.sendMessage(lookup.jid, { document: randomizedBuffer, mimetype: mime, fileName: media.originalname, caption: captionText });
                                }
                            }
                        }
                    } else if (resolvedMessage) {
                        await session.sock.sendMessage(lookup.jid, { text: resolvedMessage });
                    }
                }

                await session.sock.sendPresenceUpdate('paused', lookup.jid);
                q.sent++;
                q.remaining = Math.max(0, q.total - q.sent - q.failed);
                if (typeof q.recipients[q.currentIndex] === 'object') {
                    q.recipients[q.currentIndex].status = 'SENT';
                }

            } catch (e) {
                q.failed++;
                q.remaining = Math.max(0, q.total - q.sent - q.failed);
                if (typeof q.recipients[q.currentIndex] === 'object') {
                    q.recipients[q.currentIndex].status = 'FAILED';
                }
            }

            q.currentIndex++;
            saveActiveCampaignState(sessionId, { sessionId, queue: q });

            if (q.currentIndex < q.recipients.length && q.state === 'RUNNING') {
                if (q.sent > 0 && q.sent % 25 === 0) {
                    const breatherMs = Math.floor(Math.random() * (18000 - 12000 + 1)) + 12000;
                    await delay(breatherMs);
                } else {
                    await delay(Math.floor(Math.random() * (4200 - 2200 + 1)) + 2200);
                }
            }
        }

        q.isRunning = false;
        q.state = 'STOPPED';
        purgeCampaignMediaItems(q.sequence || q.mediaItems);
        q.sequence = [];
        q.mediaItems = [];
        upsertCampaignHistory(sessionId, q);
        clearActiveCampaignState(sessionId);
        cleanOrphanedMediaVaultFiles(sessionId);
    } finally {
        session.isDispatchLoopActive = false;
        touchSessionActivity(sessionId);
    }
}

// Background Cron: Scheduled Jobs, Auto-Expiry Notices & Lazy Socket Sleep Check (Every 20 seconds)
setInterval(async () => {
    const now = Date.now();

    for (const [tenantId, session] of sessions.entries()) {
        if (session && session.sock && (now - session.lastActive > 15 * 60 * 1000)) {
            putSocketToSleep(tenantId);
        }
    }

    try {
        const noticeExpRow = stmtGetSetting.get('global_notice_expiry');
        if (noticeExpRow && noticeExpRow.value) {
            const expTime = parseInt(noticeExpRow.value, 10);
            if (expTime > 0 && now >= expTime) {
                stmtSaveSetting.run('global_notice', '', now);
                stmtSaveSetting.run('global_notice_expiry', '0', now);
            }
        }
    } catch (e) {}

    try {
        const dueJobs = stmtGetAllDueJobs.all('SCHEDULED', now);
        for (const row of dueJobs) {
            const tenantId = row.sessionId;
            const session = await ensureActiveSocket(tenantId);

            if (session && session.status === 'CONNECTED' && (!session.queue || !session.queue.isRunning)) {
                stmtUpsertScheduledJob.run({
                    jobId: row.jobId,
                    sessionId: tenantId,
                    status: 'DISPATCHING',
                    scheduledForEpoch: row.scheduledForEpoch,
                    jobDataJson: row.jobDataJson
                });

                const job = JSON.parse(row.jobDataJson);
                session.queue = {
                    isRunning: true,
                    state: 'RUNNING',
                    campaignId: job.id,
                    timestamp: new Date().toLocaleString(),
                    total: job.recipients.length,
                    sent: 0,
                    failed: 0,
                    remaining: job.recipients.length,
                    currentIndex: 0,
                    recipients: job.recipients.map(r => ({
                        name: r.name || 'Unsaved',
                        phone: r.phone,
                        status: 'PENDING'
                    })),
                    sequence: job.sequence || [],
                    message: job.message || '',
                    mediaItems: job.mediaItems || []
                };

                saveActiveCampaignState(tenantId, { sessionId: tenantId, queue: session.queue });
                executeDispatchQueue(tenantId);
            }
        }
    } catch (e) {}
}, 20000);

// Non-blocking 24-hour maintenance cron (Prunes stale pre-keys once a day at night)
setInterval(() => {
    try {
        const licenses = stmtGetAllLicensesSorted.all();
        for (const lic of licenses) {
            pruneStaleSessionKeys(lic.sessionId);
        }
    } catch (e) {}
}, 24 * 60 * 60 * 1000);

// Only wake up active in-flight tenants on engine start
if (fs.existsSync(BASE_TENANTS_DIR)) {
    fs.readdirSync(BASE_TENANTS_DIR).forEach(d => {
        if (d.startsWith('tenant_')) {
            const tid = d.replace('tenant_', '');
            const { sessionDir, campaignStateFile } = getTenantPaths(tid);
            if (fs.existsSync(path.join(sessionDir, 'creds.json'))) {
                const saved = safeReadJsonSync(campaignStateFile, null);
                if (saved && saved.queue?.remaining > 0) {
                    initSessionSocket(tid);
                }
            }
        }
    });
}

// ============================================================================
// API ROUTES (100% Retained & Compatible with index.html)
// ============================================================================

app.post('/api/pair', authenticate, async (req, res) => {
    const { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ error: 'Missing phone number.' });
    const cleanNumber = sanitizeToValidWhatsAppJid(phoneNumber);
    if (!cleanNumber) return res.status(400).json({ error: 'Invalid phone number.' });

    wipeSessionData(cleanNumber);
    const session = await initSessionSocket(cleanNumber);

    try {
        await delay(2000);
        const code = await session.sock.requestPairingCode(cleanNumber);
        return res.json({ success: true, pairingCode: code, tenantId: cleanNumber });
    } catch (err) {
        wipeSessionData(cleanNumber);
        return res.status(500).json({ error: 'Pairing reset. Please tap Pair again.' });
    }
});

app.get('/api/status/:sessionId', async (req, res) => {
    const authHeader = req.headers['authorization'] || '';
    const token = (authHeader ? authHeader.replace(/^Bearer\s+/i, '') : (req.query.token || '')).trim();

    const isMasterToken = (token === API_SECRET_TOKEN || token === 'marketing_engine_secret_2026');
    const lic = stmtGetLicense.get(req.params.sessionId);
    const isTenantToken = lic && lic.tenantToken && token === lic.tenantToken;

    if (!isMasterToken && !isTenantToken) {
        return res.status(401).json({ error: 'Unauthorized: Invalid token.' });
    }

    const { sessionId } = req.params;
    let session = sessions.get(sessionId);

    if (!session || session.status === 'SLEEPING' || !session.sock) {
        session = await ensureActiveSocket(sessionId);
    }

    touchSessionActivity(sessionId);

    const noticeRow = stmtGetSetting.get('global_notice');
    let globalNotice = noticeRow ? noticeRow.value : '';

    const noticeExpRow = stmtGetSetting.get('global_notice_expiry');
    if (noticeExpRow && noticeExpRow.value) {
        const expTime = parseInt(noticeExpRow.value, 10);
        if (expTime > 0 && Date.now() >= expTime) {
            globalNotice = '';
        }
    }

    let daysLeft = 30;
    let isExpired = false;
    let purchasedAt = Date.now();
    let validUntil = Date.now() + 30 * 24 * 60 * 60 * 1000;

    if (lic) {
        daysLeft = Math.ceil((lic.validUntil - Date.now()) / (1000 * 60 * 60 * 24));
        isExpired = daysLeft <= 0;
        purchasedAt = lic.purchasedAt || lic.createdAt || Date.now();
        validUntil = lic.validUntil;
    }

    if (!session || session.status !== 'CONNECTED') {
        return res.json({ 
            status: 'DISCONNECTED', 
            phone: null, 
            globalNotice, 
            daysLeft, 
            isExpired,
            plan: lic?.plan || 'Monthly',
            purchasedAt,
            validUntil
        });
    }

    res.json({
        status: 'CONNECTED',
        phone: session.phone || null,
        tenantId: session.sessionId,
        tenantToken: lic?.tenantToken || '',
        globalNotice,
        daysLeft,
        isExpired,
        plan: lic?.plan || 'Monthly',
        purchasedAt,
        validUntil
    });
});

app.post('/api/groups/sync', authenticate, async (req, res) => {
    const { sessionId, groups, allowEmptyOverwrite } = req.body;
    await ensureActiveSocket(sessionId);
    const session = sessions.get(sessionId);
    if (!session || session.status !== 'CONNECTED') {
        return res.status(400).json({ error: 'WhatsApp not connected. Cannot sync.' });
    }

    const { groupsBackupFile } = getTenantPaths(sessionId);

    const existingRow = stmtGetGroups.get(sessionId);
    if (groups.length === 0 && !allowEmptyOverwrite) {
        if (existingRow && existingRow.groupsJson) {
            try {
                const existing = JSON.parse(existingRow.groupsJson);
                if (existing.length > 0) {
                    return res.json({ success: true, skippedEmptyOverwrite: true, groups: existing });
                }
            } catch (e) {}
        }
        if (fs.existsSync(groupsBackupFile)) {
            const diskExisting = safeReadJsonSync(groupsBackupFile, []);
            if (diskExisting.length > 0) {
                return res.json({ success: true, skippedEmptyOverwrite: true, groups: diskExisting });
            }
        }
    }

    stmtSaveGroups.run(sessionId, JSON.stringify(groups), Date.now());
    safeWriteJsonSync(groupsBackupFile, groups);

    touchSessionActivity(sessionId);
    res.json({ success: true });
});

app.get('/api/groups/restore', authenticate, (req, res) => {
    const sessionId = req.query.sessionId || 'unpaired_user';
    if (!sessionId || sessionId === 'unpaired_user') {
        return res.json({ success: true, groups: [] });
    }

    touchSessionActivity(sessionId);

    const row = stmtGetGroups.get(sessionId);
    if (row && row.groupsJson) {
        try {
            const parsed = JSON.parse(row.groupsJson);
            if (Array.isArray(parsed) && parsed.length > 0) {
                return res.json({ success: true, groups: parsed });
            }
        } catch (e) {}
    }

    const { groupsBackupFile } = getTenantPaths(sessionId);
    const diskGroups = safeReadJsonSync(groupsBackupFile, []);
    if (Array.isArray(diskGroups) && diskGroups.length > 0) {
        stmtSaveGroups.run(sessionId, JSON.stringify(diskGroups), Date.now());
    }

    res.json({ success: true, groups: diskGroups });
});

app.post('/api/welcome/setup', upload.any(), authenticate, async (req, res) => {
    const sessionId = req.body.sessionId || 'unpaired_user';
    await ensureActiveSocket(sessionId);
    const session = sessions.get(sessionId);
    if (!session || session.status !== 'CONNECTED') {
        purgeCampaignMediaItems(req.files);
        return res.status(400).json({ error: 'WhatsApp not connected.' });
    }

    const { welcomeRulesFile, mediaVaultDir } = getTenantPaths(sessionId);
    const resolvedVault = path.resolve(mediaVaultDir);
    const enabled = req.body.enabled === 'true' || req.body.enabled === true;

    let blocksMeta = [];
    try { blocksMeta = JSON.parse(req.body.blocksMeta || '[]'); } catch (e) { blocksMeta = []; }

    const filesMap = new Map();
    if (req.files && req.files.length > 0) {
        req.files.forEach(f => filesMap.set(f.fieldname, f));
    }

    const sequence = [];
    for (const block of blocksMeta) {
        if (block.type === 'text' && block.content && block.content.trim()) {
            sequence.push({
                id: block.id,
                type: 'text',
                content: block.content.trim()
            });
        } else if (block.type === 'image' || block.type === 'video') {
            const uploaded = filesMap.get(`file_${block.id}`);
            if (uploaded) {
                sequence.push({
                    id: block.id,
                    type: block.type,
                    filePath: uploaded.path,
                    fileName: uploaded.originalname,
                    mimetype: uploaded.mimetype,
                    caption: (block.caption || '').trim()
                });
            } else if (block.existingFilePath) {
                const resolvedExisting = path.resolve(block.existingFilePath);
                if (resolvedExisting.startsWith(resolvedVault) && fs.existsSync(resolvedExisting)) {
                    sequence.push({
                        id: block.id,
                        type: block.type,
                        filePath: resolvedExisting,
                        fileName: block.fileName || path.basename(resolvedExisting),
                        mimetype: block.mimetype || (block.type === 'video' ? 'video/mp4' : 'image/jpeg'),
                        caption: (block.caption || '').trim()
                    });
                }
            }
        }
    }

    const config = { enabled, cooldownHours: 24, sequence };
    stmtSaveWelcome.run(sessionId, JSON.stringify(config), Date.now());
    safeWriteJsonSync(welcomeRulesFile, config);
    cleanOrphanedMediaVaultFiles(sessionId);
    touchSessionActivity(sessionId);

    res.json({ success: true, config });
});

app.get('/api/welcome/rules', authenticate, (req, res) => {
    const sessionId = req.query.sessionId || 'unpaired_user';
    touchSessionActivity(sessionId);
    
    const row = stmtGetWelcome.get(sessionId);
    if (row && row.configJson) {
        try {
            return res.json({ success: true, config: JSON.parse(row.configJson) });
        } catch (e) {}
    }

    const { welcomeRulesFile } = getTenantPaths(sessionId);
    const config = safeReadJsonSync(welcomeRulesFile, { enabled: false, sequence: [] });
    res.json({ success: true, config });
});

app.post('/api/message/send-bulk', upload.any(), authenticate, async (req, res) => {
    const sessionId = req.body.sessionId || 'unpaired_user';
    const recipients = req.body.recipients;
    const scheduleEpoch = parseInt(req.body.scheduleEpoch || 0, 10);
    const isScheduled = req.body.isScheduled === 'true' || req.body.isScheduled === true;

    await ensureActiveSocket(sessionId);
    const session = sessions.get(sessionId);
    if (!session || session.status !== 'CONNECTED') {
        purgeCampaignMediaItems(req.files);
        return res.status(400).json({ error: 'WhatsApp not connected.' });
    }

    if (!isScheduled && session.queue && session.queue.isRunning) {
        purgeCampaignMediaItems(req.files);
        return res.status(400).json({ error: 'A campaign is already running. Please wait or stop it first.' });
    }

    const scheduledRows = stmtGetScheduledJobs.all(sessionId);
    if (isScheduled && scheduledRows.filter(r => r.status === 'SCHEDULED').length >= 3) {
        purgeCampaignMediaItems(req.files);
        return res.status(400).json({ error: 'Maximum 3 scheduled campaigns limit reached! Please cancel one before scheduling another.' });
    }

    let parsed = [];
    try { parsed = JSON.parse(recipients || '[]'); } catch (e) { parsed = []; }
    if (parsed.length === 0) {
        purgeCampaignMediaItems(req.files);
        return res.status(400).json({ error: 'Recipient list cannot be empty.' });
    }

    let blocksMeta = [];
    try { blocksMeta = JSON.parse(req.body.blocksMeta || '[]'); } catch (e) { blocksMeta = []; }

    const filesMap = new Map();
    if (req.files && req.files.length > 0) {
        req.files.forEach(f => filesMap.set(f.fieldname, f));
    }

    const sequence = [];
    for (const block of blocksMeta) {
        if (block.type === 'text' && block.content && block.content.trim()) {
            sequence.push({
                id: block.id,
                type: 'text',
                content: block.content.trim()
            });
        } else if (block.type === 'image' || block.type === 'video') {
            const uploaded = filesMap.get(`file_${block.id}`);
            if (uploaded) {
                sequence.push({
                    id: block.id,
                    type: block.type,
                    filePath: uploaded.path,
                    fileName: uploaded.originalname,
                    mimetype: uploaded.mimetype,
                    caption: (block.caption || '').trim()
                });
            }
        }
    }

    const legacyMessage = req.body.message || '';
    const legacyMedia = (req.files || []).map(f => ({
        path: f.path,
        originalname: f.originalname,
        mimetype: f.mimetype,
        caption: req.body[`caption_${f.fieldname}`] || req.body.globalCaption || ''
    }));

    const formattedRecipients = parsed.map(r => ({
        name: r.name || 'Unsaved',
        phone: sanitizeToValidWhatsAppJid(r.phone || r),
        status: 'PENDING'
    }));

    if (isScheduled && scheduleEpoch > Date.now()) {
        const job = {
            id: 'sch_' + Date.now(),
            scheduledForEpoch: scheduleEpoch,
            scheduledForDate: new Date(scheduleEpoch).toLocaleString(),
            status: 'SCHEDULED',
            recipients: formattedRecipients,
            sequence: sequence,
            message: legacyMessage,
            mediaItems: legacyMedia
        };

        stmtUpsertScheduledJob.run({
            jobId: job.id,
            sessionId,
            status: 'SCHEDULED',
            scheduledForEpoch: scheduleEpoch,
            jobDataJson: JSON.stringify(job)
        });

        const { scheduledJobsFile } = getTenantPaths(sessionId);
        let existingJobs = safeReadJsonSync(scheduledJobsFile, []);
        existingJobs.push(job);
        safeWriteJsonSync(scheduledJobsFile, existingJobs);

        touchSessionActivity(sessionId);
        return res.json({ status: 'SCHEDULED', jobId: job.id, scheduledForDate: job.scheduledForDate });
    }

    const campaignId = 'cmp_' + Date.now();
    session.queue = {
        isRunning: true,
        state: 'RUNNING',
        campaignId,
        timestamp: new Date().toLocaleString(),
        total: formattedRecipients.length,
        sent: 0,
        failed: 0,
        remaining: formattedRecipients.length,
        currentIndex: 0,
        recipients: formattedRecipients,
        sequence: sequence,
        message: legacyMessage,
        mediaItems: legacyMedia
    };

    saveActiveCampaignState(sessionId, { sessionId, queue: session.queue });
    touchSessionActivity(sessionId);
    res.json({ status: 'QUEUED', campaignId, totalRecipients: formattedRecipients.length, campaign: session.queue });

    executeDispatchQueue(sessionId);
});

app.get('/api/campaign/scheduled-jobs/:sessionId', authenticate, (req, res) => {
    const { sessionId } = req.params;
    touchSessionActivity(sessionId);
    const rows = stmtGetScheduledJobs.all(sessionId);
    const scheduled = rows.filter(r => r.status === 'SCHEDULED').map(r => {
        const j = JSON.parse(r.jobDataJson);
        return {
            id: j.id,
            scheduledForDate: j.scheduledForDate,
            scheduledForEpoch: j.scheduledForEpoch,
            recipientCount: (j.recipients || []).length,
            sequenceSummary: (j.sequence || []).map(s => ({
                type: s.type,
                content: s.content || '',
                caption: s.caption || '',
                fileName: s.fileName || ''
            })),
            message: j.message || '',
            mediaCount: (j.mediaItems || []).length + (j.sequence || []).filter(s => s.type !== 'text').length
        };
    });
    res.json({ success: true, jobs: scheduled });
});

app.post('/api/campaign/cancel-schedule', authenticate, (req, res) => {
    const { sessionId, jobId } = req.body;
    const rows = stmtGetScheduledJobs.all(sessionId);
    const targetRow = rows.find(r => r.jobId === jobId);

    if (targetRow) {
        try {
            const targetJob = JSON.parse(targetRow.jobDataJson);
            purgeCampaignMediaItems(targetJob.sequence || targetJob.mediaItems);
        } catch (e) {}

        stmtDeleteScheduledJob.run(jobId);

        const { scheduledJobsFile } = getTenantPaths(sessionId);
        let jobs = safeReadJsonSync(scheduledJobsFile, []);
        jobs = jobs.filter(j => j.id !== jobId);
        safeWriteJsonSync(scheduledJobsFile, jobs);

        cleanOrphanedMediaVaultFiles(sessionId);
        touchSessionActivity(sessionId);
        return res.json({ success: true, message: 'Scheduled campaign cancelled and media files purged.' });
    }
    res.status(404).json({ error: 'Job not found.' });
});

app.get('/api/campaign/state', authenticate, (req, res) => {
    const sessionId = req.query.sessionId || 'unpaired_user';
    touchSessionActivity(sessionId);
    const session = sessions.get(sessionId);
    const history = loadCampaignHistory(sessionId);

    if (session && session.queue && session.queue.campaignId && session.queue.state !== 'STOPPED') {
        return res.json({ hasState: true, queue: session.queue, history });
    }

    const { campaignStateFile } = getTenantPaths(sessionId);
    const saved = safeReadJsonSync(campaignStateFile, null);
    if (saved) {
        return res.json({ hasState: true, queue: saved.queue, history });
    }

    res.json({ hasState: false, history });
});

app.post('/api/campaign/pause', authenticate, (req, res) => {
    const sessionId = req.body.sessionId || 'unpaired_user';
    const session = sessions.get(sessionId);
    if (!session || !session.queue || session.queue.state === 'STOPPED') {
        return res.status(400).json({ error: 'No active campaign to pause.' });
    }

    session.queue.state = 'PAUSED';
    session.queue.isRunning = false;
    saveActiveCampaignState(sessionId, { sessionId, queue: session.queue });
    touchSessionActivity(sessionId);
    res.json({ status: 'PAUSED', campaignId: session.queue.campaignId, queue: session.queue });
});

app.post('/api/campaign/resume', authenticate, async (req, res) => {
    const sessionId = req.body.sessionId || 'unpaired_user';
    await ensureActiveSocket(sessionId);
    const session = sessions.get(sessionId);
    if (!session || !session.queue || session.queue.state !== 'PAUSED') {
        return res.status(400).json({ error: 'No paused campaign available to resume.' });
    }

    session.queue.state = 'RUNNING';
    session.queue.isRunning = true;
    saveActiveCampaignState(sessionId, { sessionId, queue: session.queue });
    touchSessionActivity(sessionId);
    res.json({ status: 'RUNNING', campaignId: session.queue.campaignId, queue: session.queue });

    executeDispatchQueue(sessionId);
});

app.post('/api/campaign/stop', authenticate, (req, res) => {
    const sessionId = req.body.sessionId || 'unpaired_user';
    const session = sessions.get(sessionId);
    if (!session || !session.queue) {
        return res.status(400).json({ error: 'No active campaign found.' });
    }

    session.queue.state = 'STOPPED';
    session.queue.isRunning = false;
    purgeCampaignMediaItems(session.queue.sequence || session.queue.mediaItems);
    session.queue.sequence = [];
    session.queue.mediaItems = [];
    upsertCampaignHistory(sessionId, session.queue);
    clearActiveCampaignState(sessionId);
    cleanOrphanedMediaVaultFiles(sessionId);
    touchSessionActivity(sessionId);

    res.json({ status: 'STOPPED', campaignId: session.queue.campaignId, queue: session.queue });
});

app.get('/api/queue-status/:sessionId', authenticate, (req, res) => {
    const { sessionId } = req.params;
    touchSessionActivity(sessionId);
    const session = sessions.get(sessionId);
    const history = loadCampaignHistory(sessionId);

    if (!session || !session.queue || !session.queue.campaignId) {
        return res.json({ isRunning: false, state: 'STOPPED', total: 0, sent: 0, failed: 0, remaining: 0, history });
    }
    res.json({
        isRunning: session.queue.isRunning,
        state: session.queue.state,
        campaignId: session.queue.campaignId,
        total: session.queue.total,
        sent: session.queue.sent,
        failed: session.queue.failed,
        remaining: session.queue.remaining,
        recipients: session.queue.recipients,
        history
    });
});

app.post('/api/logout', authenticate, async (req, res) => {
    const sessionId = req.body.sessionId || 'unpaired_user';
    const session = sessions.get(sessionId);
    if (session && session.sock) {
        try {
            await Promise.race([
                session.sock.logout(),
                delay(2000)
            ]);
        } catch (err) {}
    }
    wipeSessionData(sessionId);
    res.json({ success: true, status: 'DISCONNECTED', message: 'Account unlinked & session credentials wiped. Groups preserved.' });
});

app.use((err, req, res, next) => {
    if (err instanceof multer.MulterError || err) {
        if (req.files) purgeCampaignMediaItems(req.files);
        return res.status(400).json({ error: err.message || 'File upload error.' });
    }
    next();
});

// ============================================================================
// ENTERPRISE MASTER CONTROL & LICENSING DASHBOARD
// ============================================================================

app.get('/master-admin/backup-db', (req, res) => {
    const pin = req.query.pin;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).send('Forbidden');
    if (!fs.existsSync(DB_FILE)) return res.status(404).send('Database not found');
    res.download(DB_FILE, `engine_data_backup_${Date.now()}.sqlite`);
});

// Background Auto-Refresh JSON Polling API (Zero-Flicker)
app.get('/master-admin/live-metrics', (req, res) => {
    const pin = req.query.pin;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });

    const licenses = stmtGetAllLicensesSorted.all();
    const memory = process.memoryUsage();
    const nodeRamMB = (memory.heapUsed / 1024 / 1024).toFixed(1);

    const totalSysMemBytes = os.totalmem();
    const freeSysMemBytes = os.freemem();
    const usedSysMemBytes = totalSysMemBytes - freeSysMemBytes;
    const sysTotalRAMGB = (totalSysMemBytes / (1024 * 1024 * 1024)).toFixed(1);
    const sysUsedRAMGB = (usedSysMemBytes / (1024 * 1024 * 1024)).toFixed(1);
    const sysRamPercent = Math.round((usedSysMemBytes / totalSysMemBytes) * 100);

    let totalOnline = 0;
    let totalSleeping = 0;
    let totalOffline = 0;
    let activeCampaignsCount = 0;
    let totalQueuedSchedules = 0;
    let totalActiveWelcomes = 0;

    const tenantsData = {};

    for (const lic of licenses) {
        const sid = lic.sessionId;
        const session = sessions.get(sid);
        const daysLeft = Math.ceil((lic.validUntil - Date.now()) / (1000 * 60 * 60 * 24));
        const isExpired = daysLeft <= 0;

        let socketStatus = 'OFFLINE';
        let socketColor = '#ef4444';
        if (session && session.status === 'CONNECTED') {
            socketStatus = 'ONLINE';
            socketColor = '#00a884';
            totalOnline++;
        } else if (session && session.status === 'SLEEPING') {
            socketStatus = 'SLEEPING';
            socketColor = '#eab308';
            totalSleeping++;
        } else {
            totalOffline++;
        }

        let dispatchHtml = '<span style="color:#8696a0;font-size:0.75rem;">Idle ⚪</span>';
        if (session && session.queue && session.queue.isRunning) {
            activeCampaignsCount++;
            const sent = session.queue.sent || 0;
            const total = session.queue.total || 0;
            const remaining = Math.max(0, total - sent - (session.queue.failed || 0));
            dispatchHtml = `<span style="color:#00a884;font-weight:bold;font-size:0.75rem;">⚡ Sending: ${sent}/${total} (${remaining} pending)</span>`;
        } else if (session && session.queue && session.queue.state === 'PAUSED') {
            const sent = session.queue.sent || 0;
            const total = session.queue.total || 0;
            dispatchHtml = `<span style="color:#f59e0b;font-weight:bold;font-size:0.75rem;">⏸️ Paused: ${sent}/${total}</span>`;
        }

        let schedHtml = '';
        try {
            const schedRows = stmtGetScheduledJobs.all(sid);
            const activeScheds = schedRows.filter(r => r.status === 'SCHEDULED');
            if (activeScheds.length > 0) {
                totalQueuedSchedules += activeScheds.length;
                schedHtml = `<div style="font-size:0.72rem;color:#38bdf8;font-weight:600;margin-top:2px;">⏰ Sched: ${activeScheds.length} queued</div>`;
            }
        } catch (e) {}

        let welHtml = '';
        try {
            const welRow = stmtGetWelcome.get(sid);
            if (welRow && welRow.configJson) {
                const wConfig = JSON.parse(welRow.configJson);
                if (wConfig.enabled) {
                    totalActiveWelcomes++;
                    welHtml = `<div style="font-size:0.72rem;color:#c084fc;font-weight:600;margin-top:2px;">🤖 Welcome: Active</div>`;
                }
            }
        } catch (e) {}

        tenantsData[sid] = {
            socketStatus,
            socketColor,
            dispatchHtml,
            schedHtml,
            welHtml,
            status: lic.status,
            daysLeft,
            isExpired,
            plan: lic.plan || 'Standard'
        };
    }

    res.json({
        sysUsedRAMGB,
        sysTotalRAMGB,
        sysRamPercent,
        nodeRamMB,
        activeCampaignsCount,
        totalQueuedSchedules,
        totalActiveWelcomes,
        totalOnline,
        totalSleeping,
        totalOffline,
        tenants: tenantsData
    });
});

app.get('/master-admin', (req, res) => {
    const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
    const pin = req.query.pin;

    if (!checkAdminBruteForce(clientIp)) {
        return res.status(429).send(`
            <body style="background:#0b141a;color:#ef4444;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;">
                <div style="background:#111b21;padding:24px;border-radius:12px;border:1px solid #ef4444;text-align:center;">
                    <h3>🛑 Security Lockdown Active</h3>
                    <p style="color:#8696a0;margin-top:8px;">Too many failed attempts. Try again in 15 minutes.</p>
                </div>
            </body>
        `);
    }

    if (pin !== ADMIN_MASTER_PIN) {
        if (pin) recordFailedAdminAttempt(clientIp);
        return res.send(`
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Master Admin Access</title>
                <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
                <style>
                    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
                    body { background: #0b141a; color: #e9edef; display: flex; align-items: center; justify-content: center; min-height: 100vh; padding: 16px; }
                    .login-card { background: #111b21; padding: 32px 24px; border-radius: 14px; border: 1px solid #2a3942; width: 100%; max-width: 380px; text-align: center; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
                    .login-card h3 { margin-bottom: 8px; font-size: 1.25rem; color: #00a884; }
                    .login-card p { font-size: 0.85rem; color: #8696a0; margin-bottom: 20px; }
                    .login-card input { width: 100%; padding: 12px 14px; border-radius: 8px; border: 1px solid #2a3942; background: #202c33; color: #fff; font-size: 1rem; outline: none; margin-bottom: 16px; text-align: center; letter-spacing: 2px; }
                    .login-card input:focus { border-color: #00a884; }
                    .login-card button { width: 100%; padding: 12px; border-radius: 8px; background: #00a884; color: #111b21; border: none; font-weight: 700; font-size: 1rem; cursor: pointer; transition: 0.2s; }
                    .login-card button:hover { background: #06cf9c; }
                </style>
            </head>
            <body>
                <div class="login-card">
                    <h3>⚡ Master Admin Portal</h3>
                    <p>Enter Master PIN to authenticate session</p>
                    <form method="GET">
                        <input type="password" name="pin" placeholder="Enter PIN" autofocus required>
                        <button type="submit">Unlock Dashboard</button>
                    </form>
                </div>
            </body>
            </html>
        `);
    }

    clearAdminAttempt(clientIp);

    const licenses = stmtGetAllLicensesSorted.all();
    const memory = process.memoryUsage();
    const nodeRamMB = (memory.heapUsed / 1024 / 1024).toFixed(1);

    const totalSysMemBytes = os.totalmem();
    const freeSysMemBytes = os.freemem();
    const usedSysMemBytes = totalSysMemBytes - freeSysMemBytes;
    const sysTotalRAMGB = (totalSysMemBytes / (1024 * 1024 * 1024)).toFixed(1);
    const sysUsedRAMGB = (usedSysMemBytes / (1024 * 1024 * 1024)).toFixed(1);
    const sysRamPercent = Math.round((usedSysMemBytes / totalSysMemBytes) * 100);

    let totalOnline = 0;
    let totalSleeping = 0;
    let totalOffline = 0;
    let totalStorageBytes = 0;
    let activeCampaignsCount = 0;
    let totalQueuedSchedules = 0;
    let totalActiveWelcomes = 0;

    const rows = licenses.map((data) => {
        const sid = data.sessionId;
        const daysLeft = Math.ceil((data.validUntil - Date.now()) / (1000 * 60 * 60 * 24));
        const isExpired = daysLeft <= 0;
        const session = sessions.get(sid);

        let socketStatus = 'OFFLINE';
        let socketColor = '#ef4444';
        if (session && session.status === 'CONNECTED') {
            socketStatus = 'ONLINE';
            socketColor = '#00a884';
            totalOnline++;
        } else if (session && session.status === 'SLEEPING') {
            socketStatus = 'SLEEPING';
            socketColor = '#eab308';
            totalSleeping++;
        } else {
            totalOffline++;
        }

        let dispatchBadge = '<span style="color:#8696a0;font-size:0.75rem;">Idle ⚪</span>';
        if (session && session.queue && session.queue.isRunning) {
            activeCampaignsCount++;
            const sent = session.queue.sent || 0;
            const total = session.queue.total || 0;
            const remaining = Math.max(0, total - sent - (session.queue.failed || 0));
            dispatchBadge = `<span style="color:#00a884;font-weight:bold;font-size:0.75rem;">⚡ Sending: ${sent}/${total} (${remaining} pending)</span>`;
        } else if (session && session.queue && session.queue.state === 'PAUSED') {
            const sent = session.queue.sent || 0;
            const total = session.queue.total || 0;
            dispatchBadge = `<span style="color:#f59e0b;font-weight:bold;font-size:0.75rem;">⏸️ Paused: ${sent}/${total}</span>`;
        }

        let schedBadge = '';
        try {
            const schedRows = stmtGetScheduledJobs.all(sid);
            const activeScheds = schedRows.filter(r => r.status === 'SCHEDULED');
            if (activeScheds.length > 0) {
                totalQueuedSchedules += activeScheds.length;
                schedBadge = `<div style="font-size:0.72rem;color:#38bdf8;font-weight:600;margin-top:2px;">⏰ Sched: ${activeScheds.length} queued</div>`;
            }
        } catch (e) {}

        let welBadge = '';
        try {
            const welRow = stmtGetWelcome.get(sid);
            if (welRow && welRow.configJson) {
                const wConfig = JSON.parse(welRow.configJson);
                if (wConfig.enabled) {
                    totalActiveWelcomes++;
                    welBadge = `<div style="font-size:0.72rem;color:#c084fc;font-weight:600;margin-top:2px;">🤖 Welcome: Active</div>`;
                }
            }
        } catch (e) {}

        const { tenantDir } = getTenantPaths(sid);
        const diskBytes = getFolderSizeInBytes(tenantDir);
        totalStorageBytes += diskBytes;
        const diskMB = (diskBytes / (1024 * 1024)).toFixed(1);

        const cleanPhone = String(data.phone || sid).replace(/[^0-9]/g, '');
        const joinedDateStr = data.createdAt ? new Date(data.createdAt).toLocaleDateString([], { dateStyle: 'short' }) : 'Earlier';

        return `
            <tr data-phone="${data.phone || sid}" id="row_${sid}" style="border-bottom:1px solid #202c33;">
                <td style="padding:10px;text-align:center;">
                    <input type="checkbox" class="tenant-chk" value="${sid}" onchange="updateFloatingBar()" style="cursor:pointer;width:18px;height:18px;">
                </td>
                <td style="padding:10px;font-weight:600;font-size:0.9rem;">
                    <div style="display:flex;align-items:center;gap:6px;">
                        <span>+${data.phone || sid}</span>
                        <a href="https://wa.me/${cleanPhone}" target="_blank" data-tooltip="Direct WhatsApp Chat" style="text-decoration:none;font-size:0.95rem;">💬</a>
                    </div>
                    <div style="font-size:0.72rem;color:#8696a0;">${sid} • Joined: ${joinedDateStr}</div>
                </td>
                <td style="padding:10px;" id="activity_col_${sid}">
                    <span id="socket_badge_${sid}" style="display:inline-flex;align-items:center;gap:6px;font-size:0.78rem;font-weight:bold;color:${socketColor};">
                        <span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:${socketColor};"></span>
                        ${socketStatus}
                    </span>
                    <div id="dispatch_badge_${sid}" style="margin-top:2px;">${dispatchBadge}</div>
                    <div id="sched_badge_${sid}">${schedBadge}</div>
                    <div id="wel_badge_${sid}">${welBadge}</div>
                </td>
                <td style="padding:10px;font-size:0.82rem;" id="sub_col_${sid}">
                    <div>${data.plan || 'Standard'}</div>
                    <div style="font-size:0.72rem;font-weight:bold;color:${data.status === 'ACTIVE' && !isExpired ? '#00a884' : '#ef4444'};">
                        ${data.status} • ${daysLeft > 0 ? daysLeft + 'd left' : 'Expired'}
                    </div>
                </td>
                <td style="padding:10px;font-size:0.82rem;color:#8696a0;">
                    ${diskMB} MB
                </td>
                <td style="padding:10px;">
                    <div style="display:flex;gap:5px;flex-wrap:wrap;">
                        <button onclick="extendLicense('${sid}', 30, 'Monthly (₹150)')" data-tooltip="Extend subscription by 30 days" style="padding:5px 8px;border-radius:6px;background:#00a884;color:#111b21;border:none;font-weight:700;font-size:0.72rem;cursor:pointer;">+30D</button>
                        <button onclick="extendLicense('${sid}', 365, 'Yearly (₹1200)')" data-tooltip="Extend subscription by 365 days" style="padding:5px 8px;border-radius:6px;background:#0284c7;color:#fff;border:none;font-weight:700;font-size:0.72rem;cursor:pointer;">+365D</button>
                        <button onclick="cancelSubscription('${sid}')" data-tooltip="Cancel subscription: Expires instantly, purges scheduled jobs, safely pauses welcome bot" style="padding:5px 8px;border-radius:6px;background:#dc2626;color:#fff;border:none;font-weight:700;font-size:0.72rem;cursor:pointer;">Cancel Sub</button>
                        <button onclick="safeReset('${sid}')" data-tooltip="Safe Reset: Unlinks socket only. Customer groups remain 100% safe." style="padding:5px 8px;border-radius:6px;background:#f59e0b;color:#111b21;border:none;font-weight:700;font-size:0.72rem;cursor:pointer;">Reset</button>
                        <button onclick="purgeMedia('${sid}')" data-tooltip="Purge orphaned media from vault" style="padding:5px 8px;border-radius:6px;background:#374151;color:#e5e7eb;border:none;font-weight:600;font-size:0.72rem;cursor:pointer;">Purge</button>
                        <button onclick="toggleKillSwitch('${sid}', '${data.status === 'ACTIVE' ? 'DISABLED' : 'ACTIVE'}')" data-tooltip="Toggle remote access killswitch" style="padding:5px 8px;border-radius:6px;background:${data.status === 'ACTIVE' ? '#4b5563' : '#10b981'};color:#fff;border:none;font-weight:600;font-size:0.72rem;cursor:pointer;">${data.status === 'ACTIVE' ? 'Kill' : 'Enable'}</button>
                        <button onclick="deleteTenant('${sid}')" data-tooltip="Permanent full wipe of database and media" style="padding:5px 8px;border-radius:6px;background:#ef4444;color:#fff;border:none;font-weight:700;font-size:0.72rem;cursor:pointer;">Wipe</button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');

    const totalStorageMB = (totalStorageBytes / (1024 * 1024)).toFixed(1);
    const noticeRow = stmtGetSetting.get('global_notice');
    const currentNotice = noticeRow ? noticeRow.value : '';

    res.send(`
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Master Control Dashboard</title>
            <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
            <style>
                * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
                body { background: #0b141a; color: #e9edef; padding: 14px; padding-bottom: 80px; }
                .container { max-width: 1100px; margin: 0 auto; }
                .top-bar { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; margin-bottom: 16px; }
                .brand { font-size: 1.2rem; font-weight: 700; color: #00a884; }
                .vitals-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 10px; margin-bottom: 16px; }
                .vital-card { background: #111b21; border: 1px solid #202c33; border-radius: 10px; padding: 10px 12px; }
                .vital-title { font-size: 0.72rem; color: #8696a0; text-transform: uppercase; margin-bottom: 4px; font-weight: 600; }
                .vital-val { font-size: 1.05rem; font-weight: 700; color: #e9edef; }
                .notice-box { background: #111b21; border: 1px solid #202c33; border-radius: 10px; padding: 10px 12px; margin-bottom: 14px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
                .search-box { width: 100%; padding: 11px 14px; background: #111b21; border: 1px solid #202c33; border-radius: 8px; color: #fff; font-size: 0.9rem; margin-bottom: 14px; outline: none; }
                .search-box:focus { border-color: #00a884; }
                .table-container { background: #111b21; border-radius: 12px; border: 1px solid #202c33; overflow-x: auto; }
                table { width: 100%; border-collapse: collapse; text-align: left; }
                th { background: #182229; padding: 10px; font-size: 0.74rem; color: #8696a0; text-transform: uppercase; font-weight: 600; border-bottom: 1px solid #202c33; }
                .btn-refresh { padding: 7px 12px; border-radius: 6px; background: #202c33; color: #e9edef; border: 1px solid #2a3942; font-weight: 600; font-size: 0.82rem; cursor: pointer; }
                .btn-restart { padding: 7px 12px; border-radius: 6px; background: #dc2626; color: #fff; border: none; font-weight: 700; font-size: 0.82rem; cursor: pointer; }
                .btn-backup { padding: 7px 12px; border-radius: 6px; background: #0284c7; color: #fff; border: none; font-weight: 700; font-size: 0.82rem; cursor: pointer; }
                .btn-merge { padding: 7px 12px; border-radius: 6px; background: #6366f1; color: #fff; border: none; font-weight: 700; font-size: 0.82rem; cursor: pointer; }

                /* CSS Micro-Tooltips */
                [data-tooltip] { position: relative; }
                [data-tooltip]:hover::after {
                    content: attr(data-tooltip); position: absolute; bottom: 125%; left: 50%;
                    transform: translateX(-50%); background: #182229; color: #e9edef; font-size: 0.7rem;
                    padding: 4px 8px; border-radius: 6px; border: 1px solid #00a884; white-space: nowrap;
                    z-index: 1000; box-shadow: 0 4px 12px rgba(0,0,0,0.5); pointer-events: none;
                }

                /* Floating Multi-Select Action Bar */
                .floating-bar {
                    position: fixed; bottom: 16px; left: 50%; transform: translateX(-50%);
                    background: #182229; border: 1px solid #00a884; box-shadow: 0 10px 30px rgba(0,0,0,0.8);
                    border-radius: 30px; padding: 10px 18px; display: none; align-items: center; gap: 10px; z-index: 1000;
                }
                .floating-btn { padding: 6px 12px; border-radius: 20px; font-weight: 700; font-size: 0.78rem; border: none; cursor: pointer; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="top-bar">
                    <div class="brand">⚡ WA Campaign Master Control</div>
                    <div style="display:flex;gap:8px;flex-wrap:wrap;">
                        <button class="btn-refresh" onclick="location.reload()">🔄 Refresh</button>
                        <button class="btn-merge" onclick="mergeDuplicates()">🧹 Merge Duplicates</button>
                        <button class="btn-backup" onclick="downloadDbBackup()">📥 Backup DB</button>
                        <button class="btn-restart" onclick="restartServer()">⚡ Restart Server</button>
                    </div>
                </div>

                <div class="vitals-grid">
                    <div class="vital-card">
                        <div class="vital-title">System RAM</div>
                        <div class="vital-val" id="vitalSysRam" style="color:#38bdf8;">${sysUsedRAMGB} / ${sysTotalRAMGB} GB (${sysRamPercent}%)</div>
                    </div>
                    <div class="vital-card">
                        <div class="vital-title">Node Process RAM</div>
                        <div class="vital-val" id="vitalNodeRam" style="color:#a78bfa;">${nodeRamMB} MB</div>
                    </div>
                    <div class="vital-card">
                        <div class="vital-title">Active Dispatches</div>
                        <div class="vital-val" id="vitalActiveDispatches" style="color:${activeCampaignsCount > 0 ? '#00a884' : '#8696a0'};">${activeCampaignsCount} Running</div>
                    </div>
                    <div class="vital-card">
                        <div class="vital-title">Queued Schedules</div>
                        <div class="vital-val" id="vitalQueuedSchedules" style="color:#38bdf8;">${totalQueuedSchedules} Jobs</div>
                    </div>
                    <div class="vital-card">
                        <div class="vital-title">Active Welcome Bots</div>
                        <div class="vital-val" id="vitalActiveWelcomes" style="color:#c084fc;">${totalActiveWelcomes} Tenants</div>
                    </div>
                    <div class="vital-card">
                        <div class="vital-title">Sockets (Online / Sleep)</div>
                        <div class="vital-val" id="vitalSockets"><span style="color:#00a884;">${totalOnline}</span> / <span style="color:#eab308;">${totalSleeping}</span></div>
                    </div>
                    <div class="vital-card">
                        <div class="vital-title">Total Disk Used</div>
                        <div class="vital-val" style="color:#f43f5e;">${totalStorageMB} MB</div>
                    </div>
                </div>

                <div class="notice-box">
                    <span style="font-size:0.82rem;font-weight:bold;color:#00a884;white-space:nowrap;">📢 Global Notice:</span>
                    <input type="text" id="txtGlobalNotice" value="${currentNotice.replace(/"/g, '&quot;')}" placeholder="Type maintenance notice or announcement..." style="flex:1;min-width:200px;padding:7px 10px;border-radius:6px;border:1px solid #202c33;background:#0b141a;color:#fff;font-size:0.85rem;outline:none;">
                    <select id="selNoticeDuration" style="padding:7px;border-radius:6px;border:1px solid #202c33;background:#0b141a;color:#fff;font-size:0.82rem;outline:none;">
                        <option value="0">Until Cleared</option>
                        <option value="2">2 Hours</option>
                        <option value="6">6 Hours</option>
                        <option value="24">24 Hours</option>
                    </select>
                    <button onclick="saveGlobalNotice()" style="padding:7px 12px;border-radius:6px;background:#00a884;color:#111b21;border:none;font-weight:700;font-size:0.8rem;cursor:pointer;">Broadcast</button>
                    <button onclick="clearGlobalNotice()" style="padding:7px 12px;border-radius:6px;background:#ef4444;color:#fff;border:none;font-weight:700;font-size:0.8rem;cursor:pointer;">🛑 Clear</button>
                </div>

                <input type="text" id="searchInput" class="search-box" placeholder="🔍 Search by phone number or tenant ID..." onkeyup="filterTenants()">

                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th style="width:36px;text-align:center;"><input type="checkbox" id="chkSelectAll" onclick="toggleSelectAllTenants(this)" style="cursor:pointer;width:18px;height:18px;"></th>
                                <th>User / Tenant (Newest First)</th>
                                <th>Socket & Activity</th>
                                <th>Subscription</th>
                                <th>Disk</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody id="tenantBody">
                            ${rows || '<tr><td colspan="6" style="padding:24px;text-align:center;color:#8696a0;">No tenants registered yet.</td></tr>'}
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Floating Multi-Action Bar -->
            <div class="floating-bar" id="floatingBar">
                <span id="lblSelectedCount" style="font-size:0.82rem;font-weight:bold;color:#00a884;">0 Selected</span>
                <button class="floating-btn" style="background:#00a884;color:#111b21;" onclick="bulkExtend(30)">+30D</button>
                <button class="floating-btn" style="background:#f59e0b;color:#111b21;" onclick="bulkSafeReset()">Safe Reset</button>
                <button class="floating-btn" style="background:#374151;color:#fff;" onclick="bulkPurgeMedia()">Purge Media</button>
                <button class="floating-btn" style="background:#dc2626;color:#fff;" onclick="bulkKill()">Kill / Freeze</button>
            </div>

            <script>
                function filterTenants() {
                    const q = document.getElementById('searchInput').value.toLowerCase().trim();
                    const rows = document.querySelectorAll('#tenantBody tr');
                    rows.forEach(r => {
                        const phone = r.getAttribute('data-phone') || '';
                        r.style.display = phone.toLowerCase().includes(q) ? '' : 'none';
                    });
                }

                function toggleSelectAllTenants(master) {
                    const checkboxes = document.querySelectorAll('.tenant-chk');
                    checkboxes.forEach(c => {
                        if (c.closest('tr').style.display !== 'none') c.checked = master.checked;
                    });
                    updateFloatingBar();
                }

                function updateFloatingBar() {
                    const checked = document.querySelectorAll('.tenant-chk:checked');
                    const bar = document.getElementById('floatingBar');
                    if (checked.length > 0) {
                        bar.style.display = 'flex';
                        document.getElementById('lblSelectedCount').innerText = checked.length + ' Selected';
                    } else {
                        bar.style.display = 'none';
                        document.getElementById('chkSelectAll').checked = false;
                    }
                }

                function getSelectedSessionIds() {
                    return Array.from(document.querySelectorAll('.tenant-chk:checked')).map(c => c.value);
                }

                async function extendLicense(sessionId, days, plan) {
                    if (!confirm('Extend subscription by ' + days + ' days (' + plan + ')?')) return;
                    await fetch('/master-admin/extend', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', sessionId, days, plan })
                    });
                    pollMetrics();
                }

                async function cancelSubscription(sessionId) {
                    if (!confirm('CANCEL SUBSCRIPTION for this tenant?\\n\\n- Subscription will expire immediately.\\n- All upcoming scheduled campaigns and media will be PURGED.\\n- Active campaign will be STOPPED.\\n- Welcome bot rules will be safely preserved but PAUSED.\\n\\nProceed?')) return;
                    const res = await fetch('/master-admin/cancel-sub', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', sessionId })
                    });
                    const d = await res.json();
                    alert(d.message || 'Subscription cancelled.');
                    pollMetrics();
                }

                async function safeReset(sessionId) {
                    if (!confirm('Safe Reset will unlink WhatsApp session keys so user can re-pair.\\n\\nCustomer groups and contacts are 100% PRESERVED.\\n\\nProceed?')) return;
                    await fetch('/master-admin/safe-reset', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', sessionId })
                    });
                    alert('Session reset safely. Groups preserved.');
                    pollMetrics();
                }

                async function purgeMedia(sessionId) {
                    if (!confirm('Purge orphaned campaign media for this user?\\n\\nGroups and sessions will NOT be touched.')) return;
                    await fetch('/master-admin/purge-media', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', sessionId })
                    });
                    alert('Media vault purged.');
                    pollMetrics();
                }

                async function toggleKillSwitch(sessionId, newStatus) {
                    const msg = newStatus === 'DISABLED'
                        ? 'FREEZE / KILL this user?\\n\\nTheir WhatsApp socket will be disconnected and their app access blocked immediately.'
                        : 'Unfreeze and RE-ENABLE this user?';
                    if (!confirm(msg)) return;

                    await fetch('/master-admin/toggle-status', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', sessionId, status: newStatus })
                    });
                    pollMetrics();
                }

                async function deleteTenant(sessionId) {
                    if (!confirm('CRITICAL ACTION: Permanently wipe this tenant, including ALL contact groups and media?\\n\\nThis CANNOT be undone.')) return;
                    await fetch('/master-admin/delete-tenant', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', sessionId })
                    });
                    location.reload();
                }

                async function saveGlobalNotice() {
                    const notice = document.getElementById('txtGlobalNotice').value.trim();
                    const hours = parseInt(document.getElementById('selNoticeDuration').value, 10);
                    await fetch('/master-admin/save-notice', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', notice, hours })
                    });
                    alert('Broadcast notice saved successfully.');
                }

                async function clearGlobalNotice() {
                    await fetch('/master-admin/save-notice', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', notice: '', hours: 0 })
                    });
                    document.getElementById('txtGlobalNotice').value = '';
                    alert('Broadcast cleared.');
                }

                async function mergeDuplicates() {
                    if (!confirm('Scan and safely merge all duplicate tenant records?\\n\\nContacts and active sockets will be preserved into primary IDs.')) return;
                    const res = await fetch('/master-admin/merge-duplicates', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}' })
                    });
                    const d = await res.json();
                    alert(d.message || 'Merge completed.');
                    location.reload();
                }

                function downloadDbBackup() {
                    window.location.href = '/master-admin/backup-db?pin=${ADMIN_MASTER_PIN}';
                }

                async function restartServer() {
                    if (!confirm('Restart the core server process now?\\n\\nActive campaigns will pause and safely resume automatically.')) return;
                    await fetch('/master-admin/restart-server', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}' })
                    });
                    setTimeout(() => location.reload(), 3000);
                }

                // --- Bulk Actions ---
                async function bulkExtend(days) {
                    const sids = getSelectedSessionIds();
                    if (!confirm('Extend subscription by ' + days + ' days for ' + sids.length + ' selected tenants?')) return;
                    for (const sid of sids) {
                        await fetch('/master-admin/extend', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', sessionId: sid, days })
                        });
                    }
                    pollMetrics();
                }

                async function bulkSafeReset() {
                    const sids = getSelectedSessionIds();
                    if (!confirm('Perform Safe Reset for ' + sids.length + ' tenants?\\n\\nAll customer groups will remain 100% SAFE.')) return;
                    for (const sid of sids) {
                        await fetch('/master-admin/safe-reset', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', sessionId: sid })
                        });
                    }
                    pollMetrics();
                }

                async function bulkPurgeMedia() {
                    const sids = getSelectedSessionIds();
                    if (!confirm('Purge orphaned media for ' + sids.length + ' tenants to reclaim disk space?')) return;
                    for (const sid of sids) {
                        await fetch('/master-admin/purge-media', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', sessionId: sid })
                        });
                    }
                    pollMetrics();
                }

                async function bulkKill() {
                    const sids = getSelectedSessionIds();
                    if (!confirm('FREEZE / KILL ' + sids.length + ' selected tenants at once?')) return;
                    for (const sid of sids) {
                        await fetch('/master-admin/toggle-status', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ pin: '${ADMIN_MASTER_PIN}', sessionId: sid, status: 'DISABLED' })
                        });
                    }
                    pollMetrics();
                }

                // --- Silent Background Live Polling (Zero Screen Flash) ---
                async function pollMetrics() {
                    try {
                        const res = await fetch('/master-admin/live-metrics?pin=${ADMIN_MASTER_PIN}');
                        if (!res.ok) return;
                        const data = await res.json();

                        document.getElementById('vitalSysRam').innerText = data.sysUsedRAMGB + ' / ' + data.sysTotalRAMGB + ' GB (' + data.sysRamPercent + '%)';
                        document.getElementById('vitalNodeRam').innerText = data.nodeRamMB + ' MB';
                        
                        const actEl = document.getElementById('vitalActiveDispatches');
                        actEl.innerText = data.activeCampaignsCount + ' Running';
                        actEl.style.color = data.activeCampaignsCount > 0 ? '#00a884' : '#8696a0';

                        document.getElementById('vitalQueuedSchedules').innerText = data.totalQueuedSchedules + ' Jobs';
                        document.getElementById('vitalActiveWelcomes').innerText = data.totalActiveWelcomes + ' Tenants';
                        document.getElementById('vitalSockets').innerHTML = '<span style="color:#00a884;">' + data.totalOnline + '</span> / <span style="color:#eab308;">' + data.totalSleeping + '</span>';

                        if (data.tenants) {
                            for (const [sid, t] of Object.entries(data.tenants)) {
                                const sockBadge = document.getElementById('socket_badge_' + sid);
                                if (sockBadge) {
                                    sockBadge.style.color = t.socketColor;
                                    sockBadge.innerHTML = '<span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:' + t.socketColor + ';"></span> ' + t.socketStatus;
                                }

                                const dispBadge = document.getElementById('dispatch_badge_' + sid);
                                if (dispBadge) dispBadge.innerHTML = t.dispatchHtml;

                                const schedBadge = document.getElementById('sched_badge_' + sid);
                                if (schedBadge) schedBadge.innerHTML = t.schedHtml;

                                const welBadge = document.getElementById('wel_badge_' + sid);
                                if (welBadge) welBadge.innerHTML = t.welHtml;

                                const subCol = document.getElementById('sub_col_' + sid);
                                if (subCol) {
                                    const subColor = (t.status === 'ACTIVE' && !t.isExpired) ? '#00a884' : '#ef4444';
                                    const subLabel = t.daysLeft > 0 ? (t.daysLeft + 'd left') : 'Expired';
                                    subCol.innerHTML = '<div>' + (t.plan || 'Standard') + '</div><div style="font-size:0.72rem;font-weight:bold;color:' + subColor + ';">' + t.status + ' • ' + subLabel + '</div>';
                                }
                            }
                        }
                    } catch (e) {}
                }

                // Poll every 3 seconds silently
                setInterval(pollMetrics, 3000);
            </script>
        </body>
        </html>
    `);
});

app.post('/master-admin/save-notice', (req, res) => {
    const { pin, notice = '', hours = 0 } = req.body;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });
    const now = Date.now();
    const expiryEpoch = hours > 0 ? (now + hours * 60 * 60 * 1000) : 0;
    stmtSaveSetting.run('global_notice', notice, now);
    stmtSaveSetting.run('global_notice_expiry', String(expiryEpoch), now);
    res.json({ success: true });
});

app.post('/master-admin/merge-duplicates', (req, res) => {
    const { pin } = req.body;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });

    try {
        const allLicenses = stmtGetAllLicensesSorted.all();
        const phoneGroups = new Map();

        allLicenses.forEach(lic => {
            const clean = sanitizeToValidWhatsAppJid(lic.phone || lic.sessionId);
            if (!clean) return;
            if (!phoneGroups.has(clean)) phoneGroups.set(clean, []);
            phoneGroups.get(clean).push(lic);
        });

        let mergedCount = 0;

        // Read legacy JSON file if present to purge duplicates from disk
        let legacyLicObj = safeReadJsonSync(LICENSES_FILE, null);

        for (const [phone, tenantList] of phoneGroups.entries()) {
            if (tenantList.length > 1) {
                // Pick primary: online socket wins, else newest validUntil
                tenantList.sort((a, b) => {
                    const sessA = sessions.get(a.sessionId);
                    const sessB = sessions.get(b.sessionId);
                    const scoreA = (sessA?.status === 'CONNECTED' ? 10 : 0) + (a.sessionId === phone ? 5 : 0);
                    const scoreB = (sessB?.status === 'CONNECTED' ? 10 : 0) + (b.sessionId === phone ? 5 : 0);
                    if (scoreA !== scoreB) return scoreB - scoreA;
                    return b.validUntil - a.validUntil;
                });

                const primary = tenantList[0];
                const duplicates = tenantList.slice(1);

                // Collect contacts from duplicates into primary
                let primaryGroups = [];
                const pRow = stmtGetGroups.get(primary.sessionId);
                if (pRow && pRow.groupsJson) {
                    try { primaryGroups = JSON.parse(pRow.groupsJson); } catch (e) {}
                }

                duplicates.forEach(dup => {
                    const dRow = stmtGetGroups.get(dup.sessionId);
                    if (dRow && dRow.groupsJson) {
                        try {
                            const dGroups = JSON.parse(dRow.groupsJson);
                            dGroups.forEach(dg => {
                                const exists = primaryGroups.find(pg => pg.name === dg.name);
                                if (!exists) primaryGroups.push(dg);
                            });
                        } catch (e) {}
                    }

                    // 1. Remove duplicate from SQLite
                    stmtDeleteLicense.run(dup.sessionId);
                    wipeSessionData(dup.sessionId);

                    // 2. Permanently delete duplicate folder from disk
                    const { tenantDir } = getTenantPaths(dup.sessionId);
                    if (fs.existsSync(tenantDir)) {
                        try { fs.rmSync(tenantDir, { recursive: true, force: true }); } catch (e) {}
                    }

                    // 3. Remove duplicate from legacy JSON if it exists
                    if (legacyLicObj && legacyLicObj[dup.sessionId]) {
                        delete legacyLicObj[dup.sessionId];
                    }

                    mergedCount++;
                });

                stmtSaveGroups.run(primary.sessionId, JSON.stringify(primaryGroups), Date.now());
            }
        }

        // Save sanitized legacy JSON so it can never resurrect
        if (legacyLicObj) {
            safeWriteJsonSync(LICENSES_FILE, legacyLicObj);
        }

        res.json({ success: true, message: `Successfully merged and permanently deleted ${mergedCount} duplicate tenant(s) from DB and disk.` });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post('/master-admin/extend', (req, res) => {
    const { pin, sessionId, days = 30, plan = 'Monthly (₹150)' } = req.body;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });
    const lic = stmtGetLicense.get(sessionId);
    const now = Date.now();
    if (lic) {
        const base = Math.max(now, lic.validUntil);
        const newValidUntil = base + (parseInt(days, 10) * 24 * 60 * 60 * 1000);
        stmtUpsertLicense.run({
            sessionId,
            phone: lic.phone,
            status: 'ACTIVE',
            plan: plan || lic.plan,
            tenantToken: lic.tenantToken,
            validUntil: newValidUntil,
            notes: lic.notes || '',
            createdAt: lic.createdAt || now,
            purchasedAt: now
        });
    }
    res.json({ success: true });
});

// Cancel Subscription Endpoint: Immediate Expiry + Purge Scheduled Campaigns + Safe-Pause Welcome Bot
app.post('/master-admin/cancel-sub', (req, res) => {
    const { pin, sessionId } = req.body;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });

    const lic = stmtGetLicense.get(sessionId);
    const now = Date.now();
    if (lic) {
        // 1. Hard expire license immediately
        stmtUpsertLicense.run({
            sessionId,
            phone: lic.phone,
            status: 'ACTIVE',
            plan: lic.plan,
            tenantToken: lic.tenantToken,
            validUntil: now - 1000,
            notes: 'Subscription Cancelled by Admin',
            createdAt: lic.createdAt || now,
            purchasedAt: lic.purchasedAt || now
        });

        // 2. Stop any running in-flight campaign and purge its media
        const session = sessions.get(sessionId);
        if (session && session.queue) {
            session.queue.state = 'STOPPED';
            session.queue.isRunning = false;
            purgeCampaignMediaItems(session.queue.sequence || session.queue.mediaItems);
            session.queue.sequence = [];
            session.queue.mediaItems = [];
            upsertCampaignHistory(sessionId, session.queue);
            clearActiveCampaignState(sessionId);
        }

        // 3. Purge all upcoming scheduled jobs from DB and delete attached media files
        const scheduledRows = stmtGetScheduledJobs.all(sessionId);
        scheduledRows.forEach(row => {
            try {
                const j = JSON.parse(row.jobDataJson);
                purgeCampaignMediaItems(j.sequence || j.mediaItems);
            } catch (e) {}
        });
        stmtDeleteAllScheduledJobsForSession.run(sessionId);

        const { scheduledJobsFile } = getTenantPaths(sessionId);
        safeWriteJsonSync(scheduledJobsFile, []);

        // 4. Clean any orphaned media in vault
        cleanOrphanedMediaVaultFiles(sessionId);

        // Note: Welcome rules and customer groups are 100% PRESERVED, but welcome responder will not reply due to expired license.
        return res.json({ success: true, message: 'Subscription cancelled. Scheduled jobs purged and active campaign stopped. Groups & welcome config preserved.' });
    }
    res.status(404).json({ error: 'Tenant not found.' });
});

app.post('/master-admin/safe-reset', (req, res) => {
    const { pin, sessionId } = req.body;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });
    safeResetTenantSession(sessionId);
    res.json({ success: true, message: 'Session reset safely. Groups preserved.' });
});

app.post('/master-admin/purge-media', (req, res) => {
    const { pin, sessionId } = req.body;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });
    cleanOrphanedMediaVaultFiles(sessionId);
    res.json({ success: true, message: 'Media vault purged.' });
});

app.post('/master-admin/toggle-status', (req, res) => {
    const { pin, sessionId, status } = req.body;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });
    const lic = stmtGetLicense.get(sessionId);
    if (lic) {
        stmtUpsertLicense.run({
            sessionId,
            phone: lic.phone,
            status: status,
            plan: lic.plan,
            tenantToken: lic.tenantToken,
            validUntil: lic.validUntil,
            notes: lic.notes || '',
            createdAt: lic.createdAt || Date.now(),
            purchasedAt: lic.purchasedAt || Date.now()
        });
        if (status === 'DISABLED') {
            putSocketToSleep(sessionId);
        }
    }
    res.json({ success: true });
});

app.post('/master-admin/delete-tenant', (req, res) => {
    const { pin, sessionId } = req.body;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });
    stmtDeleteLicense.run(sessionId);

    const { tenantDir } = getTenantPaths(sessionId);
    if (fs.existsSync(tenantDir)) {
        try { fs.rmSync(tenantDir, { recursive: true, force: true }); } catch (e) {}
    }
    wipeSessionData(sessionId);
    res.json({ success: true });
});

app.post('/master-admin/restart-server', (req, res) => {
    const { pin } = req.body;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });
    res.json({ success: true, message: 'Restarting engine...' });
    setTimeout(() => {
        process.exit(0);
    }, 1000);
});

app.listen(PORT, '0.0.0.0', () => console.log(`[SERVER] Ultra-Optimized Enterprise Engine Online on port ${PORT}`));