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

const DAILY_OUTBOUND_CAP = 300;
const MAX_SCHEDULE_SLOTS = 1;
const MAX_SCHEDULE_WINDOW_MS = 3 * 24 * 60 * 60 * 1000; // 72 hours (3 days)
const MEDIA_POOL_MIN = 5;
const MEDIA_POOL_MAX = 10;
const VIDEO_POOL_CONCURRENCY = 1;

const BASE_TENANTS_DIR = path.join(__dirname, 'tenants');
const LICENSES_FILE = path.join(__dirname, 'admin_licenses.json');
const DB_FILE = path.join(__dirname, 'engine_data.sqlite');

if (!fs.existsSync(BASE_TENANTS_DIR)) fs.mkdirSync(BASE_TENANTS_DIR, { recursive: true });

// Zero-Crash Process Guard
process.on('uncaughtException', (err) => {
    console.error('[CRITICAL UNCAUGHT EXCEPTION]:', err);
});
process.on('unhandledRejection', (reason) => {
    console.error('[CRITICAL UNHANDLED REJECTION]:', reason);
});

// In-Memory Brute-Force Rate Limiter for Master Admin (Bounded)
const adminLoginAttempts = new Map();
const MAX_ADMIN_ATTEMPT_ENTRIES = 2000;

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
    if (!adminLoginAttempts.has(clientIp) && adminLoginAttempts.size >= MAX_ADMIN_ATTEMPT_ENTRIES) {
        const oldestKey = adminLoginAttempts.keys().next().value;
        if (oldestKey) adminLoginAttempts.delete(oldestKey);
    }
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

    CREATE TABLE IF NOT EXISTS daily_outbound_quota (
        tenantId TEXT PRIMARY KEY,
        dayKey TEXT NOT NULL,
        deliveredTodayCount INTEGER NOT NULL DEFAULT 0,
        updatedAt INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS welcome_dedup (
        tenantId TEXT NOT NULL,
        remoteJid TEXT NOT NULL,
        sentAt INTEGER NOT NULL,
        PRIMARY KEY (tenantId, remoteJid)
    );

    CREATE TABLE IF NOT EXISTS weekly_metrics (
        tenantId TEXT NOT NULL,
        weekKey TEXT NOT NULL,
        dayKey TEXT NOT NULL,
        sentCount INTEGER NOT NULL DEFAULT 0,
        failedCount INTEGER NOT NULL DEFAULT 0,
        pendingCount INTEGER NOT NULL DEFAULT 0,
        updatedAt INTEGER NOT NULL,
        PRIMARY KEY (tenantId, weekKey, dayKey)
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
const stmtGetAllScheduledJobs = db.prepare('SELECT * FROM scheduled_jobs WHERE status = ?');
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
const stmtPruneCampaignHistory = db.prepare(`
    DELETE FROM campaign_history
    WHERE sessionId = ?
      AND campaignId NOT IN (
          SELECT campaignId FROM campaign_history WHERE sessionId = ? ORDER BY createdAt DESC LIMIT 3
      )
`);

const stmtGetSetting = db.prepare('SELECT value FROM global_settings WHERE key = ?');
const stmtSaveSetting = db.prepare(`
    INSERT INTO global_settings (key, value, updatedAt)
    VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET
        value = excluded.value,
        updatedAt = excluded.updatedAt
`);

const stmtGetDailyQuota = db.prepare('SELECT * FROM daily_outbound_quota WHERE tenantId = ?');
const stmtUpsertDailyQuota = db.prepare(`
    INSERT INTO daily_outbound_quota (tenantId, dayKey, deliveredTodayCount, updatedAt)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(tenantId) DO UPDATE SET
        dayKey = excluded.dayKey,
        deliveredTodayCount = excluded.deliveredTodayCount,
        updatedAt = excluded.updatedAt
`);

const stmtGetWelcomeDedup = db.prepare('SELECT * FROM welcome_dedup WHERE tenantId = ? AND remoteJid = ?');
const stmtInsertWelcomeDedup = db.prepare(`
    INSERT INTO welcome_dedup (tenantId, remoteJid, sentAt)
    VALUES (?, ?, ?)
    ON CONFLICT(tenantId, remoteJid) DO NOTHING
`);
const stmtDeleteWelcomeDedup = db.prepare('DELETE FROM welcome_dedup WHERE tenantId = ? AND remoteJid = ?');
const stmtPruneOldWelcomeDedup = db.prepare('DELETE FROM welcome_dedup WHERE sentAt < ?');

const stmtGetWeeklyMetricDay = db.prepare('SELECT * FROM weekly_metrics WHERE tenantId = ? AND weekKey = ? AND dayKey = ?');
const stmtUpsertWeeklyMetricDay = db.prepare(`
    INSERT INTO weekly_metrics (tenantId, weekKey, dayKey, sentCount, failedCount, pendingCount, updatedAt)
    VALUES (@tenantId, @weekKey, @dayKey, @sentCount, @failedCount, @pendingCount, @updatedAt)
    ON CONFLICT(tenantId, weekKey, dayKey) DO UPDATE SET
        sentCount = excluded.sentCount,
        failedCount = excluded.failedCount,
        pendingCount = excluded.pendingCount,
        updatedAt = excluded.updatedAt
`);
const stmtGetWeeklyMetricForWeek = db.prepare('SELECT * FROM weekly_metrics WHERE tenantId = ? AND weekKey = ? ORDER BY dayKey ASC');
const stmtPruneOldWeeklyMetrics = db.prepare('DELETE FROM weekly_metrics WHERE weekKey != ?');

// ============================================================================
// DATE / TIME KEYS, DAILY 300 QUOTA & WEEKLY METRICS HELPERS
// ============================================================================
function getLocalDayKey(dateObj = new Date()) {
    const y = dateObj.getFullYear();
    const m = String(dateObj.getMonth() + 1).padStart(2, '0');
    const d = String(dateObj.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
}

function getLocalWeekKey(dateObj = new Date()) {
    // ISO-like Monday-anchored week key: YYYY-Www
    const copy = new Date(dateObj.getFullYear(), dateObj.getMonth(), dateObj.getDate());
    const dayNum = copy.getDay() || 7; // Monday = 1 .. Sunday = 7
    copy.setDate(copy.getDate() + 4 - dayNum);
    const yearStart = new Date(copy.getFullYear(), 0, 1);
    const weekNo = Math.ceil((((copy - yearStart) / 86400000) + 1) / 7);
    return `${copy.getFullYear()}-W${String(weekNo).padStart(2, '0')}`;
}

function getMsUntilNextMidnight() {
    const now = new Date();
    const nextMidnight = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1, 0, 0, 0, 0);
    return Math.max(1000, nextMidnight.getTime() - now.getTime());
}

function getTenantDailyQuotaState(tenantId) {
    const todayKey = getLocalDayKey();
    const now = Date.now();
    const row = stmtGetDailyQuota.get(tenantId);

    if (!row || row.dayKey !== todayKey) {
        stmtUpsertDailyQuota.run(tenantId, todayKey, 0, now);
        return {
            tenantId,
            dayKey: todayKey,
            deliveredTodayCount: 0,
            remainingQuota: DAILY_OUTBOUND_CAP
        };
    }

    const delivered = Math.max(0, Number(row.deliveredTodayCount) || 0);
    return {
        tenantId,
        dayKey: todayKey,
        deliveredTodayCount: delivered,
        remainingQuota: Math.max(0, DAILY_OUTBOUND_CAP - delivered)
    };
}

function recordDeliveredCampaignMessage(tenantId) {
    const todayKey = getLocalDayKey();
    const now = Date.now();
    const current = getTenantDailyQuotaState(tenantId);
    const nextDelivered = Math.min(DAILY_OUTBOUND_CAP, current.deliveredTodayCount + 1);
    stmtUpsertDailyQuota.run(tenantId, todayKey, nextDelivered, now);
    return {
        deliveredTodayCount: nextDelivered,
        remainingQuota: Math.max(0, DAILY_OUTBOUND_CAP - nextDelivered)
    };
}

function resetAllDailyQuotasAtMidnight() {
    const todayKey = getLocalDayKey();
    const now = Date.now();
    const licenses = stmtGetAllLicensesSorted.all();
    const tx = db.transaction((items) => {
        for (const lic of items) {
            stmtUpsertDailyQuota.run(lic.sessionId, todayKey, 0, now);
        }
    });
    tx(licenses);
    console.log(`[QUOTA MIDNIGHT RESET] Reset deliveredTodayCount to 0 for dayKey=${todayKey}`);
}

let midnightResetTimeout = null;
function scheduleDailyMidnightReset() {
    if (midnightResetTimeout) clearTimeout(midnightResetTimeout);
    const msLeft = getMsUntilNextMidnight();
    midnightResetTimeout = setTimeout(() => {
        try {
            resetAllDailyQuotasAtMidnight();
        } catch (e) {
            console.error('[MIDNIGHT RESET ERROR]:', e);
        }
        scheduleDailyMidnightReset();
    }, msLeft);
}
scheduleDailyMidnightReset();

function incrementWeeklyMetric(tenantId, field, delta = 1) {
    const nowDate = new Date();
    const dayKey = getLocalDayKey(nowDate);
    const weekKey = getLocalWeekKey(nowDate);
    const now = Date.now();

    const existing = stmtGetWeeklyMetricDay.get(tenantId, weekKey, dayKey) || {
        sentCount: 0,
        failedCount: 0,
        pendingCount: 0
    };

    const next = {
        tenantId,
        weekKey,
        dayKey,
        sentCount: Math.max(0, existing.sentCount + (field === 'sent' ? delta : 0)),
        failedCount: Math.max(0, existing.failedCount + (field === 'failed' ? delta : 0)),
        pendingCount: Math.max(0, existing.pendingCount + (field === 'pending' ? delta : 0)),
        updatedAt: now
    };
    stmtUpsertWeeklyMetricDay.run(next);
}

function setWeeklyPendingMetric(tenantId, pendingVal) {
    const nowDate = new Date();
    const dayKey = getLocalDayKey(nowDate);
    const weekKey = getLocalWeekKey(nowDate);
    const now = Date.now();

    const existing = stmtGetWeeklyMetricDay.get(tenantId, weekKey, dayKey) || {
        sentCount: 0,
        failedCount: 0,
        pendingCount: 0
    };

    stmtUpsertWeeklyMetricDay.run({
        tenantId,
        weekKey,
        dayKey,
        sentCount: existing.sentCount,
        failedCount: existing.failedCount,
        pendingCount: Math.max(0, Number(pendingVal) || 0),
        updatedAt: now
    });
}

function getTenantWeeklySummary(tenantId) {
    const weekKey = getLocalWeekKey();
    const days = stmtGetWeeklyMetricForWeek.all(tenantId, weekKey);
    let totalSent = 0;
    let totalFailed = 0;
    let totalPending = 0;

    const perDay = days.map(d => {
        totalSent += d.sentCount || 0;
        totalFailed += d.failedCount || 0;
        totalPending += d.pendingCount || 0;
        return {
            dayKey: d.dayKey,
            sent: d.sentCount || 0,
            failed: d.failedCount || 0,
            pending: d.pendingCount || 0
        };
    });

    return {
        weekKey,
        totalSent,
        totalFailed,
        totalPending,
        perDay
    };
}

// ============================================================================
// PER-TENANT CONCURRENCY LOCKS & VIDEO POOL SEMAPHORE
// ============================================================================
const tenantOperationLocks = new Map();

async function withTenantLock(sessionId, fn) {
    const prev = tenantOperationLocks.get(sessionId) || Promise.resolve();
    let releaseCurrent;
    const current = new Promise((resolve) => {
        releaseCurrent = resolve;
    });
    const chained = prev.then(() => current);
    tenantOperationLocks.set(sessionId, chained);

    await prev;
    try {
        return await fn();
    } finally {
        releaseCurrent();
        if (tenantOperationLocks.get(sessionId) === chained) {
            tenantOperationLocks.delete(sessionId);
        }
    }
}

let activeVideoPoolJobs = 0;
const videoPoolWaiters = [];

async function acquireVideoSlot() {
    if (activeVideoPoolJobs < VIDEO_POOL_CONCURRENCY) {
        activeVideoPoolJobs++;
        return;
    }
    await new Promise((resolve) => videoPoolWaiters.push(resolve));
    activeVideoPoolJobs++;
}

function releaseVideoSlot() {
    activeVideoPoolJobs = Math.max(0, activeVideoPoolJobs - 1);
    const next = videoPoolWaiters.shift();
    if (next) next();
}

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
            return;
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
                                    parsedHist.forEach((item) => {
                                        if (item && item.isWelcome) return; // Welcome messages never stored in campaign history
                                        stmtUpsertCampaignHistory.run({
                                            campaignId: item.id || ('mig_' + Date.now()),
                                            sessionId: sid,
                                            timestamp: item.timestamp || new Date().toLocaleString(),
                                            dataJson: JSON.stringify(item),
                                            createdAt: Date.now()
                                        });
                                    });
                                    stmtPruneCampaignHistory.run(sid, sid);
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
    limits: { fileSize: 15 * 1024 * 1024, files: 4 },
    fileFilter: (req, file, cb) => {
        const allowedMimes = ['image/jpeg', 'image/png', 'image/webp', 'video/mp4'];
        const ext = path.extname(file.originalname).toLowerCase();
        const allowedExts = ['.jpg', '.jpeg', '.png', '.webp', '.mp4'];
        if (allowedMimes.includes(file.mimetype) && allowedExts.includes(ext)) {
            cb(null, true);
        } else {
            cb(new Error('INVALID_FILE_TYPE'));
        }
    }
});

app.use(express.json({ limit: '5mb' }));
app.use(express.urlencoded({ extended: true, limit: '5mb' }));
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

function sanitizeToValidWhatsAppJid(rawInput) {
    let clean = String(rawInput || '').replace(/[^0-9]/g, '');
    if (!clean) return null;
    if (clean.length === 11 && clean.startsWith('0')) clean = clean.substring(1);
    if (clean.length === 10) clean = '91' + clean;
    if (clean.length < 10 || clean.length > 15) return null;
    return clean;
}

function normalizeRecipientJid(rawJidOrPhone) {
    const str = String(rawJidOrPhone || '').trim();
    if (!str) return null;
    if (str.endsWith('@s.whatsapp.net') || str.endsWith('@lid')) {
        return str;
    }
    const clean = sanitizeToValidWhatsAppJid(str);
    return clean ? `${clean}@s.whatsapp.net` : null;
}

function injectZeroWidthRandomizers(text) {
    if (!text || typeof text !== 'string') return text || '';
    const lines = text.split('\n');
    const randomizedLines = lines.map((line) => {
        const words = line.split(' ');
        const randomizedWords = words.map((word) => {
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

// Bounded media variation pool (5-10 varied copies per campaign) to avoid per-recipient re-generation
async function buildBoundedMediaPool(tenantId, sourceFilePath, mimeType) {
    const { mediaVaultDir } = getTenantPaths(tenantId);
    const isVideo = (mimeType || '').startsWith('video/');
    const poolSize = Math.floor(Math.random() * (MEDIA_POOL_MAX - MEDIA_POOL_MIN + 1)) + MEDIA_POOL_MIN;
    const poolPaths = [];

    if (!sourceFilePath || !fs.existsSync(sourceFilePath)) return poolPaths;

    if (isVideo) {
        await acquireVideoSlot();
    }

    try {
        const ext = path.extname(sourceFilePath) || (isVideo ? '.mp4' : '.jpg');
        for (let i = 0; i < poolSize; i++) {
            const variantName = `pool_${Date.now()}_${i}_${crypto.randomBytes(3).toString('hex')}${ext}`;
            const variantPath = path.join(mediaVaultDir, variantName);
            const padBytes = crypto.randomBytes(Math.floor(Math.random() * 12) + 4);

            // Stream/copy from disk with bounded variant marker without keeping huge buffers long-term
            await new Promise((resolve, reject) => {
                const readStream = fs.createReadStream(sourceFilePath);
                const writeStream = fs.createWriteStream(variantPath);
                readStream.on('error', reject);
                writeStream.on('error', reject);
                writeStream.on('finish', () => {
                    try {
                        fs.appendFileSync(variantPath, padBytes);
                        resolve();
                    } catch (err) {
                        reject(err);
                    }
                });
                readStream.pipe(writeStream);
            });

            poolPaths.push(variantPath);
        }
    } finally {
        if (isVideo) {
            releaseVideoSlot();
        }
    }

    return poolPaths;
}

function purgeFilesList(filePaths) {
    if (!Array.isArray(filePaths)) return;
    for (const fp of filePaths) {
        if (fp && fs.existsSync(fp)) {
            try { fs.unlinkSync(fp); } catch (e) {}
        }
    }
}

function purgeCampaignMediaItems(mediaItems) {
    if (!Array.isArray(mediaItems)) return;
    mediaItems.forEach((item) => {
        const targetPath = typeof item === 'string' ? item : (item?.path || item?.filePath);
        if (targetPath && fs.existsSync(targetPath)) {
            try { fs.unlinkSync(targetPath); } catch (e) {}
        }
    });
}

function cleanupCampaignArtifacts(tenantId, queueObj) {
    if (queueObj) {
        purgeCampaignMediaItems(queueObj.mediaItems);
        purgeFilesList(queueObj.mediaPool);
        if (queueObj.sourceMediaPath && fs.existsSync(queueObj.sourceMediaPath)) {
            try { fs.unlinkSync(queueObj.sourceMediaPath); } catch (e) {}
        }
        queueObj.mediaItems = [];
        queueObj.mediaPool = [];
        queueObj.sourceMediaPath = null;
        queueObj.recipients = [];
    }
    clearActiveCampaignState(tenantId);
    cleanOrphanedMediaVaultFiles(tenantId);
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
                if (rules.enabled) {
                    protectPath(rules.filePath);
                    (rules.sequence || []).forEach((s) => protectPath(s.filePath));
                }
            } catch (e) {}
        }

        const scheduledRows = stmtGetScheduledJobs.all(tenantId);
        scheduledRows.forEach((r) => {
            try {
                const j = JSON.parse(r.jobDataJson);
                protectPath(j.sourceMediaPath);
                (j.mediaPool || []).forEach((p) => protectPath(p));
                (j.sequence || []).forEach((s) => protectPath(s.filePath));
                (j.mediaItems || []).forEach((m) => protectPath(m.path || m.filePath));
            } catch (e) {}
        });

        const active = safeReadJsonSync(campaignStateFile, {});
        if (active.queue && (active.queue.isRunning || active.queue.state === 'PAUSED' || active.queue.state === 'RUNNING')) {
            protectPath(active.queue.sourceMediaPath);
            (active.queue.mediaPool || []).forEach((p) => protectPath(p));
            (active.queue.sequence || []).forEach((s) => protectPath(s.filePath));
            (active.queue.mediaItems || []).forEach((m) => protectPath(m.path || m.filePath));
        }

        const session = sessions.get(tenantId);
        if (session && session.queue && (session.queue.isRunning || session.queue.state === 'PAUSED' || session.queue.state === 'RUNNING')) {
            protectPath(session.queue.sourceMediaPath);
            (session.queue.mediaPool || []).forEach((p) => protectPath(p));
            (session.queue.sequence || []).forEach((s) => protectPath(s.filePath));
            (session.queue.mediaItems || []).forEach((m) => protectPath(m.path || m.filePath));
        }

        const vaultFiles = fs.readdirSync(mediaVaultDir);
        vaultFiles.forEach((file) => {
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

        files.forEach((file) => {
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
            return rows.map((r) => JSON.parse(r.dataJson)).filter((item) => !item.isWelcome);
        }
    } catch (e) {}

    const { campaignHistoryFile } = getTenantPaths(tenantId);
    return safeReadJsonSync(campaignHistoryFile, []).filter((item) => !item.isWelcome);
}

function upsertCampaignHistory(tenantId, queueObj) {
    if (!queueObj || !queueObj.campaignId || queueObj.isWelcome) return;
    const { logsFile, campaignHistoryFile } = getTenantPaths(tenantId);
    const isFinished = queueObj.state === 'STOPPED' && !queueObj.isRunning;

    const filteredRecipients = (queueObj.recipients || [])
        .filter((r) => r.status === 'FAILED' || r.status === 'PENDING')
        .map((r) => ({
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
        recipients: isFinished ? filteredRecipients : (queueObj.recipients || []).map((r) => ({
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
    stmtPruneCampaignHistory.run(tenantId, tenantId);

    let diskHist = safeReadJsonSync(campaignHistoryFile, []).filter((h) => !h.isWelcome);
    const existingIdx = diskHist.findIndex((h) => h.id === snapshot.id);
    if (existingIdx >= 0) diskHist[existingIdx] = snapshot;
    else diskHist.unshift(snapshot);
    if (diskHist.length > 3) diskHist = diskHist.slice(0, 3);
    safeWriteJsonSync(campaignHistoryFile, diskHist);

    if (isFinished && fs.existsSync(logsFile)) {
        try { fs.unlinkSync(logsFile); } catch (e) {}
    }
}

function saveActiveCampaignState(tenantId, state) {
    if (state?.queue?.isWelcome) return;
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
// 1+1+1 PAYLOAD NORMALIZATION & VALIDATION
// ============================================================================
function normalizeSingle111Payload(body, files) {
    const uploadedFiles = Array.isArray(files) ? files : [];
    const primaryFile = uploadedFiles[0] || null;

    let blocksMeta = [];
    if (body?.blocksMeta) {
        try {
            blocksMeta = JSON.parse(body.blocksMeta);
        } catch (e) {
            blocksMeta = [];
        }
    }

    const metaFirst = Array.isArray(blocksMeta) && blocksMeta.length > 0 ? blocksMeta[0] : null;
    const explicitMode = String(body?.mode || body?.itemType || metaFirst?.type || '').toLowerCase();

    const textContent = String(
        body?.text !== undefined ? body.text :
        body?.message !== undefined ? body.message :
        (metaFirst?.type === 'text' ? metaFirst?.content : '') || ''
    ).trim();

    const captionContent = String(
        body?.caption !== undefined ? body.caption :
        (metaFirst?.caption || '')
    ).trim();

    const existingFilePath = metaFirst?.existingFilePath ? String(metaFirst.existingFilePath).trim() : '';
    const existingMime = metaFirst?.mimetype ? String(metaFirst.mimetype).trim() : '';
    const existingFileName = metaFirst?.fileName ? String(metaFirst.fileName).trim() : '';

    if (primaryFile) {
        const isVid = primaryFile.mimetype === 'video/mp4' || path.extname(primaryFile.originalname).toLowerCase() === '.mp4';
        if (explicitMode === 'video' || isVid) {
            return {
                valid: true,
                kind: 'video',
                text: '',
                caption: captionContent,
                filePath: primaryFile.path,
                fileName: primaryFile.originalname,
                mimetype: primaryFile.mimetype || 'video/mp4',
                allUploadedFiles: uploadedFiles
            };
        }
        return {
            valid: true,
            kind: 'image',
            text: '',
            caption: captionContent,
            filePath: primaryFile.path,
            fileName: primaryFile.originalname,
            mimetype: primaryFile.mimetype || 'image/jpeg',
            allUploadedFiles: uploadedFiles
        };
    }

    if (existingFilePath && (explicitMode === 'image' || explicitMode === 'video')) {
        return {
            valid: true,
            kind: explicitMode,
            text: '',
            caption: captionContent,
            filePath: existingFilePath,
            fileName: existingFileName || path.basename(existingFilePath),
            mimetype: existingMime || (explicitMode === 'video' ? 'video/mp4' : 'image/jpeg'),
            allUploadedFiles: uploadedFiles
        };
    }

    if (textContent.length > 0) {
        return {
            valid: true,
            kind: 'text',
            text: textContent,
            caption: '',
            filePath: null,
            fileName: '',
            mimetype: '',
            allUploadedFiles: uploadedFiles
        };
    }

    return {
        valid: false,
        error: '1+1+1 rule requires either Text Only, 1 Image + Caption, or 1 Video + Caption.',
        allUploadedFiles: uploadedFiles
    };
}

function normalizeWelcome11Payload(body, files, existingConfig = null) {
    const uploadedFiles = Array.isArray(files) ? files : [];
    const primaryFile = uploadedFiles[0] || null;

    let blocksMeta = [];
    if (body?.blocksMeta) {
        try {
            blocksMeta = JSON.parse(body.blocksMeta);
        } catch (e) {
            blocksMeta = [];
        }
    }

    const metaFirst = Array.isArray(blocksMeta) && blocksMeta.length > 0 ? blocksMeta[0] : null;
    const requestedType = String(body?.type || metaFirst?.type || '').toLowerCase();

    if (requestedType === 'video' || (primaryFile && primaryFile.mimetype && primaryFile.mimetype.startsWith('video/'))) {
        return {
            valid: false,
            error: 'Video is not allowed in Welcome Bot. Use Text Only or 1 compressed Image + Caption.',
            allUploadedFiles: uploadedFiles
        };
    }

    const textContent = String(
        body?.text !== undefined ? body.text :
        body?.content !== undefined ? body.content :
        (metaFirst?.type === 'text' ? metaFirst?.content : '') || ''
    ).trim();

    const captionContent = String(
        body?.caption !== undefined ? body.caption :
        (metaFirst?.caption || '')
    ).trim();

    if (primaryFile) {
        return {
            valid: true,
            type: 'image',
            content: '',
            caption: captionContent,
            filePath: primaryFile.path,
            fileName: primaryFile.originalname,
            mimetype: primaryFile.mimetype || 'image/jpeg',
            allUploadedFiles: uploadedFiles
        };
    }

    const candidateExistingPath = metaFirst?.existingFilePath ||
        (existingConfig?.type === 'image' ? existingConfig?.filePath : '');

    if (candidateExistingPath && (requestedType === 'image' || !textContent)) {
        return {
            valid: true,
            type: 'image',
            content: '',
            caption: captionContent || existingConfig?.caption || '',
            filePath: candidateExistingPath,
            fileName: metaFirst?.fileName || existingConfig?.fileName || path.basename(candidateExistingPath),
            mimetype: metaFirst?.mimetype || existingConfig?.mimetype || 'image/jpeg',
            allUploadedFiles: uploadedFiles
        };
    }

    if (textContent.length > 0) {
        return {
            valid: true,
            type: 'text',
            content: textContent,
            caption: '',
            filePath: null,
            fileName: '',
            mimetype: '',
            allUploadedFiles: uploadedFiles
        };
    }

    return {
        valid: false,
        error: 'Welcome Bot requires either Text Only or 1 compressed Image + Caption.',
        allUploadedFiles: uploadedFiles
    };
}

// ============================================================================
// SCHEDULE EXPIRY & PURGE (MAX 1 SLOT, MAX 3 DAYS / 72 HOURS)
// ============================================================================
function purgeExpiredOrOverWindowSchedules(tenantId = null) {
    const now = Date.now();
    const rows = tenantId
        ? stmtGetScheduledJobs.all(tenantId)
        : stmtGetAllScheduledJobs.all('SCHEDULED');

    for (const row of rows) {
        let parsedJob = null;
        try {
            parsedJob = JSON.parse(row.jobDataJson);
        } catch (e) {}

        const createdEpoch = parsedJob?.createdAt || parsedJob?.scheduledForEpoch || row.scheduledForEpoch || now;
        const isPast3DaysWindow = (now - createdEpoch) > MAX_SCHEDULE_WINDOW_MS;
        const isPastScheduleWindow = row.scheduledForEpoch && (now - row.scheduledForEpoch) > MAX_SCHEDULE_WINDOW_MS;

        if (isPast3DaysWindow || isPastScheduleWindow) {
            if (parsedJob) {
                purgeCampaignMediaItems(parsedJob.mediaItems);
                purgeFilesList(parsedJob.mediaPool);
                if (parsedJob.sourceMediaPath && fs.existsSync(parsedJob.sourceMediaPath)) {
                    try { fs.unlinkSync(parsedJob.sourceMediaPath); } catch (e) {}
                }
            }
            stmtDeleteScheduledJob.run(row.jobId);
            cleanOrphanedMediaVaultFiles(row.sessionId);
        }
    }
}

// ============================================================================
// LOW-RAM BAILEYS SOCKET & AUTONOMOUS WELCOME BOT
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
            sessionId,
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
                mode: 'text',
                message: '',
                caption: '',
                sourceMediaPath: null,
                mediaPool: [],
                mediaItems: []
            }
        });
    }
    const sess = sessions.get(sessionId);
    sess.lastActive = Date.now();
    return sess;
}

function hasActiveWelcomeBot(sessionId) {
    try {
        const row = stmtGetWelcome.get(sessionId);
        if (!row || !row.configJson) return false;
        const cfg = JSON.parse(row.configJson);
        return !!cfg.enabled;
    } catch (e) {
        return false;
    }
}

function hasPendingOrActiveWork(sessionId) {
    const session = sessions.get(sessionId);
    if (session && (session.queue?.isRunning || session.queue?.state === 'PAUSED' || session.isDispatchLoopActive || session.isExecutingPriorityTask)) {
        return true;
    }
    try {
        const scheds = stmtGetScheduledJobs.all(sessionId);
        if (scheds.some((s) => s.status === 'SCHEDULED' || s.status === 'DISPATCHING')) {
            return true;
        }
    } catch (e) {}
    return false;
}

function putSocketToSleep(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.sock) return;

    // Never sleep sockets that have active welcome bot or active/pending campaign/schedule work
    if (hasActiveWelcomeBot(sessionId) || hasPendingOrActiveWork(sessionId)) {
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
            files.forEach((f) => {
                try { fs.unlinkSync(path.join(sessionDir, f)); } catch (err) {}
            });
        } catch (e) {}
    }
}

async function sendWelcomeBotMessage(sock, sessionId, remoteJid, rules) {
    const cooldownHours = Math.max(1, Number(rules.cooldownHours) || 24);
    const cooldownMs = cooldownHours * 60 * 60 * 1000;
    const now = Date.now();

    // Atomic SQLite deduplication check + pre-insert to prevent concurrent duplicate replies
    const existingDedup = stmtGetWelcomeDedup.get(sessionId, remoteJid);
    if (existingDedup && (now - existingDedup.sentAt) < cooldownMs) {
        return;
    }

    if (existingDedup && (now - existingDedup.sentAt) >= cooldownMs) {
        stmtDeleteWelcomeDedup.run(sessionId, remoteJid);
    }

    const insertRes = stmtInsertWelcomeDedup.run(sessionId, remoteJid, now);
    if (!insertRes || insertRes.changes === 0) {
        // Another concurrent event already claimed this recipient
        const recheck = stmtGetWelcomeDedup.get(sessionId, remoteJid);
        if (!recheck || (now - recheck.sentAt) < cooldownMs) {
            return;
        }
    }

    try {
        await delay(1200);
        try { await sock.sendPresenceUpdate('composing', remoteJid); } catch (e) {}
        await delay(800);

        if (rules.type === 'image' && rules.filePath && fs.existsSync(rules.filePath)) {
            const caption = rules.caption ? injectZeroWidthRandomizers(rules.caption) : '';
            await sock.sendMessage(remoteJid, {
                image: { url: rules.filePath },
                caption
            });
        } else if (rules.content) {
            await sock.sendMessage(remoteJid, {
                text: injectZeroWidthRandomizers(rules.content)
            });
        } else {
            throw new Error('EMPTY_WELCOME_CONFIG');
        }

        try { await sock.sendPresenceUpdate('paused', remoteJid); } catch (e) {}
    } catch (err) {
        // Roll back deduplication entry if sending genuinely failed
        stmtDeleteWelcomeDedup.run(sessionId, remoteJid);
        throw err;
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
        syncFullHistory: false,
        markOnlineOnConnect: false,
        getMessage: undefined,
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

            for (const msg of msgs) {
                // Immediate noise & self-message filters
                if (!msg || !msg.message || msg.key?.fromMe) continue;

                const remoteJid = msg.key?.remoteJid || '';
                if (!remoteJid || remoteJid === 'status@broadcast' || remoteJid.endsWith('@broadcast') || remoteJid.endsWith('@g.us')) {
                    continue;
                }
                if (!remoteJid.endsWith('@s.whatsapp.net') && !remoteJid.endsWith('@lid')) {
                    continue;
                }

                const msgTimestamp = Number(msg.messageTimestamp || nowSec);
                if (nowSec - msgTimestamp > 120) continue;

                if (msg.message.reactionMessage || msg.message.protocolMessage || msg.message.stickerMessage) {
                    continue;
                }

                const welcomeRow = stmtGetWelcome.get(sessionId);
                if (!welcomeRow || !welcomeRow.configJson) continue;

                let rules = null;
                try {
                    rules = JSON.parse(welcomeRow.configJson);
                } catch (e) {
                    continue;
                }
                if (!rules || !rules.enabled) continue;

                session.isExecutingPriorityTask = true;
                touchSessionActivity(sessionId);

                try {
                    await sendWelcomeBotMessage(sock, sessionId, remoteJid, rules);
                } finally {
                    session.isExecutingPriorityTask = false;
                    touchSessionActivity(sessionId);
                }
            }
        } catch (err) {
            session.isExecutingPriorityTask = false;
            console.error('[WELCOME ERROR]:', err.message || err);
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
                // Autonomous reconnect even if customer phone is offline
                setTimeout(() => {
                    if (sessions.has(sessionId)) {
                        initSessionSocket(sessionId).catch(() => {});
                    }
                }, 3000);
            }
        } else if (connection === 'open') {
            session.status = 'CONNECTED';
            const rawId = sock.user?.id || '';
            session.phone = rawId.split(':')[0] || rawId.split('@')[0];

            const lic = stmtGetLicense.get(sessionId);
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

            getTenantDailyQuotaState(sessionId);
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
            while (session.status !== 'CONNECTED' && attempts < 20) {
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
                const quotaState = getTenantDailyQuotaState(sessionId);
                if (quotaState.remainingQuota <= 0) {
                    session.queue.isRunning = false;
                    session.queue.state = 'PAUSED';
                    saveActiveCampaignState(sessionId, { sessionId, queue: session.queue });
                    return;
                }
                session.queue.isRunning = true;
                executeDispatchQueue(sessionId);
            } else {
                session.queue.isRunning = false;
            }
        }
    } catch (e) {}
}

// ============================================================================
// CAMPAIGN & SCHEDULE DISPATCH ENGINE (DAILY 300 CAP + BOUNDED POOL + PACING)
// ============================================================================
async function executeDispatchQueue(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.queue || session.queue.isWelcome) return;
    if (session.isDispatchLoopActive) return;

    session.isDispatchLoopActive = true;
    const q = session.queue;

    try {
        // Ensure bounded 5-10 variation pool exists if this is a media campaign
        if ((q.mode === 'image' || q.mode === 'video') && q.sourceMediaPath && (!Array.isArray(q.mediaPool) || q.mediaPool.length === 0)) {
            q.mediaPool = await buildBoundedMediaPool(sessionId, q.sourceMediaPath, q.mimetype);
            saveActiveCampaignState(sessionId, { sessionId, queue: q });
        }

        let poolCursor = 0;

        while (q.currentIndex < q.recipients.length) {
            touchSessionActivity(sessionId);

            if (q.state === 'STOPPED') {
                q.isRunning = false;
                const remainingCount = Math.max(0, (q.total || q.recipients.length) - (q.sent || 0) - (q.failed || 0));
                q.remaining = remainingCount;
                setWeeklyPendingMetric(sessionId, 0);
                upsertCampaignHistory(sessionId, q);
                cleanupCampaignArtifacts(sessionId, q);
                session.isDispatchLoopActive = false;
                return;
            }

            if (q.state === 'PAUSED') {
                q.isRunning = false;
                setWeeklyPendingMetric(sessionId, q.remaining || 0);
                saveActiveCampaignState(sessionId, { sessionId, queue: q });
                session.isDispatchLoopActive = false;
                return;
            }

            // Enforce Daily 300 Outbound Cap across campaigns and schedules
            const quotaState = getTenantDailyQuotaState(sessionId);
            if (quotaState.remainingQuota <= 0) {
                q.isRunning = false;
                q.state = 'PAUSED';
                q.remaining = Math.max(0, q.total - q.sent - q.failed);
                setWeeklyPendingMetric(sessionId, q.remaining);
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
                incrementWeeklyMetric(sessionId, 'failed', 1);
                setWeeklyPendingMetric(sessionId, q.remaining);
                q.currentIndex++;
                saveActiveCampaignState(sessionId, { sessionId, queue: q });
                continue;
            }

            const targetJid = `${clean}@s.whatsapp.net`;

            try {
                if (!session.sock || session.status !== 'CONNECTED') {
                    await ensureActiveSocket(sessionId);
                }

                const [lookup] = await session.sock.onWhatsApp(targetJid);
                if (!lookup?.exists) {
                    throw new Error('NOT_ON_WHATSAPP');
                }

                if (q.state === 'PAUSED' || q.state === 'STOPPED') continue;

                const jidToSend = lookup.jid || targetJid;
                await session.sock.sendPresenceUpdate('composing', jidToSend);
                await delay(Math.floor(Math.random() * (900 - 500 + 1)) + 500);

                if (q.mode === 'image' && Array.isArray(q.mediaPool) && q.mediaPool.length > 0) {
                    const variantPath = q.mediaPool[poolCursor % q.mediaPool.length];
                    poolCursor++;
                    const caption = q.caption ? injectZeroWidthRandomizers(q.caption) : '';
                    await session.sock.sendMessage(jidToSend, {
                        image: { url: variantPath },
                        caption
                    });
                } else if (q.mode === 'video' && Array.isArray(q.mediaPool) && q.mediaPool.length > 0) {
                    const variantPath = q.mediaPool[poolCursor % q.mediaPool.length];
                    poolCursor++;
                    const caption = q.caption ? injectZeroWidthRandomizers(q.caption) : '';
                    await session.sock.sendMessage(jidToSend, {
                        video: { url: variantPath },
                        caption
                    });
                } else {
                    const resolvedMessage = q.message ? injectZeroWidthRandomizers(q.message) : '';
                    await session.sock.sendMessage(jidToSend, { text: resolvedMessage });
                }

                try { await session.sock.sendPresenceUpdate('paused', jidToSend); } catch (e) {}

                // Deduct strictly 1 delivered message in persistent SQLite daily quota
                recordDeliveredCampaignMessage(sessionId);
                q.sent++;
                q.remaining = Math.max(0, q.total - q.sent - q.failed);
                if (typeof q.recipients[q.currentIndex] === 'object') {
                    q.recipients[q.currentIndex].status = 'SENT';
                }
                incrementWeeklyMetric(sessionId, 'sent', 1);
                setWeeklyPendingMetric(sessionId, q.remaining);
            } catch (e) {
                q.failed++;
                q.remaining = Math.max(0, q.total - q.sent - q.failed);
                if (typeof q.recipients[q.currentIndex] === 'object') {
                    q.recipients[q.currentIndex].status = 'FAILED';
                }
                incrementWeeklyMetric(sessionId, 'failed', 1);
                setWeeklyPendingMetric(sessionId, q.remaining);
            }

            q.currentIndex++;
            saveActiveCampaignState(sessionId, { sessionId, queue: q });

            // Tested pacing baseline (~8.7s average / ~172 messages per 25 minutes)
            if (q.currentIndex < q.recipients.length && q.state === 'RUNNING') {
                const pacingMs = Math.floor(Math.random() * (10200 - 7200 + 1)) + 7200;
                await delay(pacingMs);
            }
        }

        q.isRunning = false;
        q.state = 'STOPPED';
        q.remaining = Math.max(0, q.total - q.sent - q.failed);
        setWeeklyPendingMetric(sessionId, 0);
        upsertCampaignHistory(sessionId, q);
        cleanupCampaignArtifacts(sessionId, q);
    } finally {
        session.isDispatchLoopActive = false;
        touchSessionActivity(sessionId);
    }
}

// ============================================================================
// BACKGROUND CRONS & ENGINE STARTUP RECOVERY
// ============================================================================
setInterval(async () => {
    const now = Date.now();

    // 1. Auto-expire and purge any schedule past 3 days (72 hours)
    try {
        purgeExpiredOrOverWindowSchedules();
    } catch (e) {}

    // 2. Idle socket check (never sleeps welcome-bot or active/pending tenants)
    for (const [tenantId, session] of sessions.entries()) {
        if (session && session.sock && (now - session.lastActive > 60 * 60 * 1000)) {
            putSocketToSleep(tenantId);
        }
    }

    // 3. Auto-expire global notices
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

    // 4. Trigger due scheduled campaigns (1+1+1 format)
    try {
        const dueJobs = stmtGetAllDueJobs.all('SCHEDULED', now);
        for (const row of dueJobs) {
            const tenantId = row.sessionId;
            await withTenantLock(tenantId, async () => {
                const quotaState = getTenantDailyQuotaState(tenantId);
                let job = null;
                try {
                    job = JSON.parse(row.jobDataJson);
                } catch (e) {
                    stmtDeleteScheduledJob.run(row.jobId);
                    return;
                }

                if ((now - (job.createdAt || row.scheduledForEpoch)) > MAX_SCHEDULE_WINDOW_MS) {
                    purgeCampaignMediaItems(job.mediaItems);
                    purgeFilesList(job.mediaPool);
                    if (job.sourceMediaPath && fs.existsSync(job.sourceMediaPath)) {
                        try { fs.unlinkSync(job.sourceMediaPath); } catch (e) {}
                    }
                    stmtDeleteScheduledJob.run(row.jobId);
                    cleanOrphanedMediaVaultFiles(tenantId);
                    return;
                }

                if (quotaState.remainingQuota <= 0) {
                    return; // Wait for next quota window / midnight reset; do not lose schedule before 72h expiry
                }

                const session = await ensureActiveSocket(tenantId);
                if (session && session.status === 'CONNECTED' && (!session.queue || !session.queue.isRunning)) {
                    const allowedRecipients = (job.recipients || []).slice(0, quotaState.remainingQuota);
                    stmtDeleteScheduledJob.run(row.jobId);
                    const { scheduledJobsFile } = getTenantPaths(tenantId);
                    safeWriteJsonSync(scheduledJobsFile, []);

                    session.queue = {
                        isRunning: true,
                        state: 'RUNNING',
                        campaignId: job.id,
                        timestamp: new Date().toLocaleString(),
                        total: allowedRecipients.length,
                        sent: 0,
                        failed: 0,
                        remaining: allowedRecipients.length,
                        currentIndex: 0,
                        recipients: allowedRecipients.map((r) => ({
                            name: r.name || 'Unsaved',
                            phone: r.phone,
                            status: 'PENDING'
                        })),
                        mode: job.mode || 'text',
                        message: job.message || '',
                        caption: job.caption || '',
                        sourceMediaPath: job.sourceMediaPath || null,
                        mimetype: job.mimetype || '',
                        mediaPool: job.mediaPool || [],
                        mediaItems: job.mediaItems || []
                    };

                    setWeeklyPendingMetric(tenantId, session.queue.remaining);
                    saveActiveCampaignState(tenantId, { sessionId: tenantId, queue: session.queue });
                    executeDispatchQueue(tenantId);
                }
            });
        }
    } catch (e) {}
}, 20000);

// Non-blocking 24-hour maintenance cron: prune old welcome dedup rows, weekly metrics, and stale session keys
setInterval(() => {
    try {
        const thirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000);
        stmtPruneOldWelcomeDedup.run(thirtyDaysAgo);
        stmtPruneOldWeeklyMetrics.run(getLocalWeekKey());

        const licenses = stmtGetAllLicensesSorted.all();
        for (const lic of licenses) {
            pruneStaleSessionKeys(lic.sessionId);
            cleanOrphanedMediaVaultFiles(lic.sessionId);
        }
        db.pragma('wal_checkpoint(PASSIVE)');
    } catch (e) {}
}, 24 * 60 * 60 * 1000);

// Boot recovery: wake up tenants with active welcome bots or pending/in-flight campaigns
if (fs.existsSync(BASE_TENANTS_DIR)) {
    purgeExpiredOrOverWindowSchedules();
    fs.readdirSync(BASE_TENANTS_DIR).forEach((d) => {
        if (d.startsWith('tenant_')) {
            const tid = d.replace('tenant_', '');
            const { sessionDir, campaignStateFile } = getTenantPaths(tid);
            if (fs.existsSync(path.join(sessionDir, 'creds.json'))) {
                const saved = safeReadJsonSync(campaignStateFile, null);
                const hasPendingCamp = !!(saved && saved.queue?.remaining > 0 && saved.queue?.state !== 'STOPPED');
                const welcomeActive = hasActiveWelcomeBot(tid);
                if (hasPendingCamp || welcomeActive) {
                    initSessionSocket(tid).catch(() => {});
                }
            }
        }
    });
}

// ============================================================================
// API ROUTES
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

    const quotaState = getTenantDailyQuotaState(sessionId);
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
            validUntil,
            dailyCap: DAILY_OUTBOUND_CAP,
            deliveredTodayCount: quotaState.deliveredTodayCount,
            remainingQuota: quotaState.remainingQuota
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
        validUntil,
        dailyCap: DAILY_OUTBOUND_CAP,
        deliveredTodayCount: quotaState.deliveredTodayCount,
        remainingQuota: quotaState.remainingQuota
    });
});

app.get('/api/quota/:sessionId', authenticate, (req, res) => {
    const { sessionId } = req.params;
    const quotaState = getTenantDailyQuotaState(sessionId);
    res.json({
        success: true,
        dailyCap: DAILY_OUTBOUND_CAP,
        dayKey: quotaState.dayKey,
        deliveredTodayCount: quotaState.deliveredTodayCount,
        remainingQuota: quotaState.remainingQuota,
        welcomeExempt: true
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
    if (Array.isArray(groups) && groups.length === 0 && !allowEmptyOverwrite) {
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

    stmtSaveGroups.run(sessionId, JSON.stringify(groups || []), Date.now());
    safeWriteJsonSync(groupsBackupFile, groups || []);

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

// Welcome Bot Setup (1+1: Text Only OR 1 Compressed Image + Caption; no video; exempt from 300/day quota)
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
    const cooldownHours = Math.max(1, parseInt(req.body.cooldownHours || 24, 10) || 24);

    let prevConfig = null;
    const prevRow = stmtGetWelcome.get(sessionId);
    if (prevRow && prevRow.configJson) {
        try { prevConfig = JSON.parse(prevRow.configJson); } catch (e) {}
    }

    if (!enabled) {
        // Immediately purge welcome media when disabled
        if (prevConfig?.filePath && path.resolve(prevConfig.filePath).startsWith(resolvedVault) && fs.existsSync(prevConfig.filePath)) {
            try { fs.unlinkSync(prevConfig.filePath); } catch (e) {}
        }
        (prevConfig?.sequence || []).forEach((s) => {
            if (s.filePath && path.resolve(s.filePath).startsWith(resolvedVault) && fs.existsSync(s.filePath)) {
                try { fs.unlinkSync(s.filePath); } catch (e) {}
            }
        });
        purgeCampaignMediaItems(req.files);

        const disabledConfig = { enabled: false, cooldownHours, type: 'text', content: '', caption: '', filePath: null, fileName: '', mimetype: '' };
        stmtSaveWelcome.run(sessionId, JSON.stringify(disabledConfig), Date.now());
        safeWriteJsonSync(welcomeRulesFile, disabledConfig);
        cleanOrphanedMediaVaultFiles(sessionId);
        touchSessionActivity(sessionId);
        return res.json({ success: true, config: disabledConfig });
    }

    const parsed11 = normalizeWelcome11Payload(req.body, req.files, prevConfig);
    if (!parsed11.valid) {
        purgeCampaignMediaItems(req.files);
        return res.status(400).json({ error: parsed11.error });
    }

    // Purge any extra files if more than 1 file was uploaded
    if (req.files && req.files.length > 1) {
        purgeCampaignMediaItems(req.files.slice(1));
    }

    let finalFilePath = null;
    if (parsed11.type === 'image' && parsed11.filePath) {
        const resolvedCandidate = path.resolve(parsed11.filePath);
        if (!resolvedCandidate.startsWith(resolvedVault) || !fs.existsSync(resolvedCandidate)) {
            purgeCampaignMediaItems(req.files);
            return res.status(400).json({ error: 'Invalid welcome image path.' });
        }
        finalFilePath = resolvedCandidate;

        // Purge previous welcome image if replaced
        const oldPaths = new Set();
        if (prevConfig?.filePath) oldPaths.add(path.resolve(prevConfig.filePath));
        (prevConfig?.sequence || []).forEach((s) => { if (s.filePath) oldPaths.add(path.resolve(s.filePath)); });
        for (const oldP of oldPaths) {
            if (oldP !== finalFilePath && oldP.startsWith(resolvedVault) && fs.existsSync(oldP)) {
                try { fs.unlinkSync(oldP); } catch (e) {}
            }
        }
    } else {
        // Switching to text welcome: purge old welcome media
        const oldPaths = new Set();
        if (prevConfig?.filePath) oldPaths.add(path.resolve(prevConfig.filePath));
        (prevConfig?.sequence || []).forEach((s) => { if (s.filePath) oldPaths.add(path.resolve(s.filePath)); });
        for (const oldP of oldPaths) {
            if (oldP.startsWith(resolvedVault) && fs.existsSync(oldP)) {
                try { fs.unlinkSync(oldP); } catch (e) {}
            }
        }
    }

    const config = {
        enabled: true,
        cooldownHours,
        type: parsed11.type,
        content: parsed11.content,
        caption: parsed11.caption,
        filePath: finalFilePath,
        fileName: parsed11.type === 'image' ? parsed11.fileName : '',
        mimetype: parsed11.type === 'image' ? parsed11.mimetype : ''
    };

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
    const config = safeReadJsonSync(welcomeRulesFile, { enabled: false, cooldownHours: 24, type: 'text', content: '' });
    res.json({ success: true, config });
});

// Bulk Campaign or 1-Slot Schedule (Strict 1+1+1 + Max 1 Schedule Slot + Max 3 Days / 72h + 300/day Quota)
app.post('/api/message/send-bulk', upload.any(), authenticate, async (req, res) => {
    const sessionId = req.body.sessionId || 'unpaired_user';
    const recipientsRaw = req.body.recipients;
    const scheduleEpoch = parseInt(req.body.scheduleEpoch || 0, 10);
    const isScheduled = req.body.isScheduled === 'true' || req.body.isScheduled === true;

    purgeExpiredOrOverWindowSchedules(sessionId);

    const payload111 = normalizeSingle111Payload(req.body, req.files);
    if (!payload111.valid) {
        purgeCampaignMediaItems(req.files);
        return res.status(400).json({ error: payload111.error });
    }

    // Purge any extra files beyond slot 1
    if (req.files && req.files.length > 1) {
        purgeCampaignMediaItems(req.files.slice(1));
    }

    let parsed = [];
    try { parsed = JSON.parse(recipientsRaw || '[]'); } catch (e) { parsed = []; }
    if (!Array.isArray(parsed) || parsed.length === 0) {
        purgeCampaignMediaItems(req.files);
        return res.status(400).json({ error: 'Recipient list cannot be empty.' });
    }

    await withTenantLock(sessionId, async () => {
        await ensureActiveSocket(sessionId);
        const session = sessions.get(sessionId);
        if (!session || session.status !== 'CONNECTED') {
            purgeCampaignMediaItems(req.files);
            return res.status(400).json({ error: 'WhatsApp not connected.' });
        }

        const now = Date.now();
        const quotaState = getTenantDailyQuotaState(sessionId);

        if (isScheduled) {
            if (quotaState.remainingQuota <= 0) {
                purgeCampaignMediaItems(req.files);
                return res.status(400).json({ error: 'Daily 300 message quota exhausted for today. Resets at 12:00 AM.' });
            }

            const existingScheds = stmtGetScheduledJobs.all(sessionId).filter((r) => r.status === 'SCHEDULED' || r.status === 'DISPATCHING');
            if (existingScheds.length >= MAX_SCHEDULE_SLOTS) {
                purgeCampaignMediaItems(req.files);
                return res.status(400).json({ error: 'Only 1 scheduled campaign slot is allowed at a time. Cancel or wait for the active schedule first.' });
            }

            if (!scheduleEpoch || scheduleEpoch <= now) {
                purgeCampaignMediaItems(req.files);
                return res.status(400).json({ error: 'Schedule time must be in the future.' });
            }
            if ((scheduleEpoch - now) > MAX_SCHEDULE_WINDOW_MS) {
                purgeCampaignMediaItems(req.files);
                return res.status(400).json({ error: 'Maximum schedule window is 3 days (72 hours).' });
            }

            const cappedRecipients = parsed.slice(0, quotaState.remainingQuota).map((r) => ({
                name: r.name || 'Unsaved',
                phone: sanitizeToValidWhatsAppJid(typeof r === 'object' ? r.phone : r),
                status: 'PENDING'
            }));

            const job = {
                id: 'sch_' + now,
                createdAt: now,
                scheduledForEpoch: scheduleEpoch,
                scheduledForDate: new Date(scheduleEpoch).toLocaleString(),
                status: 'SCHEDULED',
                mode: payload111.kind,
                message: payload111.text,
                caption: payload111.caption,
                sourceMediaPath: payload111.filePath || null,
                mimetype: payload111.mimetype || '',
                mediaPool: [],
                mediaItems: payload111.filePath ? [{
                    path: payload111.filePath,
                    originalname: payload111.fileName,
                    mimetype: payload111.mimetype,
                    caption: payload111.caption
                }] : [],
                recipients: cappedRecipients
            };

            stmtUpsertScheduledJob.run({
                jobId: job.id,
                sessionId,
                status: 'SCHEDULED',
                scheduledForEpoch: scheduleEpoch,
                jobDataJson: JSON.stringify(job)
            });

            const { scheduledJobsFile } = getTenantPaths(sessionId);
            safeWriteJsonSync(scheduledJobsFile, [job]);

            touchSessionActivity(sessionId);
            return res.json({
                status: 'SCHEDULED',
                jobId: job.id,
                scheduledForDate: job.scheduledForDate,
                recipientsCount: cappedRecipients.length,
                remainingQuota: quotaState.remainingQuota
            });
        }

        // Immediate Campaign (Only 1 active campaign slot)
        if (session.queue && (session.queue.isRunning || session.queue.state === 'RUNNING' || session.queue.state === 'PAUSED')) {
            purgeCampaignMediaItems(req.files);
            return res.status(400).json({ error: 'A campaign is already active or paused. Stop or finish it before starting a new one.' });
        }

        if (quotaState.remainingQuota <= 0) {
            purgeCampaignMediaItems(req.files);
            return res.status(400).json({ error: 'Daily 300 message quota exhausted for today. Resets at 12:00 AM.' });
        }

        const allowedRecipients = parsed.slice(0, quotaState.remainingQuota).map((r) => ({
            name: r.name || 'Unsaved',
            phone: sanitizeToValidWhatsAppJid(typeof r === 'object' ? r.phone : r),
            status: 'PENDING'
        }));

        const campaignId = 'cmp_' + now;
        session.queue = {
            isRunning: true,
            state: 'RUNNING',
            campaignId,
            timestamp: new Date().toLocaleString(),
            total: allowedRecipients.length,
            sent: 0,
            failed: 0,
            remaining: allowedRecipients.length,
            currentIndex: 0,
            recipients: allowedRecipients,
            mode: payload111.kind,
            message: payload111.text,
            caption: payload111.caption,
            sourceMediaPath: payload111.filePath || null,
            mimetype: payload111.mimetype || '',
            mediaPool: [],
            mediaItems: payload111.filePath ? [{
                path: payload111.filePath,
                originalname: payload111.fileName,
                mimetype: payload111.mimetype,
                caption: payload111.caption
            }] : []
        };

        setWeeklyPendingMetric(sessionId, session.queue.remaining);
        saveActiveCampaignState(sessionId, { sessionId, queue: session.queue });
        touchSessionActivity(sessionId);

        res.json({
            status: 'QUEUED',
            campaignId,
            totalRecipients: allowedRecipients.length,
            dailyCap: DAILY_OUTBOUND_CAP,
            deliveredTodayCount: quotaState.deliveredTodayCount,
            remainingQuota: quotaState.remainingQuota,
            campaign: session.queue
        });

        executeDispatchQueue(sessionId);
    });
});

app.get('/api/campaign/scheduled-jobs/:sessionId', authenticate, (req, res) => {
    const { sessionId } = req.params;
    purgeExpiredOrOverWindowSchedules(sessionId);
    touchSessionActivity(sessionId);

    const rows = stmtGetScheduledJobs.all(sessionId);
    const scheduled = rows
        .filter((r) => r.status === 'SCHEDULED')
        .slice(0, MAX_SCHEDULE_SLOTS)
        .map((r) => {
            const j = JSON.parse(r.jobDataJson);
            return {
                id: j.id,
                scheduledForDate: j.scheduledForDate,
                scheduledForEpoch: j.scheduledForEpoch,
                expiresAtEpoch: (j.createdAt || j.scheduledForEpoch || Date.now()) + MAX_SCHEDULE_WINDOW_MS,
                recipientCount: (j.recipients || []).length,
                mode: j.mode || 'text',
                message: j.message || '',
                caption: j.caption || '',
                mediaCount: j.sourceMediaPath || (j.mediaItems || []).length > 0 ? 1 : 0
            };
        });

    res.json({ success: true, maxSlots: MAX_SCHEDULE_SLOTS, maxWindowHours: 72, jobs: scheduled });
});

app.post('/api/campaign/cancel-schedule', authenticate, async (req, res) => {
    const { sessionId, jobId } = req.body;
    await withTenantLock(sessionId, async () => {
        const rows = stmtGetScheduledJobs.all(sessionId);
        const targetRow = jobId ? rows.find((r) => r.jobId === jobId) : rows[0];

        if (!targetRow) {
            return res.status(404).json({ error: 'Scheduled job not found.' });
        }

        try {
            const targetJob = JSON.parse(targetRow.jobDataJson);
            purgeCampaignMediaItems(targetJob.mediaItems);
            purgeFilesList(targetJob.mediaPool);
            if (targetJob.sourceMediaPath && fs.existsSync(targetJob.sourceMediaPath)) {
                try { fs.unlinkSync(targetJob.sourceMediaPath); } catch (e) {}
            }
        } catch (e) {}

        stmtDeleteScheduledJob.run(targetRow.jobId);
        const { scheduledJobsFile } = getTenantPaths(sessionId);
        safeWriteJsonSync(scheduledJobsFile, []);
        cleanOrphanedMediaVaultFiles(sessionId);
        touchSessionActivity(sessionId);

        const quotaState = getTenantDailyQuotaState(sessionId);
        return res.json({
            success: true,
            message: 'Scheduled campaign cancelled and media purged. Delivered quota preserved.',
            deliveredTodayCount: quotaState.deliveredTodayCount,
            remainingQuota: quotaState.remainingQuota
        });
    });
});

app.get('/api/campaign/state', authenticate, (req, res) => {
    const sessionId = req.query.sessionId || 'unpaired_user';
    touchSessionActivity(sessionId);
    const session = sessions.get(sessionId);
    const history = loadCampaignHistory(sessionId);
    const quotaState = getTenantDailyQuotaState(sessionId);

    if (session && session.queue && session.queue.campaignId && !session.queue.isWelcome && session.queue.state !== 'STOPPED') {
        return res.json({ hasState: true, queue: session.queue, history, quota: quotaState });
    }

    const { campaignStateFile } = getTenantPaths(sessionId);
    const saved = safeReadJsonSync(campaignStateFile, null);
    if (saved && saved.queue && !saved.queue.isWelcome && saved.queue.state !== 'STOPPED') {
        return res.json({ hasState: true, queue: saved.queue, history, quota: quotaState });
    }

    res.json({ hasState: false, history, quota: quotaState });
});

app.post('/api/campaign/pause', authenticate, async (req, res) => {
    const sessionId = req.body.sessionId || 'unpaired_user';
    await withTenantLock(sessionId, async () => {
        const session = sessions.get(sessionId);
        if (!session || !session.queue || session.queue.isWelcome || session.queue.state === 'STOPPED') {
            return res.status(400).json({ error: 'No active campaign to pause.' });
        }

        session.queue.state = 'PAUSED';
        session.queue.isRunning = false;
        setWeeklyPendingMetric(sessionId, session.queue.remaining || 0);
        saveActiveCampaignState(sessionId, { sessionId, queue: session.queue });
        touchSessionActivity(sessionId);

        const quotaState = getTenantDailyQuotaState(sessionId);
        res.json({
            status: 'PAUSED',
            campaignId: session.queue.campaignId,
            queue: session.queue,
            deliveredTodayCount: quotaState.deliveredTodayCount,
            remainingQuota: quotaState.remainingQuota
        });
    });
});

app.post('/api/campaign/resume', authenticate, async (req, res) => {
    const sessionId = req.body.sessionId || 'unpaired_user';
    await withTenantLock(sessionId, async () => {
        const quotaState = getTenantDailyQuotaState(sessionId);
        if (quotaState.remainingQuota <= 0) {
            return res.status(400).json({ error: 'Daily 300 message quota reached. Resets at 12:00 AM.' });
        }

        await ensureActiveSocket(sessionId);
        const session = sessions.get(sessionId);
        if (!session || !session.queue || session.queue.isWelcome || session.queue.state !== 'PAUSED') {
            return res.status(400).json({ error: 'No paused campaign available to resume.' });
        }

        session.queue.state = 'RUNNING';
        session.queue.isRunning = true;
        saveActiveCampaignState(sessionId, { sessionId, queue: session.queue });
        touchSessionActivity(sessionId);
        res.json({ status: 'RUNNING', campaignId: session.queue.campaignId, queue: session.queue });

        executeDispatchQueue(sessionId);
    });
});

// Stop / Close Campaign: Deduct ONLY actually delivered messages; never refund delivered counts; purge media/state
app.post('/api/campaign/stop', authenticate, async (req, res) => {
    const sessionId = req.body.sessionId || 'unpaired_user';
    await withTenantLock(sessionId, async () => {
        const session = sessions.get(sessionId);
        if (!session || !session.queue || session.queue.isWelcome) {
            return res.status(400).json({ error: 'No active campaign found.' });
        }

        const q = session.queue;
        q.state = 'STOPPED';
        q.isRunning = false;
        q.remaining = Math.max(0, (q.total || 0) - (q.sent || 0) - (q.failed || 0));

        setWeeklyPendingMetric(sessionId, 0);
        upsertCampaignHistory(sessionId, q);
        cleanupCampaignArtifacts(sessionId, q);
        touchSessionActivity(sessionId);

        const quotaState = getTenantDailyQuotaState(sessionId);
        res.json({
            status: 'STOPPED',
            campaignId: q.campaignId,
            deliveredInCampaign: q.sent || 0,
            deliveredTodayCount: quotaState.deliveredTodayCount,
            remainingQuota: quotaState.remainingQuota,
            queue: {
                campaignId: q.campaignId,
                state: q.state,
                isRunning: false,
                total: q.total,
                sent: q.sent,
                failed: q.failed,
                remaining: q.remaining
            }
        });
    });
});

app.get('/api/queue-status/:sessionId', authenticate, (req, res) => {
    const { sessionId } = req.params;
    touchSessionActivity(sessionId);
    const session = sessions.get(sessionId);
    const history = loadCampaignHistory(sessionId);
    const quotaState = getTenantDailyQuotaState(sessionId);

    if (!session || !session.queue || !session.queue.campaignId || session.queue.isWelcome) {
        return res.json({
            isRunning: false,
            state: 'STOPPED',
            total: 0,
            sent: 0,
            failed: 0,
            remaining: 0,
            history,
            deliveredTodayCount: quotaState.deliveredTodayCount,
            remainingQuota: quotaState.remainingQuota
        });
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
        history,
        deliveredTodayCount: quotaState.deliveredTodayCount,
        remainingQuota: quotaState.remainingQuota
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

app.get('/master-admin/live-metrics', (req, res) => {
    const pin = req.query.pin;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });

    purgeExpiredOrOverWindowSchedules();

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
        const quotaState = getTenantDailyQuotaState(sid);
        const weekly = getTenantWeeklySummary(sid);

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
        if (session && session.queue && session.queue.isRunning && !session.queue.isWelcome) {
            activeCampaignsCount++;
            const sent = session.queue.sent || 0;
            const total = session.queue.total || 0;
            const remaining = Math.max(0, total - sent - (session.queue.failed || 0));
            dispatchHtml = `<span style="color:#00a884;font-weight:bold;font-size:0.75rem;">⚡ Sending: ${sent}/${total} (${remaining} pending)</span>`;
        } else if (session && session.queue && session.queue.state === 'PAUSED' && !session.queue.isWelcome) {
            const sent = session.queue.sent || 0;
            const total = session.queue.total || 0;
            dispatchHtml = `<span style="color:#f59e0b;font-weight:bold;font-size:0.75rem;">⏸️ Paused: ${sent}/${total}</span>`;
        }

        let schedHtml = '';
        try {
            const schedRows = stmtGetScheduledJobs.all(sid);
            const activeScheds = schedRows.filter((r) => r.status === 'SCHEDULED');
            if (activeScheds.length > 0) {
                totalQueuedSchedules += activeScheds.length;
                schedHtml = `<div style="font-size:0.72rem;color:#38bdf8;font-weight:600;margin-top:2px;">⏰ Sched: ${activeScheds.length}/1 queued</div>`;
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
            plan: lic.plan || 'Standard',
            dailyCap: DAILY_OUTBOUND_CAP,
            deliveredTodayCount: quotaState.deliveredTodayCount,
            remainingQuota: quotaState.remainingQuota,
            weekly
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
    purgeExpiredOrOverWindowSchedules();

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
        const quotaState = getTenantDailyQuotaState(sid);
        const weekly = getTenantWeeklySummary(sid);

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
        if (session && session.queue && session.queue.isRunning && !session.queue.isWelcome) {
            activeCampaignsCount++;
            const sent = session.queue.sent || 0;
            const total = session.queue.total || 0;
            const remaining = Math.max(0, total - sent - (session.queue.failed || 0));
            dispatchBadge = `<span style="color:#00a884;font-weight:bold;font-size:0.75rem;">⚡ Sending: ${sent}/${total} (${remaining} pending)</span>`;
        } else if (session && session.queue && session.queue.state === 'PAUSED' && !session.queue.isWelcome) {
            const sent = session.queue.sent || 0;
            const total = session.queue.total || 0;
            dispatchBadge = `<span style="color:#f59e0b;font-weight:bold;font-size:0.75rem;">⏸️ Paused: ${sent}/${total}</span>`;
        }

        let schedBadge = '';
        try {
            const schedRows = stmtGetScheduledJobs.all(sid);
            const activeScheds = schedRows.filter((r) => r.status === 'SCHEDULED');
            if (activeScheds.length > 0) {
                totalQueuedSchedules += activeScheds.length;
                schedBadge = `<div style="font-size:0.72rem;color:#38bdf8;font-weight:600;margin-top:2px;">⏰ Sched: ${activeScheds.length}/1 queued</div>`;
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
                    <div id="quota_badge_${sid}" style="font-size:0.72rem;color:#38bdf8;margin-top:2px;">Quota: ${quotaState.deliveredTodayCount}/${DAILY_OUTBOUND_CAP} (${quotaState.remainingQuota} left) • Wk S:${weekly.totalSent} F:${weekly.totalFailed} P:${weekly.totalPending}</div>
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
                [data-tooltip] { position: relative; }
                [data-tooltip]:hover::after {
                    content: attr(data-tooltip); position: absolute; bottom: 125%; left: 50%;
                    transform: translateX(-50%); background: #182229; color: #e9edef; font-size: 0.7rem;
                    padding: 4px 8px; border-radius: 6px; border: 1px solid #00a884; white-space: nowrap;
                    z-index: 1000; box-shadow: 0 4px 12px rgba(0,0,0,0.5); pointer-events: none;
                }
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
                    rows.forEach((r) => {
                        const phone = r.getAttribute('data-phone') || '';
                        r.style.display = phone.toLowerCase().includes(q) ? '' : 'none';
                    });
                }

                function toggleSelectAllTenants(master) {
                    const checkboxes = document.querySelectorAll('.tenant-chk');
                    checkboxes.forEach((c) => {
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
                    return Array.from(document.querySelectorAll('.tenant-chk:checked')).map((c) => c.value);
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
                    if (!confirm('CANCEL SUBSCRIPTION for this tenant?\\n\\n- Subscription will expire immediately.\\n- Upcoming scheduled campaign and media will be PURGED.\\n- Active campaign will be STOPPED.\\n- Welcome bot rules will be safely preserved but PAUSED.\\n\\nProceed?')) return;
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

                                const quotaBadge = document.getElementById('quota_badge_' + sid);
                                if (quotaBadge && t.weekly) {
                                    quotaBadge.innerText = 'Quota: ' + t.deliveredTodayCount + '/${DAILY_OUTBOUND_CAP} (' + t.remainingQuota + ' left) • Wk S:' + t.weekly.totalSent + ' F:' + t.weekly.totalFailed + ' P:' + t.weekly.totalPending;
                                }

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

        allLicenses.forEach((lic) => {
            const clean = sanitizeToValidWhatsAppJid(lic.phone || lic.sessionId);
            if (!clean) return;
            if (!phoneGroups.has(clean)) phoneGroups.set(clean, []);
            phoneGroups.get(clean).push(lic);
        });

        let mergedCount = 0;
        let legacyLicObj = safeReadJsonSync(LICENSES_FILE, null);

        for (const [phone, tenantList] of phoneGroups.entries()) {
            if (tenantList.length > 1) {
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

                let primaryGroups = [];
                const pRow = stmtGetGroups.get(primary.sessionId);
                if (pRow && pRow.groupsJson) {
                    try { primaryGroups = JSON.parse(pRow.groupsJson); } catch (e) {}
                }

                duplicates.forEach((dup) => {
                    const dRow = stmtGetGroups.get(dup.sessionId);
                    if (dRow && dRow.groupsJson) {
                        try {
                            const dGroups = JSON.parse(dRow.groupsJson);
                            dGroups.forEach((dg) => {
                                const exists = primaryGroups.find((pg) => pg.name === dg.name);
                                if (!exists) primaryGroups.push(dg);
                            });
                        } catch (e) {}
                    }

                    stmtDeleteLicense.run(dup.sessionId);
                    wipeSessionData(dup.sessionId);

                    const { tenantDir } = getTenantPaths(dup.sessionId);
                    if (fs.existsSync(tenantDir)) {
                        try { fs.rmSync(tenantDir, { recursive: true, force: true }); } catch (e) {}
                    }

                    if (legacyLicObj && legacyLicObj[dup.sessionId]) {
                        delete legacyLicObj[dup.sessionId];
                    }

                    mergedCount++;
                });

                stmtSaveGroups.run(primary.sessionId, JSON.stringify(primaryGroups), Date.now());
            }
        }

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

app.post('/master-admin/cancel-sub', (req, res) => {
    const { pin, sessionId } = req.body;
    if (pin !== ADMIN_MASTER_PIN) return res.status(401).json({ error: 'Forbidden' });

    const lic = stmtGetLicense.get(sessionId);
    const now = Date.now();
    if (lic) {
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

        const session = sessions.get(sessionId);
        if (session && session.queue) {
            session.queue.state = 'STOPPED';
            session.queue.isRunning = false;
            upsertCampaignHistory(sessionId, session.queue);
            cleanupCampaignArtifacts(sessionId, session.queue);
        }

        const scheduledRows = stmtGetScheduledJobs.all(sessionId);
        scheduledRows.forEach((row) => {
            try {
                const j = JSON.parse(row.jobDataJson);
                purgeCampaignMediaItems(j.mediaItems);
                purgeFilesList(j.mediaPool);
                if (j.sourceMediaPath && fs.existsSync(j.sourceMediaPath)) {
                    try { fs.unlinkSync(j.sourceMediaPath); } catch (e) {}
                }
            } catch (e) {}
        });
        stmtDeleteAllScheduledJobsForSession.run(sessionId);

        const { scheduledJobsFile } = getTenantPaths(sessionId);
        safeWriteJsonSync(scheduledJobsFile, []);
        cleanOrphanedMediaVaultFiles(sessionId);

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
            status,
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

// Graceful shutdown handling for PM2 / process managers
function gracefulShutdown(signal) {
    console.log(`[SHUTDOWN] Received ${signal}. Persisting active states and closing sockets...`);
    for (const [sid, sess] of sessions.entries()) {
        if (sess?.queue && (sess.queue.isRunning || sess.queue.state === 'RUNNING')) {
            sess.queue.state = 'PAUSED';
            sess.queue.isRunning = false;
            saveActiveCampaignState(sid, { sessionId: sid, queue: sess.queue });
        }
        try {
            if (sess?.sock) {
                sess.sock.end(undefined);
            }
        } catch (e) {}
    }
    try {
        db.pragma('wal_checkpoint(PASSIVE)');
        db.close();
    } catch (e) {}
    process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

app.listen(PORT, '0.0.0.0', () => console.log(`[SERVER] Ultra-Optimized Enterprise Engine Online on port ${PORT}`));
