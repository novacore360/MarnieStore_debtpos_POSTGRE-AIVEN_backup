require("dotenv").config();
const { initializeApp: initClientApp } = require("firebase/app");
const {
  getFirestore: getClientFirestore,
  collection: fsCollection,
  getDocs,
  Timestamp: ClientTimestamp,
  GeoPoint: ClientGeoPoint,
} = require("firebase/firestore");
const { Pool } = require("pg");
const cron = require("node-cron");
const http = require("http");

// ═════════════════════════════════════════════════════════════════════════════
// 1. FIREBASE INITIALIZATION (Client SDK only — no service account, no admin)
// ═════════════════════════════════════════════════════════════════════════════

// SOURCE — client SDK (web config). Reads only, governed by your Firestore
// security rules. No admin key anywhere in this service.
const primaryClientApp = initClientApp(
  {
    apiKey:            process.env.PRIMARY_API_KEY,
    authDomain:        process.env.PRIMARY_AUTH_DOMAIN,
    projectId:         process.env.PRIMARY_PROJECT_ID,
    storageBucket:     process.env.PRIMARY_STORAGE_BUCKET,
    messagingSenderId: process.env.PRIMARY_MESSAGING_SENDER_ID,
    appId:             process.env.PRIMARY_APP_ID,
  },
  "primary-client"
);
const primaryDB = getClientFirestore(primaryClientApp);

// ═════════════════════════════════════════════════════════════════════════════
// 2. POSTGRES (AIVEN) INITIALIZATION — backup target
// ═════════════════════════════════════════════════════════════════════════════

function buildSSLConfig() {
  if ((process.env.PGSSLMODE || "").toLowerCase() === "disable") return false;
  const caCert = process.env.PG_CA_CERT
    ? process.env.PG_CA_CERT.replace(/\\n/g, "\n")
    : undefined;
  if (caCert) return { ca: caCert, rejectUnauthorized: true };
  // No CA supplied — still encrypted in transit, just not certificate-verified.
  // Get the CA cert from your Aiven service overview page and set PG_CA_CERT
  // for full verification.
  return { rejectUnauthorized: false };
}

// `pg` derives its OWN ssl config from a `?sslmode=...` query param in the
// connection string, and that SILENTLY OVERRIDES the explicit `ssl` object
// passed to Pool below — falling back to Node's default system CA trust
// store (which doesn't include Aiven's CA), causing a "self-signed
// certificate in certificate chain" error even when PG_CA_CERT is set
// correctly. Aiven's default "Service URI" includes `?sslmode=require`, so
// we strip it here and let buildSSLConfig() above be the single source of
// truth for TLS behavior.
function stripSslModeParam(connectionString) {
  if (!connectionString) return connectionString;
  try {
    const url = new URL(connectionString);
    url.searchParams.delete("sslmode");
    return url.toString();
  } catch (err) {
    log("warn", `Could not parse DATABASE_URL to strip sslmode param: ${err.message}`);
    return connectionString;
  }
}

const pgPool = new Pool({
  connectionString: stripSslModeParam(process.env.DATABASE_URL),
  ssl: buildSSLConfig(),
  max: parseInt(process.env.PG_POOL_MAX || "5", 10),
});

pgPool.on("error", (err) => {
  log("error", `Postgres pool error: ${err.message}`);
});

// ═════════════════════════════════════════════════════════════════════════════
// 3. CONFIGURATION
// ═════════════════════════════════════════════════════════════════════════════

const COLLECTIONS = (process.env.COLLECTIONS || "customers,purchases,products")
  .split(",").map((c) => c.trim()).filter(Boolean);

const PORT = process.env.PORT || 3000;

// Schedule: ONE slot per day in PHT.
const BACKUP_HOUR = parseInt(process.env.BACKUP_HOUR ?? "18", 10); // 6:00 PM PHT

// Retry policy:
//   • Within the first hour: up to MAX_CONSECUTIVE_FAILS fast attempts
//     (CONSECUTIVE_RETRY_DELAY ms apart, enforced by the minute-cron tick).
//   • After the first hour is exhausted: retry once per hour (anchored to the
//     first attempt time, e.g. 6:04 PM → 7:04 PM → 8:04 PM …).
//   • Stop when the slot age exceeds GRACE_HOURS. For a 6 PM slot and
//     GRACE_HOURS=5, the last allowed hourly window starts before 11 PM.
const MAX_CONSECUTIVE_FAILS   = parseInt(process.env.MAX_CONSECUTIVE_FAILS   ?? "5",     10);
const CONSECUTIVE_RETRY_DELAY = parseInt(process.env.CONSECUTIVE_RETRY_DELAY ?? "30000", 10); // ms
const GRACE_HOURS             = parseInt(process.env.GRACE_HOURS             ?? "5",     10); // hours after slot

// ═════════════════════════════════════════════════════════════════════════════
// 4. LOGGER
// ═════════════════════════════════════════════════════════════════════════════

function phNow() {
  return new Date().toLocaleString("en-PH", { timeZone: "Asia/Manila" });
}

function log(level, message, data = null) {
  const prefix = `[${phNow()}] [${level.toUpperCase()}]`;
  if (data) console.log(`${prefix} ${message}`, JSON.stringify(data, null, 2));
  else      console.log(`${prefix} ${message}`);
}

// ═════════════════════════════════════════════════════════════════════════════
// 5. IN-MEMORY STATE
// ═════════════════════════════════════════════════════════════════════════════

const backupState = {
  isRunning:   false,
  lastRun:     null,
  lastResults: null,
};

// ═════════════════════════════════════════════════════════════════════════════
// 6. TIMEZONE HELPERS
// ═════════════════════════════════════════════════════════════════════════════

const PH_OFFSET_MINUTES = 8 * 60;

function getPhWallClockParts(date) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Manila",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  }).formatToParts(date);
  const m = {};
  for (const p of parts) m[p.type] = p.value;
  return {
    year:   parseInt(m.year,   10),
    month:  parseInt(m.month,  10),
    day:    parseInt(m.day,    10),
    hour:   parseInt(m.hour,   10) % 24,
    minute: parseInt(m.minute, 10),
  };
}

function phWallClockToUTC(year, month, day, hour, minute) {
  return new Date(
    Date.UTC(year, month - 1, day, hour, minute, 0) - PH_OFFSET_MINUTES * 60000
  );
}

// Returns the most recent scheduled slot (as a true UTC Date) that is <= now.
function getMostRecentSlot() {
  const now = new Date();
  const { year, month, day } = getPhWallClockParts(now);
  const candidates = [];
  for (const dayOffset of [0, -1]) {
    const base = new Date(Date.UTC(year, month - 1, day + dayOffset));
    candidates.push(phWallClockToUTC(
      base.getUTCFullYear(), base.getUTCMonth() + 1, base.getUTCDate(), BACKUP_HOUR, 0
    ));
  }
  const past = candidates.filter((d) => d <= now);
  past.sort((a, b) => b - a);
  return past[0];
}

// Slot deadline = slot time + GRACE_HOURS. No retries after this.
function getSlotDeadline(slotDate) {
  return new Date(slotDate.getTime() + GRACE_HOURS * 60 * 60 * 1000);
}

// Next scheduled 6 PM PHT slot (today's, if not yet passed — else tomorrow's).
function getNextScheduledTime() {
  const now = new Date();
  const { year, month, day, hour, minute } = getPhWallClockParts(now);
  let next;
  if (hour < BACKUP_HOUR || (hour === BACKUP_HOUR && minute === 0)) {
    next = phWallClockToUTC(year, month, day, BACKUP_HOUR, 0);
  } else {
    const tomorrow = new Date(Date.UTC(year, month - 1, day + 1));
    next = phWallClockToUTC(
      tomorrow.getUTCFullYear(), tomorrow.getUTCMonth() + 1, tomorrow.getUTCDate(), BACKUP_HOUR, 0
    );
  }
  return {
    iso: next.toISOString(),
    pht: next.toLocaleString("en-PH", { timeZone: "Asia/Manila" }),
  };
}

// ═════════════════════════════════════════════════════════════════════════════
// 7. POSTGRES SCHEMA + TABLE NAME HELPERS
// ═════════════════════════════════════════════════════════════════════════════

// Sanitize a Firestore collection name into a safe Postgres identifier.
// Restricted charset means this is safe to interpolate directly into SQL.
function dataTableName(collectionName) {
  const safe = collectionName.toLowerCase().replace(/[^a-z0-9_]/g, "_");
  return `backup_${safe}`;
}

async function ensureSchema() {
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS backup_collection_meta (
      collection_name  TEXT PRIMARY KEY,
      doc_checksums    JSONB DEFAULT '{}'::jsonb,
      last_doc_count   INT,
      last_synced_at   TIMESTAMPTZ,
      last_sync_stats  JSONB
    );
  `);

  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS backup_slots (
      slot_time                    TIMESTAMPTZ PRIMARY KEY,
      slot_time_pht                TEXT,
      deadline                     TIMESTAMPTZ,
      deadline_pht                 TEXT,
      status                       TEXT,
      first_attempt_at             TIMESTAMPTZ,
      next_retry_at                TIMESTAMPTZ,
      next_retry_at_pht            TEXT,
      hourly_window_index          INT DEFAULT 0,
      consecutive_fails_in_window  INT DEFAULT 0,
      total_attempts               INT DEFAULT 0,
      last_attempt_at              TIMESTAMPTZ,
      last_attempt_at_pht          TEXT,
      last_attempt_status          TEXT,
      attempts                     JSONB DEFAULT '[]'::jsonb
    );
  `);

  for (const col of COLLECTIONS) {
    const table = dataTableName(col);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS ${table} (
        doc_id      TEXT PRIMARY KEY,
        data        JSONB NOT NULL,
        updated_at  TIMESTAMPTZ DEFAULT now()
      );
    `);
  }

  log("info", "Postgres schema verified/created.");
}

// ═════════════════════════════════════════════════════════════════════════════
// 8. SLOT METADATA (persisted in Postgres — this is the "admin" record)
// ═════════════════════════════════════════════════════════════════════════════
//
// Table: backup_slots — one row per scheduled 6 PM PHT slot.
//   status: "pending" | "in_progress" | "success" | "abandoned"
//   firstAttemptAt / nextRetryAt drive the hourly retry windows.
//   attempts: append-only JSON array log of every attempt made for the slot.

const SLOT_COLUMNS = {
  slotTime:                 "slot_time",
  slotTimePHT:               "slot_time_pht",
  deadline:                  "deadline",
  deadlinePHT:                "deadline_pht",
  status:                    "status",
  firstAttemptAt:            "first_attempt_at",
  nextRetryAt:               "next_retry_at",
  nextRetryAtPHT:             "next_retry_at_pht",
  hourlyWindowIndex:         "hourly_window_index",
  consecutiveFailsInWindow:  "consecutive_fails_in_window",
  totalAttempts:             "total_attempts",
  lastAttemptAt:             "last_attempt_at",
  lastAttemptAtPHT:           "last_attempt_at_pht",
  lastAttemptStatus:         "last_attempt_status",
  attempts:                  "attempts",
};

function rowToSlotMeta(row) {
  if (!row) return null;
  return {
    slotTime:                 row.slot_time?.toISOString?.() ?? row.slot_time,
    slotTimePHT:               row.slot_time_pht,
    deadline:                  row.deadline?.toISOString?.() ?? row.deadline,
    deadlinePHT:                row.deadline_pht,
    status:                    row.status,
    firstAttemptAt:            row.first_attempt_at?.toISOString?.() ?? row.first_attempt_at,
    nextRetryAt:               row.next_retry_at?.toISOString?.() ?? row.next_retry_at,
    nextRetryAtPHT:             row.next_retry_at_pht,
    hourlyWindowIndex:         row.hourly_window_index,
    consecutiveFailsInWindow:  row.consecutive_fails_in_window,
    totalAttempts:             row.total_attempts,
    lastAttemptAt:             row.last_attempt_at?.toISOString?.() ?? row.last_attempt_at,
    lastAttemptAtPHT:           row.last_attempt_at_pht,
    lastAttemptStatus:         row.last_attempt_status,
    attempts:                  row.attempts || [],
  };
}

async function readSlotMeta(slotDate) {
  try {
    const { rows } = await pgPool.query(
      `SELECT * FROM backup_slots WHERE slot_time = $1`,
      [slotDate]
    );
    return rows.length ? rowToSlotMeta(rows[0]) : null;
  } catch (err) {
    log("warn", `Could not read slot metadata: ${err.message}`);
    return null;
  }
}

// Partial "merge" upsert — only the provided fields are written/overwritten.
async function writeSlotMeta(slotDate, data) {
  const cols   = ["slot_time"];
  const vals   = [slotDate];
  const updateClauses = [];

  for (const [key, val] of Object.entries(data)) {
    const col = SLOT_COLUMNS[key];
    if (!col) continue;
    cols.push(col);
    vals.push(key === "attempts" ? JSON.stringify(val) : val);
    updateClauses.push(`${col} = EXCLUDED.${col}`);
  }

  const placeholders = cols.map((_, i) => `$${i + 1}`).join(", ");
  const sql = `
    INSERT INTO backup_slots (${cols.join(", ")})
    VALUES (${placeholders})
    ON CONFLICT (slot_time) DO UPDATE SET ${updateClauses.join(", ")}
  `;

  try {
    await pgPool.query(sql, vals);
  } catch (err) {
    log("warn", `Could not write slot metadata: ${err.message}`);
  }
}

async function appendAttemptLog(slotDate, _slotMeta, attemptRecord) {
  try {
    await pgPool.query(
      `UPDATE backup_slots
         SET attempts = COALESCE(attempts, '[]'::jsonb) || $1::jsonb
       WHERE slot_time = $2`,
      [JSON.stringify([attemptRecord]), slotDate]
    );
  } catch (err) {
    log("warn", `Could not append attempt log: ${err.message}`);
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// 9. PER-COLLECTION METADATA (checksum diffing) — Postgres
// ═════════════════════════════════════════════════════════════════════════════

async function readCollectionMeta(collectionName) {
  try {
    const { rows } = await pgPool.query(
      `SELECT * FROM backup_collection_meta WHERE collection_name = $1`,
      [collectionName]
    );
    if (!rows.length) return null;
    const r = rows[0];
    return {
      docChecksums:  r.doc_checksums || {},
      lastDocCount:  r.last_doc_count,
      lastSyncedAt:  r.last_synced_at,
      lastSyncStats: r.last_sync_stats,
    };
  } catch (err) {
    log("warn", `Could not read collection metadata [${collectionName}]: ${err.message}`);
    return null;
  }
}

async function writeCollectionMeta(collectionName, metadata) {
  try {
    await pgPool.query(
      `
      INSERT INTO backup_collection_meta
        (collection_name, doc_checksums, last_doc_count, last_synced_at, last_sync_stats)
      VALUES ($1, $2::jsonb, $3, $4, $5::jsonb)
      ON CONFLICT (collection_name) DO UPDATE SET
        doc_checksums   = EXCLUDED.doc_checksums,
        last_doc_count  = EXCLUDED.last_doc_count,
        last_synced_at  = EXCLUDED.last_synced_at,
        last_sync_stats = EXCLUDED.last_sync_stats
      `,
      [
        collectionName,
        JSON.stringify(metadata.docChecksums || {}),
        metadata.lastDocCount ?? null,
        metadata.lastSyncedAt ?? null,
        JSON.stringify(metadata.lastSyncStats || {}),
      ]
    );
  } catch (err) {
    log("warn", `Could not write collection metadata [${collectionName}]: ${err.message}`);
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// 10. CORE SYNC LOGIC
// ═════════════════════════════════════════════════════════════════════════════

// Convert Firestore client-SDK types into plain JSON-safe values so they can
// be stored in a Postgres JSONB column.
function sanitizeForJSON(value) {
  if (value === null || value === undefined) return value;

  if (value instanceof ClientTimestamp) return value.toDate().toISOString();
  if (value instanceof ClientGeoPoint) {
    return { latitude: value.latitude, longitude: value.longitude };
  }
  if (value instanceof Date) return value.toISOString();

  if (Array.isArray(value)) return value.map(sanitizeForJSON);

  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = sanitizeForJSON(v);
    return out;
  }

  return value; // string, number, boolean
}

function buildChecksumMap(docs) {
  const map = {};
  for (const doc of docs) {
    const data = doc.data();
    const ts =
      data?.updatedAt?.toDate?.()?.toISOString() ||
      data?.updated_at?.toDate?.()?.toISOString() ||
      data?.createdAt?.toDate?.()?.toISOString() ||
      data?.created_at?.toDate?.()?.toISOString() ||
      String(JSON.stringify(data).length);
    map[doc.id] = ts;
  }
  return map;
}

const BATCH_LIMIT = 400;

async function upsertDocs(tableName, docs) {
  for (let i = 0; i < docs.length; i += BATCH_LIMIT) {
    const chunk   = docs.slice(i, i + BATCH_LIMIT);
    const ids     = chunk.map((d) => d.id);
    const dataArr = chunk.map((d) => JSON.stringify(sanitizeForJSON(d.data())));

    await pgPool.query(
      `
      INSERT INTO ${tableName} (doc_id, data, updated_at)
      SELECT id, data, now()
      FROM UNNEST($1::text[], $2::jsonb[]) AS t(id, data)
      ON CONFLICT (doc_id) DO UPDATE SET
        data       = EXCLUDED.data,
        updated_at = EXCLUDED.updated_at
      `,
      [ids, dataArr]
    );
    log("info", `  ✓ Upserted batch of ${chunk.length} docs into ${tableName}.`);
  }
}

async function deleteDocs(tableName, ids) {
  for (let i = 0; i < ids.length; i += BATCH_LIMIT) {
    const chunk = ids.slice(i, i + BATCH_LIMIT);
    await pgPool.query(`DELETE FROM ${tableName} WHERE doc_id = ANY($1::text[])`, [chunk]);
    log("info", `  ✓ Deleted batch of ${chunk.length} stale rows from ${tableName}.`);
  }
}

async function syncCollection(collectionName) {
  const stats = { reads: 0, writes: 0, deletes: 0, skipped: 0, status: "ok" };
  const table = dataTableName(collectionName);

  log("info", `━━━ Syncing collection: [${collectionName}] → table ${table} ━━━`);

  const meta = await readCollectionMeta(collectionName);
  const previousChecksums = meta?.docChecksums || {};
  log("info", `  Metadata loaded. Previously tracked ${Object.keys(previousChecksums).length} docs.`);

  let primarySnapshot;
  try {
    primarySnapshot = await getDocs(fsCollection(primaryDB, collectionName));
    stats.reads += primarySnapshot.size;
  } catch (err) {
    log("error", `  Failed to read primary [${collectionName}]: ${err.message}`);
    stats.status = "error";
    stats.error = err.message;
    return stats;
  }

  if (primarySnapshot.empty) {
    log("warn", `  [${collectionName}] is EMPTY in primary — skipping to avoid data loss.`);
    stats.status = "skipped_empty";
    return stats;
  }

  const primaryDocs      = primarySnapshot.docs;
  const currentChecksums = buildChecksumMap(primaryDocs);
  const toUpsert = [];
  const toDelete = [];

  for (const doc of primaryDocs) {
    if (previousChecksums[doc.id] !== currentChecksums[doc.id]) toUpsert.push(doc);
    else stats.skipped++;
  }
  const currentIds = new Set(primaryDocs.map((d) => d.id));
  for (const oldId of Object.keys(previousChecksums)) {
    if (!currentIds.has(oldId)) toDelete.push(oldId);
  }

  log("info", `  Diff → upsert: ${toUpsert.length}, delete: ${toDelete.length}, unchanged: ${stats.skipped}`);

  if (toUpsert.length === 0 && toDelete.length === 0) {
    log("info", `  No changes detected for [${collectionName}].`);
    stats.status = "no_changes";
    return stats;
  }

  try {
    if (toUpsert.length) {
      await upsertDocs(table, toUpsert);
      stats.writes += toUpsert.length;
    }
    if (toDelete.length) {
      await deleteDocs(table, toDelete);
      stats.deletes += toDelete.length;
    }
  } catch (err) {
    log("error", `  Postgres write failed for [${collectionName}]: ${err.message}`);
    stats.status = "error";
    stats.error = err.message;
    return stats;
  }

  await writeCollectionMeta(collectionName, {
    docChecksums:  currentChecksums,
    lastDocCount:  primaryDocs.length,
    lastSyncedAt:  new Date().toISOString(),
    lastSyncStats: stats,
  });

  log("info", `  ✓ Collection metadata updated for [${collectionName}].`);
  return stats;
}

// ═════════════════════════════════════════════════════════════════════════════
// 11. MASTER BACKUP RUNNER
// ═════════════════════════════════════════════════════════════════════════════

async function runBackup(triggeredBy, slotDate, slotMeta, attemptNumber, hourlyWindow, windowAttempt) {
  if (backupState.isRunning) {
    log("warn", "Backup already in progress — skipping.");
    return null;
  }

  backupState.isRunning = true;
  const startedAt  = new Date();
  const startMs     = startedAt.getTime();
  const startLabel = startedAt.toLocaleString("en-PH", { timeZone: "Asia/Manila" });

  log("info", "╔══════════════════════════════════════════════╗");
  log("info", "║   FIRESTORE → POSTGRES (AIVEN) BACKUP STARTED ║");
  log("info", `║  ${startLabel.padEnd(44)}║`);
  log("info", `║  Triggered by : ${triggeredBy.padEnd(29)}║`);
  log("info", `║  Attempt      : #${String(attemptNumber).padEnd(28)}║`);
  log("info", `║  Hourly window: ${String(hourlyWindow).padEnd(29)}║`);
  log("info", `║  Window try   : ${String(windowAttempt).padEnd(29)}║`);
  log("info", "╚══════════════════════════════════════════════╝");

  // Write "in_progress" immediately so there's a record even if the process
  // crashes mid-backup.
  await writeSlotMeta(slotDate, {
    status:        "in_progress",
    lastAttemptAt: startedAt.toISOString(),
    totalAttempts: attemptNumber,
  });

  const results = {};
  for (const col of COLLECTIONS) {
    try {
      results[col] = await syncCollection(col);
    } catch (err) {
      log("error", `Unexpected error syncing [${col}]: ${err.message}`);
      results[col] = { status: "fatal_error", error: err.message };
    }
  }

  const finishedAt     = new Date();
  const elapsedSeconds = ((finishedAt - startMs) / 1000).toFixed(2);

  let totalReads = 0, totalWrites = 0, totalDeletes = 0;
  for (const s of Object.values(results)) {
    totalReads   += s.reads   || 0;
    totalWrites  += s.writes  || 0;
    totalDeletes += s.deletes || 0;
  }

  const allFailed = Object.values(results).every((r) => r.status === "error" || r.status === "fatal_error");
  const anyFailed = Object.values(results).some((r)  => r.status === "error" || r.status === "fatal_error");
  const runStatus = allFailed ? "all_failed" : anyFailed ? "partial" : "success";

  log("info", `Backup ${runStatus} — reads: ${totalReads}, writes: ${totalWrites}, deletes: ${totalDeletes}, elapsed: ${elapsedSeconds}s`);

  const attemptRecord = {
    attemptNumber,
    hourlyWindow,
    windowAttempt,
    triggeredBy,
    startedAt:     startedAt.toISOString(),
    startedAtPHT:  startLabel,
    finishedAt:    finishedAt.toISOString(),
    finishedAtPHT: finishedAt.toLocaleString("en-PH", { timeZone: "Asia/Manila" }),
    elapsedSeconds,
    status: runStatus,
    totalReads, totalWrites, totalDeletes,
    collections: Object.fromEntries(
      Object.entries(results).map(([k, v]) => [k, {
        status:  v.status,
        reads:   v.reads   || 0,
        writes:  v.writes  || 0,
        deletes: v.deletes || 0,
        ...(v.error ? { error: v.error } : {}),
      }])
    ),
  };

  await appendAttemptLog(slotDate, slotMeta, attemptRecord);

  backupState.isRunning   = false;
  backupState.lastRun     = finishedAt.toISOString();
  backupState.lastResults = { attemptRecord, triggeredBy };

  return attemptRecord;
}

// ═════════════════════════════════════════════════════════════════════════════
// 12. RETRY ORCHESTRATOR
// ═════════════════════════════════════════════════════════════════════════════
//
//   [cron tick / HTTP hit]
//        │
//        ▼
//   Read slot metadata from Postgres (backup_slots)
//        │
//        ├── status = "success"   ────────────────────► Done for this slot
//        ├── status = "abandoned" ────────────────────► Done for this slot
//        └── status = "pending" | "in_progress" | null
//                 │
//                 ▼
//        Past the slot deadline (slot + GRACE_HOURS)?
//                 │
//                 ├── Yes ──► Mark abandoned, stop
//                 └── No
//                          │
//                          ▼
//                 First attempt ever for this slot?
//                          │
//                          ├── No  ──► Run now (window 0, attempt 1)
//                          └── Yes
//                                   │
//                                   ▼
//                          New hourly window open?
//                                   │
//                                   ├── No  ──► Hit MAX_CONSECUTIVE_FAILS?
//                                   │           ├── No  ──► Run now (same window)
//                                   │           └── Yes ──► Wait for next window
//                                   └── Yes ──► Open new window, run now

let orchestratorInFlight = false;

async function runSlotOrchestrator(triggeredBy) {
  if (backupState.isRunning || orchestratorInFlight) return;
  orchestratorInFlight = true;

  try {
    const now  = new Date();
    const slot = getMostRecentSlot();
    if (!slot) return;

    const deadline         = getSlotDeadline(slot);
    const slotLabelPHT     = slot.toLocaleString("en-PH", { timeZone: "Asia/Manila" });
    const deadlineLabelPHT = deadline.toLocaleString("en-PH", { timeZone: "Asia/Manila" });

    if (now >= deadline) return; // Past grace window entirely — wait for next slot.

    let meta = await readSlotMeta(slot);

    if (meta?.status === "success" || meta?.status === "abandoned") return;

    if (!meta || !meta.firstAttemptAt) {
      log("info", `Slot ${slotLabelPHT}: first attempt. Deadline: ${deadlineLabelPHT}.`);

      const initMeta = {
        slotTime:                 slot.toISOString(),
        slotTimePHT:               slotLabelPHT,
        deadline:                  deadline.toISOString(),
        deadlinePHT:               deadlineLabelPHT,
        status:                   "pending",
        firstAttemptAt:            now.toISOString(),
        nextRetryAt:               null,
        hourlyWindowIndex:         0,
        consecutiveFailsInWindow:  0,
        totalAttempts:             0,
        lastAttemptAt:             null,
        lastAttemptStatus:         null,
        attempts:                  [],
      };
      await writeSlotMeta(slot, initMeta);
      meta = initMeta;
    }

    const firstAttemptAt = new Date(meta.firstAttemptAt);
    const msSinceFirst   = now - firstAttemptAt;
    const currentWindow  = Math.floor(msSinceFirst / (60 * 60 * 1000)); // 0, 1, 2 …
    const lastWindow     = meta.hourlyWindowIndex ?? 0;
    const failsInWindow  = meta.consecutiveFailsInWindow ?? 0;
    const totalAttempts  = meta.totalAttempts ?? 0;

    const newWindowOpen = currentWindow > lastWindow;

    if (!newWindowOpen && failsInWindow >= MAX_CONSECUTIVE_FAILS) {
      const nextRetryAt = meta.nextRetryAt ? new Date(meta.nextRetryAt) : null;
      const waitMin = nextRetryAt ? Math.ceil((nextRetryAt - now) / 60000) : "?";
      log("info", `Slot ${slotLabelPHT}: window ${lastWindow} exhausted (${failsInWindow}/${MAX_CONSECUTIVE_FAILS} fails). Next retry in ~${waitMin} min.`);
      return;
    }

    let windowIndex, windowAttempt;
    if (newWindowOpen) {
      windowIndex   = currentWindow;
      windowAttempt = 1;
      log("info", `Slot ${slotLabelPHT}: opening hourly window ${windowIndex} (retry hour ${windowIndex}).`);
      await writeSlotMeta(slot, {
        hourlyWindowIndex:        windowIndex,
        consecutiveFailsInWindow: 0,
      });
      meta = (await readSlotMeta(slot)) || meta;
      meta.consecutiveFailsInWindow = 0;
      meta.hourlyWindowIndex        = windowIndex;
    } else {
      windowIndex   = lastWindow;
      windowAttempt = failsInWindow + 1;
    }

    const attemptNumber = totalAttempts + 1;

    const record = await runBackup(triggeredBy, slot, meta, attemptNumber, windowIndex, windowAttempt);
    if (!record) return; // was already running

    const succeeded = record.status === "success";
    const newFails  = succeeded ? 0 : (meta.consecutiveFailsInWindow ?? 0) + 1;

    const nextWindowStart = new Date(firstAttemptAt.getTime() + (windowIndex + 1) * 60 * 60 * 1000);
    const nextRetryAt     = nextWindowStart < deadline ? nextWindowStart.toISOString() : null;

    const updatedSlotMeta = {
      status:                   succeeded ? "success" : (nextRetryAt ? "pending" : "abandoned"),
      lastAttemptAt:            record.finishedAt,
      lastAttemptAtPHT:         record.finishedAtPHT,
      lastAttemptStatus:        record.status,
      totalAttempts:            attemptNumber,
      consecutiveFailsInWindow: newFails,
      nextRetryAt:              succeeded ? null : nextRetryAt,
      nextRetryAtPHT:           (succeeded || !nextRetryAt) ? null
        : new Date(nextRetryAt).toLocaleString("en-PH", { timeZone: "Asia/Manila" }),
    };
    await writeSlotMeta(slot, updatedSlotMeta);

    if (succeeded) {
      log("info", `✅ Slot ${slotLabelPHT}: backup succeeded on attempt #${attemptNumber}.`);
    } else if (!nextRetryAt) {
      log("warn", `⛔ Slot ${slotLabelPHT}: all retry windows exhausted (deadline ${deadlineLabelPHT}). Marking abandoned.`);
    } else if (newFails >= MAX_CONSECUTIVE_FAILS) {
      log("warn", `⚠️  Slot ${slotLabelPHT}: window ${windowIndex} exhausted (${newFails}/${MAX_CONSECUTIVE_FAILS} fails). Next hourly retry at ${new Date(nextRetryAt).toLocaleString("en-PH", { timeZone: "Asia/Manila" })}.`);
    } else {
      log("warn", `↩️  Slot ${slotLabelPHT}: attempt #${attemptNumber} failed (${newFails}/${MAX_CONSECUTIVE_FAILS}). Will retry in window ${windowIndex} again.`);
    }

  } catch (err) {
    log("error", `Orchestrator error: ${err.message}`);
  } finally {
    orchestratorInFlight = false;
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// 13. HTTP SERVER WITH CORS SUPPORT (includes /admin metadata endpoint)
// ═════════════════════════════════════════════════════════════════════════════

// Helper function to add CORS headers to responses
function setCorsHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Max-Age', '86400'); // 24 hours cache for preflight
}

const server = http.createServer((req, res) => {
  const url = req.url;

  // Handle preflight OPTIONS request (browsers send this before actual request)
  if (req.method === 'OPTIONS') {
    setCorsHeaders(res);
    res.writeHead(200);
    res.end();
    return;
  }

  // Add CORS headers to all responses
  setCorsHeaders(res);

  if (url === "/" || url === "/health") {
    runSlotOrchestrator("http_health_check").catch((err) =>
      log("error", `Orchestrator trigger error: ${err.message}`)
    );
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      status:         "ok",
      service:        "Firestore → PostgreSQL (Aiven) Backup",
      currentTimePHT: phNow(),
      nextBackupPHT:  getNextScheduledTime().pht,
      isRunning:      backupState.isRunning,
      lastRun:        backupState.lastRun,
    }));
    return;
  }

  if (url === "/status") {
    runSlotOrchestrator("http_status_check").catch((err) =>
      log("error", `Orchestrator trigger error: ${err.message}`)
    );
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      service:        "Firestore → PostgreSQL (Aiven) Backup",
      collections:    COLLECTIONS,
      currentTimePHT: phNow(),
      nextBackupPHT:  getNextScheduledTime().pht,
      isRunning:      backupState.isRunning,
      lastRun:        backupState.lastRun,
      lastResults:    backupState.lastResults,
    }, null, 2));
    return;
  }

  // ── /admin — full metadata for an admin dashboard ──────────────────────
  if (url === "/admin" || url === "/admin/metadata") {
    (async () => {
      try {
        const slot     = getMostRecentSlot();
        const deadline = getSlotDeadline(slot);
        const meta     = await readSlotMeta(slot);
        const next     = getNextScheduledTime();

        const { rows: collRows } = await pgPool.query(
          `SELECT collection_name, last_doc_count, last_synced_at, last_sync_stats
             FROM backup_collection_meta
            ORDER BY collection_name`
        );

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({
          service:        "Firestore → PostgreSQL (Aiven) Backup",
          currentTimePHT: phNow(),
          isRunning:      backupState.isRunning,

          schedule: {
            backupTimePHT:      `${String(BACKUP_HOUR).padStart(2, "0")}:00`,
            maxAttemptsPerHour: MAX_CONSECUTIVE_FAILS,
            fastRetryDelayMs:   CONSECUTIVE_RETRY_DELAY,
            graceHours:         GRACE_HOURS,
            nextBackupAtPHT:    next.pht,
            nextBackupAtISO:    next.iso,
          },

          currentSlot: {
            slotTimePHT:              slot.toLocaleString("en-PH", { timeZone: "Asia/Manila" }),
            deadlinePHT:               deadline.toLocaleString("en-PH", { timeZone: "Asia/Manila" }),
            status:                   meta?.status ?? "not_started",
            totalAttempts:            meta?.totalAttempts ?? 0,
            hourlyWindowIndex:        meta?.hourlyWindowIndex ?? 0,
            consecutiveFailsInWindow: meta?.consecutiveFailsInWindow ?? 0,
            lastAttemptAtPHT:         meta?.lastAttemptAtPHT ?? null,
            lastAttemptStatus:        meta?.lastAttemptStatus ?? null,
            nextRetryAtPHT:           meta?.nextRetryAtPHT ?? null,
            attempts:                 meta?.attempts ?? [],
          },

          collections: collRows.map((r) => ({
            collection:    r.collection_name,
            lastDocCount:  r.last_doc_count,
            lastSyncedAt:  r.last_synced_at,
            lastSyncStats: r.last_sync_stats,
          })),
        }, null, 2));
      } catch (err) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: err.message }));
      }
    })();
    return;
  }

  if (url === "/trigger" && req.method === "POST") {
    if (backupState.isRunning) {
      res.writeHead(409, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Backup already in progress" }));
      return;
    }
    res.writeHead(202, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ message: "Backup triggered manually" }));
    runSlotOrchestrator("manual_trigger").catch((err) =>
      log("error", `Manual trigger error: ${err.message}`)
    );
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

// ═════════════════════════════════════════════════════════════════════════════
// 14. STARTUP
// ═════════════════════════════════════════════════════════════════════════════

async function start() {
  log("info", "Firestore → PostgreSQL (Aiven) Backup Service starting...");
  await ensureSchema();

  server.listen(PORT, () => {
    log("info", `HTTP server listening on port ${PORT}`);
    log("info", `  GET  /            — health check + catch-up trigger`);
    log("info", `  GET  /health      — health check + catch-up trigger`);
    log("info", `  GET  /status      — last backup results`);
    log("info", `  GET  /admin       — full admin metadata (schedule, current slot, per-collection stats)`);
    log("info", `  POST /trigger     — run backup now (for testing)`);
  });

  log("info", `Scheduled slot  : ${BACKUP_HOUR}:00 PHT daily`);
  log("info", `Grace window    : ${GRACE_HOURS} hours after the slot`);
  log("info", `Fast retry      : up to ${MAX_CONSECUTIVE_FAILS} attempts per hourly window`);
  log("info", `Collections     : ${COLLECTIONS.join(", ")}`);

  cron.schedule(
    "* * * * *",
    () => {
      runSlotOrchestrator("cron_tick").catch((err) =>
        log("error", `Cron orchestrator error: ${err.message}`)
      );
    },
    { timezone: "Asia/Manila" }
  );
}

start().catch((err) => {
  log("error", `Fatal startup error: ${err.message}`);
  process.exit(1);
});

// ═════════════════════════════════════════════════════════════════════════════
// 15. GRACEFUL SHUTDOWN
// ═════════════════════════════════════════════════════════════════════════════

async function shutdown(signal) {
  log("info", `${signal} received — closing Postgres pool and exiting.`);
  try { await pgPool.end(); } catch (_) {}
  process.exit(0);
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT",  () => shutdown("SIGINT"));
