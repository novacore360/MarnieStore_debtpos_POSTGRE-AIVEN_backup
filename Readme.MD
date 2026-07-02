# Firestore → PostgreSQL (Aiven) Backup Service

Automated backup of your Firestore POS data into a PostgreSQL database on Aiven.
Runs once daily at **6:00 PM Philippine Time**, with retry protection and an
admin metadata endpoint so you can always see backup health at a glance.

Reads Firestore with the **client SDK only** — no Firebase Admin SDK, no
service-account JSON anywhere in this service. Firestore access is governed
entirely by your Firestore security rules.

---

## How It Works

```
Every minute (cron tick, Asia/Manila)
    │
    ▼
Is there an active 6 PM PHT slot that hasn't succeeded/been abandoned yet?
    │
    ├── No  → do nothing
    │
    └── Yes
           │
           ▼
    Read per-collection metadata from Postgres (cheap, 1 row per collection)
           │
           ▼
    Read ALL documents from that Firestore collection (client SDK)
           │
           ▼
    Collection empty in Firestore?
           │
           ├── Yes → SKIP (data-loss guard — never wipes Postgres because
           │          Firestore looked empty, e.g. rules misconfigured)
           │
           └── No
                  │
                  ▼
           Diff checksums vs. last-synced metadata
                  │
                  ├── No changes → 0 writes, exit
                  │
                  └── Changes found
                         │
                         ├── Upsert changed/new docs → backup_<collection> table (JSONB)
                         ├── Delete rows for docs removed from Firestore
                         └── Update collection metadata
```

Each Firestore collection is mirrored into its own Postgres table:

```
backup_<collection> (
  doc_id      TEXT PRIMARY KEY,   -- Firestore document ID
  data        JSONB NOT NULL,     -- full document, as JSON
  updated_at  TIMESTAMPTZ         -- when this row was last written
)
```

Firestore `Timestamp` fields become ISO-8601 strings and `GeoPoint` fields
become `{ latitude, longitude }` objects inside the JSON — both fully queryable
with Postgres's JSONB operators (`data->>'createdAt'`, `data->'location'`, etc).
Tables and metadata tables are created automatically on startup if they don't
already exist — no manual migration step.

---

## Retry Policy (matches your spec)

- **Schedule:** one slot per day, at `BACKUP_HOUR` (default **18:00 / 6 PM PHT**).
- **Per-hour attempt limit:** up to `MAX_CONSECUTIVE_FAILS` (default **5**)
  attempts within the same hourly window before backing off.
- **Rerun window:** if a slot keeps failing, it opens a new hourly retry
  window anchored to the first attempt (e.g. 6:04 PM → 7:04 PM → 8:04 PM …),
  each with its own 5-attempt budget.
- **Grace / hard stop:** retries stop entirely `GRACE_HOURS` (default **5**)
  after the 6 PM slot — i.e. no more retries after **11 PM**. The slot is then
  marked `abandoned` and left alone until the next day's 6 PM slot.

All of this state — status, attempt counts, timestamps, full attempt log — is
persisted in the `backup_slots` Postgres table, so it survives restarts and
deploys.

---

## Admin / Metadata Endpoint

```
GET /admin        (or /admin/metadata)
```

Returns everything an admin dashboard needs:

```json
{
  "service": "Firestore → PostgreSQL (Aiven) Backup",
  "currentTimePHT": "7/2/2026, 8:03:00 AM",
  "isRunning": false,
  "schedule": {
    "backupTimePHT": "18:00",
    "maxAttemptsPerHour": 5,
    "fastRetryDelayMs": 30000,
    "graceHours": 5,
    "nextBackupAtPHT": "7/2/2026, 6:00:00 PM",
    "nextBackupAtISO": "2026-07-02T10:00:00.000Z"
  },
  "currentSlot": {
    "slotTimePHT": "7/1/2026, 6:00:00 PM",
    "deadlinePHT": "7/1/2026, 11:00:00 PM",
    "status": "success",
    "totalAttempts": 1,
    "hourlyWindowIndex": 0,
    "consecutiveFailsInWindow": 0,
    "lastAttemptAtPHT": "7/1/2026, 6:00:12 PM",
    "lastAttemptStatus": "success",
    "nextRetryAtPHT": null,
    "attempts": [ "full attempt log for this slot" ]
  },
  "collections": [
    { "collection": "customers", "lastDocCount": 812, "lastSyncedAt": "...", "lastSyncStats": { "reads": 812, "writes": 3, "deletes": 0, "skipped": 809, "status": "ok" } }
  ]
}
```

Other endpoints:

| Route | Method | Purpose |
|---|---|---|
| `/` or `/health` | GET | Health check + triggers a catch-up check |
| `/status` | GET | Quick summary of the last run |
| `/admin` | GET | Full metadata for an admin dashboard (above) |
| `/trigger` | POST | Manually run a backup attempt now |

---

## Setup

### 1. Firestore — Client SDK config (no service account needed)

Firebase Console → Project Settings → General → **Your apps** → Web app.
Copy the `firebaseConfig` values into `.env`.

Make sure your Firestore **security rules** allow read access for whatever
auth context this service runs under (e.g. a rule allowing reads for a
specific custom claim, or public read if that's acceptable for your data).

### 2. PostgreSQL — Aiven

1. Create a PostgreSQL service on [Aiven](https://aiven.io).
2. From the service **Overview** page, copy the **Service URI** → `DATABASE_URL`.
3. (Recommended) Copy the **CA Certificate** shown on the same page → `PG_CA_CERT`
   as one line with literal `\n`, for full TLS verification. Without it, the
   connection is still encrypted, just not certificate-verified.

### 3. Create `.env`

```bash
cp .env.example .env
```

```env
PRIMARY_API_KEY=...
PRIMARY_AUTH_DOMAIN=your-project.firebaseapp.com
PRIMARY_PROJECT_ID=your-project
PRIMARY_STORAGE_BUCKET=your-project.appspot.com
PRIMARY_MESSAGING_SENDER_ID=...
PRIMARY_APP_ID=...

DATABASE_URL=postgres://avnadmin:PASSWORD@your-service.aivencloud.com:PORT/defaultdb?sslmode=require
PG_CA_CERT="-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n"

COLLECTIONS=customers,purchases,products
TZ=Asia/Manila
BACKUP_HOUR=18

MAX_CONSECUTIVE_FAILS=5
CONSECUTIVE_RETRY_DELAY=30000
GRACE_HOURS=5
```

### 4. Install & run

```bash
npm install
npm start
```

On first start, the service creates all needed tables in Postgres automatically.

---

## Deploying to Render

1. Push this folder to a GitHub repo.
2. On [Render](https://render.com), create a new **Background Worker** (or Web
   Service if you want the `/admin` endpoint reachable publicly).
3. Build Command: `npm install` · Start Command: `npm start` · Environment: Node.
4. Add all `.env` variables under **Environment → Environment Variables**.
5. Deploy.

> This service sleeps-safe on the free tier the same way the original did:
> every request to `/`, `/health`, `/status`, or `/admin`, plus every cron
> tick, re-checks whether today's 6 PM slot still needs to run and, if so,
> runs it — so a late-waking free-tier instance still catches up within the
> `GRACE_HOURS` window.

---

## Read/Write Cost Notes

- A backup with no changes since last sync costs roughly **1 Firestore read
  per document** (needed to diff) but **0 Postgres writes**.
- Checksum diffing (via `updatedAt`/`createdAt` fields when present, else a
  length-based fallback) means only changed/new/deleted documents get written.
- Postgres writes are batched (400 rows/statement via `UNNEST`).
- Empty-collection guard prevents an accidental Firestore rules/outage from
  wiping your Postgres backup.

---

## Collections Backed Up

- `customers`
- `purchases`
- `products`

Add more by updating `COLLECTIONS` in `.env` — a matching `backup_<name>`
table is created automatically on next startup.
