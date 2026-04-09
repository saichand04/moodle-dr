# OpenClaw Integration Guide

Moodle DR Dashboard v11 exposes a machine-readable status API that allows AI agents — including OpenClaw (local AI assistant for WhatsApp/Telegram/Discord) — to query live DR status, replication lag, and readiness scores without logging into the dashboard.

---

## What OpenClaw Can Do

- Ask "What's the Moodle DR status?" and get a real-time answer
- Receive automatic alerts when DR readiness score drops below threshold
- Check replication lag, lsyncd status, file sync health
- Monitor via a 5-minute polling cron inside OpenClaw

---

## Quick Setup

### Step 1 — Generate an API Token

1. Open the dashboard: `http://192.168.1.241:8080`
2. Navigate to **Integrations → OpenClaw API**
3. Click **Create Token**
4. Copy the token immediately (stored as a hash — not recoverable after creation)

### Step 2 — Download the Skill File

On the OpenClaw API page, click **Download OpenClaw Skill File**.

This downloads a `SKILL.md` file that teaches OpenClaw how to:
- Query the Moodle DR status endpoint
- Parse the response
- Format human-readable alerts
- Run periodic monitoring via cron

### Step 3 — Install the Skill in OpenClaw

```bash
# Copy skill file to your OpenClaw skills directory
cp moodle-dr-skill.json ~/.openclaw/skills/
# or for the SKILL.md format:
cp SKILL.md ~/.openclaw/skills/moodle-dr.md

# Restart OpenClaw to load the new skill
openclaw restart
```

---

## Public API Reference

### Status Endpoint (No Auth Required)

```
GET http://192.168.1.241:8080/api/public/status
```

**Response:**
```json
{
  "ts": "2026-04-09T06:00:00.000Z",
  "file_sync": {
    "lsyncd_running": true,
    "rsync_running": false,
    "last_result": "success"
  },
  "db_sync": {
    "status": "healthy",
    "lag_seconds": 0,
    "heartbeat_lag": 0.8,
    "io_running": "Yes",
    "sql_running": "Yes"
  },
  "dr_readiness": {
    "score": 95,
    "rpo_seconds": 60
  }
}
```

### Authenticated Status (Token Required)

For more detailed data (GTID info, disk usage, connectivity state):

```
GET http://192.168.1.241:8080/api/public/status
X-API-Token: your-token-here
```

### SSE Stream (Real-Time Events)

For real-time push-based monitoring (Server-Sent Events):

```
GET http://192.168.1.241:8080/api/public/stream
```

Events are emitted every 30 seconds or when status changes.

### Prometheus Metrics

```
GET http://192.168.1.241:8080/metrics
```

Returns Prometheus-format metrics:
```
# HELP moodle_dr_db_lag_seconds MySQL replication lag in seconds
moodle_dr_db_lag_seconds 0
# HELP moodle_dr_readiness_score DR readiness score 0-100
moodle_dr_readiness_score 95
# HELP moodle_dr_lsyncd_running lsyncd service running (1=yes, 0=no)
moodle_dr_lsyncd_running 1
```

---

## OpenClaw Skill File (Manual)

If the dashboard's skill file download isn't available, create this manually:

```markdown
# Moodle DR Monitor

## Description
Monitors Moodle Disaster Recovery status including file sync, MySQL replication lag, and DR readiness score.

## Trigger Phrases
- "moodle dr status"
- "check dr readiness"
- "replication lag"
- "is moodle backup ok"

## API
- Endpoint: http://192.168.1.241:8080/api/public/status
- Method: GET
- Headers: { "X-API-Token": "YOUR_TOKEN_HERE" }

## Response Parsing
- `file_sync.lsyncd_running` → File sync health (true = OK)
- `db_sync.status` → DB status (healthy/lagging/error/stopped)
- `db_sync.lag_seconds` → Replication lag in seconds
- `dr_readiness.score` → Readiness score 0-100 (≥90 = good, <70 = critical)
- `dr_readiness.rpo_seconds` → Recovery Point Objective in seconds

## Alert Conditions
- DR score < 80 → Warning
- DR score < 60 → Critical alert
- lsyncd_running = false → File sync down
- db_sync.status = "error" → Replication error
- db_sync.lag_seconds > 300 → Lag > 5 minutes

## Response Format
"Moodle DR Status:
  File Sync: {lsyncd_running ? '✓ Active' : '✗ Down'}
  DB Replication: {db_sync.status} (lag: {db_sync.lag_seconds}s)
  DR Readiness: {dr_readiness.score}/100
  RPO: {dr_readiness.rpo_seconds}s"

## Cron
Run every 5 minutes. Alert if any condition above is triggered.
```

---

## Making the Dashboard Publicly Accessible

For OpenClaw to reach the dashboard from outside your LAN, use one of these options:

### Option A: Cloudflare Tunnel (Easiest, Free)
```
Dashboard → Integrations → External Access → Cloudflare Tunnel → Start Tunnel
```
Generates a public `*.trycloudflare.com` URL. No account needed for quick tunnels.
For a persistent URL, use a named tunnel with your Cloudflare account token.

### Option B: Tailscale (Most Secure)
```
Dashboard → Integrations → External Access → Tailscale → Connect (tailscale up)
```
Joins the server to your Tailscale mesh. Access via `http://100.x.x.x:8080` from any device on your tailnet.

### Option C: SSH Tunnel (For Developers)
```
Dashboard → Integrations → External Access → SSH Tunnel
```
Generates the exact `ssh -L` command to run on your laptop. Forwards dashboard port locally.

### Option D: Webhook Push (Event-Driven)
```
Dashboard → Integrations → External Access → Webhook Push
```
Instead of OpenClaw polling, the dashboard POSTs events to your webhook URL when:
- DR score changes
- Replication lag exceeds threshold
- lsyncd goes down
- Failover is initiated

Webhook payload example:
```json
{
  "event": "dr_score_changed",
  "ts": "2026-04-09T06:00:00Z",
  "data": {
    "score": 75,
    "previous_score": 95,
    "lsyncd_running": false
  }
}
```
HMAC signature in header: `X-Moodle-DR-Secret: sha256=...`

---

## cURL Quick Test

Test your token from any machine:
```bash
curl -s -H "X-API-Token: YOUR_TOKEN_HERE" \
  http://192.168.1.241:8080/api/public/status | python3 -m json.tool
```

Test without token (public read-only):
```bash
curl -s http://192.168.1.241:8080/api/public/status | python3 -m json.tool
```

---

## Grafana / Prometheus Integration

The `/metrics` endpoint is compatible with Prometheus scraping.

Add to your `prometheus.yml`:
```yaml
scrape_configs:
  - job_name: 'moodle-dr'
    static_configs:
      - targets: ['192.168.1.241:8080']
    metrics_path: /metrics
    scrape_interval: 30s
```

Then build a Grafana dashboard using `moodle_dr_*` metrics.

---

*OpenClaw: https://github.com/openclaw/openclaw — Local AI agent for messaging platforms*
