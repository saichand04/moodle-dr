"""
integrations.py — External access & notification integrations.

Endpoints:
  GET/POST /api/integrations/cloudflare/config
  GET      /api/integrations/cloudflare/status
  POST     /api/integrations/cloudflare/start
  POST     /api/integrations/cloudflare/stop

  GET/POST /api/integrations/tailscale/config
  GET      /api/integrations/tailscale/status
  POST     /api/integrations/tailscale/up
  POST     /api/integrations/tailscale/down

  GET/POST /api/integrations/webhook/config
  POST     /api/integrations/webhook/test

  GET/POST /api/integrations/notifications/config
  POST     /api/integrations/notifications/test/{channel}

  GET      /api/integrations/openclaw/tokens
  POST     /api/integrations/openclaw/tokens
  DELETE   /api/integrations/openclaw/tokens/{token_id}
  GET      /api/integrations/openclaw/skill-file
"""
import asyncio
import json
import secrets
import subprocess
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

import db_replication_db as db_db
import state

router = APIRouter(prefix="/api/integrations", tags=["integrations"])

# ── Helpers ───────────────────────────────────────────────────────────────────

def run_cmd(cmd: list, timeout: int = 15) -> dict:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {"ok": r.returncode == 0, "stdout": r.stdout.strip(), "stderr": r.stderr.strip()}
    except subprocess.TimeoutExpired:
        return {"ok": False, "stdout": "", "stderr": "timed out"}
    except FileNotFoundError as e:
        return {"ok": False, "stdout": "", "stderr": str(e)}
    except Exception as e:
        return {"ok": False, "stdout": "", "stderr": str(e)}


# ── Cloudflare Tunnel ─────────────────────────────────────────────────────────

@router.get("/cloudflare/status")
def cloudflare_status():
    r = run_cmd(["systemctl", "is-active", "cloudflared"])
    active = r["stdout"] == "active"
    r2 = run_cmd(["pgrep", "-f", "cloudflared"])
    cfg = db_db.get_all_integration_config()
    return {
        "active": active,
        "process_running": r2["ok"] and bool(r2["stdout"]),
        "public_hostname": cfg.get("cf_public_hostname", ""),
        "configured": bool(cfg.get("cf_token")),
    }


@router.get("/cloudflare/config")
def get_cloudflare_config():
    cfg = db_db.get_all_integration_config()
    return {
        "public_hostname": cfg.get("cf_public_hostname", ""),
        "cf_access_client_id": cfg.get("cf_access_client_id", ""),
        # Never return secrets
        "token_set": bool(cfg.get("cf_token")),
        "secret_set": bool(cfg.get("cf_access_secret")),
    }


@router.post("/cloudflare/config")
def save_cloudflare_config(body: dict):
    if body.get("token"):
        db_db.set_integration_config("cf_token", body["token"])
    if body.get("public_hostname"):
        db_db.set_integration_config("cf_public_hostname", body["public_hostname"])
        db_db.set_integration_config("public_url", f"https://{body['public_hostname']}")
    if body.get("cf_access_client_id"):
        db_db.set_integration_config("cf_access_client_id", body["cf_access_client_id"])
    if body.get("cf_access_secret"):
        db_db.set_integration_config("cf_access_secret", body["cf_access_secret"])
    return {"ok": True, "message": "Cloudflare config saved"}


@router.post("/cloudflare/start")
def cloudflare_start():
    token = db_db.get_integration_config("cf_token")
    if not token:
        return {"ok": False, "error": "No Cloudflare tunnel token configured"}
    # Try systemctl first
    r = run_cmd(["systemctl", "start", "cloudflared"])
    if r["ok"]:
        db_db.log_audit("cloudflare_start", result="ok")
        return {"ok": True, "message": "cloudflared started via systemctl"}
    # Try running directly (write a simple systemd unit or use subprocess)
    r2 = run_cmd(["which", "cloudflared"])
    if not r2["ok"]:
        return {"ok": False, "error": "cloudflared not installed. Run: curl -L --output cloudflared.deb https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb && sudo dpkg -i cloudflared.deb"}
    # Launch as background process
    try:
        subprocess.Popen(
            ["cloudflared", "tunnel", "run", "--token", token],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        db_db.log_audit("cloudflare_start", result="ok", details={"method": "direct"})
        return {"ok": True, "message": "cloudflared tunnel started"}
    except Exception as e:
        db_db.log_audit("cloudflare_start", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e)}


@router.post("/cloudflare/stop")
def cloudflare_stop():
    r = run_cmd(["systemctl", "stop", "cloudflared"])
    if not r["ok"]:
        run_cmd(["pkill", "-f", "cloudflared"])
    db_db.log_audit("cloudflare_stop", result="ok")
    return {"ok": True, "message": "cloudflared stopped"}


# ── Tailscale ─────────────────────────────────────────────────────────────────

@router.get("/tailscale/status")
def tailscale_status():
    r = run_cmd(["tailscale", "status", "--json"], timeout=8)
    r_ip = run_cmd(["tailscale", "ip", "-4"], timeout=8)
    installed = run_cmd(["which", "tailscale"])["ok"]
    ip = r_ip["stdout"] if r_ip["ok"] else None
    connected = False
    if r["ok"] and r["stdout"]:
        try:
            data = json.loads(r["stdout"])
            connected = data.get("BackendState") == "Running"
        except Exception:
            pass
    return {
        "installed": installed,
        "connected": connected,
        "tailscale_ip": ip,
    }


@router.get("/tailscale/config")
def get_tailscale_config():
    return {
        "auth_key_set": bool(db_db.get_integration_config("ts_auth_key")),
    }


@router.post("/tailscale/config")
def save_tailscale_config(body: dict):
    if body.get("auth_key"):
        db_db.set_integration_config("ts_auth_key", body["auth_key"])
    return {"ok": True, "message": "Tailscale config saved"}


@router.post("/tailscale/up")
def tailscale_up():
    auth_key = db_db.get_integration_config("ts_auth_key")
    if not auth_key:
        return {"ok": False, "error": "No Tailscale auth key configured"}
    r = run_cmd(["tailscale", "up", f"--authkey={auth_key}", "--accept-routes"], timeout=30)
    if r["ok"]:
        ip_r = run_cmd(["tailscale", "ip", "-4"])
        ip = ip_r["stdout"] if ip_r["ok"] else "unknown"
        db_db.set_integration_config("ts_ip", ip)
        db_db.set_integration_config("public_url", f"http://{ip}:8080")
        db_db.log_audit("tailscale_up", result="ok", details={"ip": ip})
        return {"ok": True, "message": f"Tailscale connected. IP: {ip}", "tailscale_ip": ip}
    db_db.log_audit("tailscale_up", result="error", details={"error": r["stderr"]})
    return {"ok": False, "error": r["stderr"] or "tailscale up failed"}


@router.post("/tailscale/down")
def tailscale_down():
    r = run_cmd(["tailscale", "down"])
    db_db.log_audit("tailscale_down", result="ok" if r["ok"] else "error")
    return {"ok": r["ok"], "message": "Tailscale disconnected" if r["ok"] else r["stderr"]}


# ── Webhook ───────────────────────────────────────────────────────────────────

@router.get("/webhook/config")
def get_webhook_config():
    cfg = db_db.get_all_integration_config()
    return {
        "webhook_url": cfg.get("webhook_url", ""),
        "webhook_secret_set": bool(cfg.get("webhook_secret")),
        "webhook_events": json.loads(cfg.get("webhook_events", "[]")),
        "webhook_lag_threshold": int(cfg.get("webhook_lag_threshold", 300)),
        "webhook_score_threshold": int(cfg.get("webhook_score_threshold", 80)),
    }


@router.post("/webhook/config")
def save_webhook_config(body: dict):
    if "webhook_url" in body:
        db_db.set_integration_config("webhook_url", body["webhook_url"])
    if "secret" in body and body["secret"]:
        db_db.set_integration_config("webhook_secret", body["secret"])
    if "events" in body:
        db_db.set_integration_config("webhook_events", json.dumps(body["events"]))
    if "lag_threshold" in body:
        db_db.set_integration_config("webhook_lag_threshold", str(body["lag_threshold"]))
    if "score_threshold" in body:
        db_db.set_integration_config("webhook_score_threshold", str(body["score_threshold"]))
    return {"ok": True, "message": "Webhook config saved"}


@router.post("/webhook/test")
async def test_webhook():
    url = db_db.get_integration_config("webhook_url")
    secret = db_db.get_integration_config("webhook_secret")
    if not url:
        return {"ok": False, "error": "No webhook URL configured"}
    payload = {
        "event": "test",
        "ts": datetime.utcnow().isoformat() + "Z",
        "severity": "info",
        "message": "Moodle DR Dashboard webhook test",
        "data": {"test": True},
        "dashboard_url": db_db.get_integration_config("public_url") or "",
    }
    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-Moodle-DR-Secret"] = secret
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload, headers=headers)
        return {"ok": resp.status_code < 400, "status_code": resp.status_code,
                "message": f"Webhook responded with HTTP {resp.status_code}"}
    except ImportError:
        # Fallback without httpx
        return {"ok": False, "error": "httpx not installed — add to requirements.txt"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── Dispatch webhook (called from watchdog/health check logic) ─────────────────

async def dispatch_webhook(event_type: str, severity: str, message: str, data: dict):
    """Fire configured webhook if event_type is in enabled events list."""
    try:
        url = db_db.get_integration_config("webhook_url")
        secret = db_db.get_integration_config("webhook_secret")
        enabled_events_raw = db_db.get_integration_config("webhook_events") or "[]"
        enabled_events = json.loads(enabled_events_raw)

        if not url or event_type not in enabled_events:
            return

        payload = {
            "event": event_type,
            "ts": datetime.utcnow().isoformat() + "Z",
            "severity": severity,
            "message": message,
            "data": data,
            "dashboard_url": db_db.get_integration_config("public_url") or "",
        }
        headers = {"Content-Type": "application/json"}
        if secret:
            headers["X-Moodle-DR-Secret"] = secret

        try:
            import httpx
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json=payload, headers=headers)
        except Exception:
            pass  # Webhook failures must never crash callers
    except Exception:
        pass


# ── Notifications ─────────────────────────────────────────────────────────────

@router.get("/notifications/config")
def get_notifications_config():
    channels = db_db.get_notification_channels()
    result = {}
    for ch in channels:
        try:
            result[ch["channel"]] = json.loads(ch["config"] or "{}")
        except Exception:
            result[ch["channel"]] = {}
    return {"ok": True, "channels": result}


@router.post("/notifications/config")
def save_notifications_config(body: dict):
    channel = body.get("channel")
    config = body.get("config", {})
    if not channel:
        return {"ok": False, "error": "channel required"}
    db_db.save_notification_channel(channel, config)
    return {"ok": True, "message": f"{channel} config saved"}


@router.post("/notifications/test/{channel}")
async def test_notification(channel: str):
    channels = db_db.get_notification_channels()
    ch_data = next((c for c in channels if c["channel"] == channel), None)
    if not ch_data:
        return {"ok": False, "error": f"Channel '{channel}' not configured"}
    try:
        cfg = json.loads(ch_data["config"] or "{}")
    except Exception:
        cfg = {}

    test_msg = "Moodle DR Dashboard — test notification"

    try:
        import httpx
        if channel == "telegram":
            token = cfg.get("bot_token", "")
            chat_id = cfg.get("chat_id", "")
            if not token or not chat_id:
                return {"ok": False, "error": "Telegram: bot_token and chat_id required"}
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": test_msg},
                )
            return {"ok": r.status_code == 200, "status_code": r.status_code}

        elif channel == "slack":
            url = cfg.get("webhook_url", "")
            if not url:
                return {"ok": False, "error": "Slack: webhook_url required"}
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.post(url, json={"text": test_msg})
            return {"ok": r.status_code == 200, "status_code": r.status_code}

        elif channel == "discord":
            url = cfg.get("webhook_url", "")
            if not url:
                return {"ok": False, "error": "Discord: webhook_url required"}
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.post(url, json={"content": test_msg})
            return {"ok": r.status_code in (200, 204), "status_code": r.status_code}

        elif channel == "email":
            import smtplib
            from email.message import EmailMessage
            msg = EmailMessage()
            msg["Subject"] = "Moodle DR — Test Notification"
            msg["From"] = cfg.get("from_address", "")
            msg["To"] = cfg.get("to_address", "")
            msg.set_content(test_msg)
            with smtplib.SMTP(cfg.get("smtp_host", "localhost"), int(cfg.get("smtp_port", 587))) as s:
                if cfg.get("smtp_user"):
                    s.starttls()
                    s.login(cfg["smtp_user"], cfg.get("smtp_password", ""))
                s.send_message(msg)
            return {"ok": True, "message": "Email sent"}

        else:
            return {"ok": False, "error": f"Unknown channel: {channel}"}

    except ImportError:
        return {"ok": False, "error": "httpx not installed"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── OpenClaw API Tokens ───────────────────────────────────────────────────────

@router.get("/openclaw/tokens")
def get_openclaw_tokens():
    tokens = db_db.get_api_tokens()
    # Mask token hashes
    for t in tokens:
        t["token_hash"] = t["token_hash"][:8] + "..." if t.get("token_hash") else "—"
    return {"ok": True, "tokens": tokens}


@router.post("/openclaw/tokens")
def create_openclaw_token(body: dict = None):
    label = (body or {}).get("label", "OpenClaw token")
    raw_token = secrets.token_urlsafe(32)
    token_id = db_db.create_api_token(label, raw_token)
    db_db.log_audit("create_api_token", result="ok", details={"label": label})
    return {
        "ok": True,
        "token_id": token_id,
        "token": raw_token,  # Only returned once — user must copy this
        "label": label,
        "message": "Save this token — it will not be shown again",
    }


@router.delete("/openclaw/tokens/{token_id}")
def revoke_openclaw_token(token_id: int):
    db_db.revoke_api_token(token_id)
    db_db.log_audit("revoke_api_token", result="ok", details={"token_id": token_id})
    return {"ok": True, "message": f"Token {token_id} revoked"}


@router.get("/openclaw/skill-file")
def get_openclaw_skill_file():
    cfg = db_db.get_all_integration_config()
    dashboard_url = cfg.get("public_url", "http://YOUR-DASHBOARD-URL:8080")
    # Get first active token for display
    tokens = db_db.get_api_tokens()
    active_tokens = [t for t in tokens if not t.get("revoked")]
    api_token = active_tokens[0].get("token_hash", "YOUR-API-TOKEN")[:8] + "..." if active_tokens else "YOUR-API-TOKEN"
    lag_threshold = cfg.get("webhook_lag_threshold", "300")
    score_threshold = cfg.get("webhook_score_threshold", "80")
    return {
        "ok": True,
        "dashboard_url": dashboard_url,
        "api_token_preview": api_token,
        "lag_threshold": lag_threshold,
        "score_threshold": score_threshold,
    }


@router.get("/openclaw/skill-file/download", response_class=PlainTextResponse)
def download_openclaw_skill_file():
    cfg = db_db.get_all_integration_config()
    dashboard_url = cfg.get("public_url", "http://YOUR-DASHBOARD-URL:8080")
    lag_threshold = cfg.get("webhook_lag_threshold", "300")
    score_threshold = cfg.get("webhook_score_threshold", "80")

    content = f"""---
name: moodle-dr
description: Monitor Moodle DR sync status, database replication health, and DR readiness. Query live status or receive alerts.
version: 1.0.0
---

# Moodle DR Skill

## Configuration
Dashboard URL: {dashboard_url}
Note: Include your API token as the X-API-Token header in all requests.

## What you can ask

- "moodle dr status" — full system snapshot
- "is replication running" — db replication health
- "what is the rpo" — current RPO estimate
- "dr readiness score" — readiness percentage
- "how many files transferred today" — file sync stats
- "stop replication" — STOP REPLICA (requires confirmation)
- "start replication" — START REPLICA

## Instructions

When the user asks about Moodle DR status, call:
GET {dashboard_url}/api/public/status
Header: X-API-Token: {{your_api_token}}

Format the response naturally:
- DR Readiness: {{score}}% (healthy/warning/critical)
- File Sync: lsyncd {{running/stopped}}, last sync Ns ago
- DB Replication: {{status}}, lag Ns, pt-heartbeat Ns
- RPO: ~N seconds

For control commands (stop/start replication), call the respective POST endpoint and ask for confirmation before executing.
- Stop replication: POST {dashboard_url}/api/db/replication/stop
- Start replication: POST {dashboard_url}/api/db/replication/start

## Cron job for proactive alerts

Run every 5 minutes:
1. Call GET {dashboard_url}/api/public/status with X-API-Token header
2. If dr_readiness.score < {score_threshold}: send alert "⚠️ Moodle DR readiness dropped to {{score}}%"
3. If db_sync.lag_seconds > {lag_threshold}: send alert "⚠️ MySQL replication lag: {{lag}}s"
4. If db_sync.io_running == "OFF": send alert "🔴 MySQL IO thread stopped"
5. If db_sync.status == "error": send alert "🔴 MySQL replication error"
"""
    return PlainTextResponse(content=content, headers={
        "Content-Disposition": "attachment; filename=moodle-dr-skill.md"
    })
