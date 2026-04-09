"""Public status API — no auth required. For external integrations."""
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse, StreamingResponse
import state
import json
import asyncio
from datetime import datetime

router = APIRouter(tags=["public"])


@router.get("/api/public/status")
def public_status():
    """No-auth endpoint. Returns DR snapshot. Rate limited by Nginx."""
    lsyncd_ok = True
    try:
        import subprocess
        r = subprocess.run(["systemctl", "is-active", "lsyncd"], capture_output=True, text=True, timeout=5)
        lsyncd_ok = r.stdout.strip() == "active"
    except Exception:
        pass

    return {
        "ts": datetime.utcnow().isoformat(),
        "file_sync": {
            "lsyncd_running": lsyncd_ok,
            "rsync_running": state.rsync_job.get("running", False),
            "last_result": state.rsync_job.get("last_result"),
        },
        "db_sync": {
            "status": state.db_replication_status.get("status", "unknown"),
            "lag_seconds": state.db_replication_status.get("lag_seconds"),
            "heartbeat_lag": state.db_replication_status.get("heartbeat_lag"),
            "io_running": state.db_replication_status.get("io_running"),
            "sql_running": state.db_replication_status.get("sql_running"),
        },
        "dr_readiness": {
            "score": _calc_dr_score(),
            "rpo_seconds": _calc_rpo(),
        }
    }


@router.get("/api/public/stream")
async def public_stream():
    """SSE stream of live status updates every 30s."""
    async def gen():
        while True:
            data = public_status()
            yield f"data: {json.dumps(data)}\n\n"
            await asyncio.sleep(30)
    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/metrics", response_class=PlainTextResponse)
def prometheus_metrics():
    """Prometheus text format metrics endpoint."""
    lag = state.db_replication_status.get("lag_seconds") or 0
    score = _calc_dr_score()
    rpo = _calc_rpo()
    lines = [
        "# HELP moodle_dr_db_lag_seconds MySQL replication lag in seconds",
        "# TYPE moodle_dr_db_lag_seconds gauge",
        f"moodle_dr_db_lag_seconds {lag}",
        "# HELP moodle_dr_readiness_score DR readiness score 0-100",
        "# TYPE moodle_dr_readiness_score gauge",
        f"moodle_dr_readiness_score {score}",
        "# HELP moodle_dr_rpo_seconds Estimated RPO in seconds",
        "# TYPE moodle_dr_rpo_seconds gauge",
        f"moodle_dr_rpo_seconds {rpo}",
        "# HELP moodle_dr_rsync_running Whether rsync is currently running",
        "# TYPE moodle_dr_rsync_running gauge",
        f"moodle_dr_rsync_running {1 if state.rsync_job.get('running') else 0}",
        "# HELP moodle_dr_file_setup_complete File sync setup complete",
        "# TYPE moodle_dr_file_setup_complete gauge",
        f"moodle_dr_file_setup_complete {1 if state.file_setup_complete else 0}",
        "# HELP moodle_dr_db_setup_complete DB replication setup complete",
        "# TYPE moodle_dr_db_setup_complete gauge",
        f"moodle_dr_db_setup_complete {1 if state.db_setup_complete else 0}",
    ]
    return "\n".join(lines) + "\n"


@router.post("/api/public/webhook/test")
def test_webhook():
    return {"ok": True, "message": "Webhook test endpoint — no webhook configured"}


def _calc_dr_score() -> int:
    score = 100
    db = state.db_replication_status
    if db.get("status") == "error":
        score -= 40
    elif db.get("status") == "stopped":
        score -= 30
    elif db.get("status") == "lagging":
        score -= 15
    elif db.get("status") == "warning":
        score -= 5
    if db.get("last_io_error"):
        score -= 10
    if not state.file_setup_complete:
        score -= 10
    if not state.db_setup_complete:
        score -= 10
    return max(0, score)


def _calc_rpo() -> int:
    lag = state.db_replication_status.get("lag_seconds") or 0
    return max(0, lag)
