"""
watchdog.py — Connectivity Watchdog.

Runs as a background asyncio task. Every N seconds it SSH-pings the target.
When it detects a reconnect after a dropout it:
  1. Logs the event (with downtime duration) to SQLite
  2. Calls state.trigger_rsync_fn('watchdog') to start a catch-up rsync
  3. After a successful catch-up rsync, marks partial file records as resolved

Imports only: state, transfer_db — no imports from main.py or transfers.py.
"""

import asyncio
import subprocess
from datetime import datetime
from typing import Optional

import state
import transfer_db


class ConnectivityWatchdog:
    def __init__(self, check_interval: int = 30):
        self.check_interval = check_interval
        self._task: Optional[asyncio.Task] = None
        self._running = False

        # Public state
        self.connected: Optional[bool] = None   # None = first check not done yet
        self.connected_since: Optional[str] = None
        self.last_drop_at: Optional[str] = None
        self.last_reconnect_at: Optional[str] = None
        self.last_downtime_secs: Optional[float] = None
        self._drop_started: Optional[str] = None
        self.total_drops = 0
        self.total_catchups_triggered = 0

    # ── Public state dict ─────────────────────────────────────────────────────

    @property
    def state_dict(self) -> dict:
        now = datetime.now()
        up_secs = None
        down_secs = None

        if self.connected and self.connected_since:
            try:
                up_secs = round(
                    (now - datetime.fromisoformat(self.connected_since)).total_seconds()
                )
            except Exception:
                pass

        if self.connected is False and self._drop_started:
            try:
                down_secs = round(
                    (now - datetime.fromisoformat(self._drop_started)).total_seconds()
                )
            except Exception:
                pass

        return {
            "running":               self._running,
            "connected":             self.connected,
            "connected_since":       self.connected_since,
            "last_drop_at":          self.last_drop_at,
            "last_reconnect_at":     self.last_reconnect_at,
            "last_downtime_secs":    self.last_downtime_secs,
            "current_uptime_secs":   up_secs,
            "current_downtime_secs": down_secs,
            "check_interval":        self.check_interval,
            "total_drops":           self.total_drops,
            "total_catchups":        self.total_catchups_triggered,
        }

    # ── SSH ping (blocking — run in thread pool) ──────────────────────────────

    def _ssh_ping(self) -> bool:
        if not state.AZURE_VM_IP:
            return False
        try:
            r = subprocess.run(
                [
                    "ssh",
                    "-i", state.SSH_KEY_PATH,
                    "-o", "StrictHostKeyChecking=accept-new",
                    "-o", f"ConnectTimeout={min(self.check_interval // 3, 10)}",
                    "-o", "BatchMode=yes",
                    "-o", "ServerAliveInterval=5",
                    "-o", "ServerAliveCountMax=1",
                    f"{state.AZURE_VM_USER}@{state.AZURE_VM_IP}",
                    "echo __ok__",
                ],
                capture_output=True,
                text=True,
                timeout=15,
            )
            return r.returncode == 0 and "__ok__" in r.stdout
        except Exception:
            return False

    # ── Single check cycle ────────────────────────────────────────────────────

    async def _check(self):
        loop = asyncio.get_event_loop()
        is_up = await loop.run_in_executor(None, self._ssh_ping)
        now   = datetime.now().isoformat()
        tip   = state.AZURE_VM_IP

        was_connected = self.connected

        if is_up:
            if was_connected is False:
                # ── Reconnect after dropout ───────────────────────────────────
                downtime = 0.0
                if self._drop_started:
                    try:
                        downtime = round(
                            (datetime.now() - datetime.fromisoformat(self._drop_started)
                             ).total_seconds(), 1
                        )
                    except Exception:
                        pass

                self.last_downtime_secs = downtime
                self.last_reconnect_at  = now
                self.connected          = True
                self.connected_since    = now

                await transfer_db.log_connectivity(
                    "reconnected",
                    tip,
                    downtime_secs=downtime,
                    triggered_catchup=True,
                    notes=f"Down for {downtime:.0f}s. Triggering catch-up rsync.",
                )

                # Trigger catch-up
                if state.trigger_rsync_fn:
                    try:
                        asyncio.ensure_future(state.trigger_rsync_fn("watchdog"))
                        self.total_catchups_triggered += 1
                    except Exception:
                        pass

            elif was_connected is None:
                # ── First successful check ────────────────────────────────────
                self.connected       = True
                self.connected_since = now
                await transfer_db.log_connectivity(
                    "connected", tip, notes="Watchdog initialised — target reachable"
                )

            # else: still connected — no event needed

        else:
            if was_connected is True:
                # ── Connection just dropped ───────────────────────────────────
                self._drop_started = now
                self.last_drop_at  = now
                self.connected     = False
                self.total_drops  += 1
                await transfer_db.log_connectivity(
                    "disconnected", tip, notes="SSH ping failed"
                )

            elif was_connected is None:
                # ── First check: target unreachable ───────────────────────────
                self.connected     = False
                self._drop_started = now
                await transfer_db.log_connectivity(
                    "unreachable", tip,
                    notes="Watchdog started but target is not reachable"
                )

            # else: still disconnected — we just wait

    # ── Background loop ───────────────────────────────────────────────────────

    async def _loop(self):
        while self._running:
            try:
                await self._check()
            except Exception:
                pass  # Never crash the watchdog
            # Sleep in small increments so stop() is responsive
            for _ in range(self.check_interval * 2):
                if not self._running:
                    break
                await asyncio.sleep(0.5)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> bool:
        if self._task and not self._task.done():
            return False  # already running
        self._running = True
        self._task = asyncio.ensure_future(self._loop())
        return True

    def stop(self) -> bool:
        self._running = False
        if self._task:
            self._task.cancel()
        return True
