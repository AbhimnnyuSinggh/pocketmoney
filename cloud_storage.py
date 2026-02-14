"""
cloud_storage.py — Persistent JSON storage using GitHub Gist (free).

Solves the Render ephemeral disk problem: every time Render restarts,
local files are wiped. This module syncs critical data (subscriptions,
preferences) to a private GitHub Gist so it survives restarts.

Setup (one-time):
  1. Go to https://github.com/settings/tokens → Generate new token (classic)
  2. Check ONLY the "gist" scope → Generate
  3. In Render dashboard → Environment → Add:
     GITHUB_GIST_TOKEN = ghp_your_token_here

That's it. On first run, the module auto-creates a private gist.
On every save, it uploads. On every load, it pulls from the gist.
"""
import os
import json
import logging
import time
import threading
import requests

logger = logging.getLogger("arb_bot.cloud_storage")

GITHUB_API = "https://api.github.com"
GIST_ID_FILE = ".gist_id"  # Local cache of the gist ID


class GistStorage:
    """
    Read/write JSON files to a private GitHub Gist.
    Thread-safe with write batching (saves at most once every N seconds).
    """

    def __init__(self, token: str | None = None, batch_interval: int = 10):
        self.token = token or os.environ.get("GITHUB_GIST_TOKEN", "")
        self.enabled = bool(self.token)
        self.gist_id = self._load_gist_id()
        self._lock = threading.Lock()
        self._pending_writes: dict[str, str] = {}  # filename -> json content
        self._batch_interval = batch_interval
        self._last_write = 0

        if not self.enabled:
            logger.warning(
                "GITHUB_GIST_TOKEN not set — cloud backup DISABLED. "
                "Subscription data will NOT survive Render restarts! "
                "Set it in Render → Environment to enable."
            )
        else:
            # Ensure gist exists
            if not self.gist_id:
                self.gist_id = self._create_gist()
            logger.info(
                f"Cloud storage enabled (Gist ID: {self.gist_id[:8]}...)"
            )

            # Start background writer thread
            self._writer_thread = threading.Thread(
                target=self._background_writer, daemon=True
            )
            self._writer_thread.start()

    # -----------------------------------------------------------------
    # Gist ID management
    # -----------------------------------------------------------------

    def _load_gist_id(self) -> str:
        """Load gist ID from local file or env var."""
        # Env var takes priority (survives disk wipes)
        gist_id = os.environ.get("GITHUB_GIST_ID", "")
        if gist_id:
            return gist_id

        # Fall back to local file
        if os.path.exists(GIST_ID_FILE):
            try:
                with open(GIST_ID_FILE, "r") as f:
                    return f.read().strip()
            except IOError:
                pass
        return ""

    def _save_gist_id(self, gist_id: str):
        """Save gist ID locally."""
        try:
            with open(GIST_ID_FILE, "w") as f:
                f.write(gist_id)
        except IOError:
            pass

    # -----------------------------------------------------------------
    # Gist CRUD
    # -----------------------------------------------------------------

    def _headers(self) -> dict:
        return {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def _create_gist(self) -> str:
        """Create a new private gist to store bot data."""
        try:
            resp = requests.post(
                f"{GITHUB_API}/gists",
                headers=self._headers(),
                json={
                    "description": "PolyQuick Bot Data (auto-managed)",
                    "public": False,
                    "files": {
                        "user_subs.json": {
                            "content": json.dumps({"subs": {}, "created": time.time()})
                        },
                        "user_prefs.json": {
                            "content": json.dumps({"users": {}, "created": time.time()})
                        },
                    },
                },
                timeout=15,
            )
            resp.raise_for_status()
            gist_id = resp.json()["id"]
            self._save_gist_id(gist_id)
            logger.info(f"Created new cloud storage gist: {gist_id}")
            return gist_id
        except Exception as e:
            logger.error(f"Failed to create gist: {e}")
            return ""

    def save(self, filename: str, data: dict):
        """
        Queue a file for cloud save. Writes are batched to avoid
        hitting GitHub rate limits (5000/hour).
        """
        if not self.enabled or not self.gist_id:
            return

        content = json.dumps(data, indent=2)
        with self._lock:
            self._pending_writes[filename] = content

    def _flush_writes(self):
        """Actually write all pending files to the gist."""
        with self._lock:
            if not self._pending_writes:
                return
            pending = dict(self._pending_writes)
            self._pending_writes.clear()

        files = {}
        for filename, content in pending.items():
            files[filename] = {"content": content}

        try:
            resp = requests.patch(
                f"{GITHUB_API}/gists/{self.gist_id}",
                headers=self._headers(),
                json={"files": files},
                timeout=15,
            )
            if resp.status_code == 404:
                # Gist was deleted — recreate
                logger.warning("Gist not found — recreating...")
                self.gist_id = self._create_gist()
                if self.gist_id:
                    self._flush_writes()  # Retry
                return

            resp.raise_for_status()
            self._last_write = time.time()
            logger.debug(
                f"Cloud saved {len(files)} file(s): "
                f"{', '.join(files.keys())}"
            )
        except Exception as e:
            logger.warning(f"Cloud save failed: {e}")
            # Put writes back so they retry next cycle
            with self._lock:
                for k, v in pending.items():
                    if k not in self._pending_writes:
                        self._pending_writes[k] = v

    def load(self, filename: str) -> dict | None:
        """
        Load a file from the gist. Returns parsed JSON dict,
        or None if not found / not enabled.
        """
        if not self.enabled or not self.gist_id:
            return None

        try:
            resp = requests.get(
                f"{GITHUB_API}/gists/{self.gist_id}",
                headers=self._headers(),
                timeout=15,
            )
            resp.raise_for_status()
            gist_data = resp.json()

            file_info = gist_data.get("files", {}).get(filename)
            if not file_info:
                return None

            content = file_info.get("content", "{}")
            return json.loads(content)
        except Exception as e:
            logger.warning(f"Cloud load failed for {filename}: {e}")
            return None

    def _background_writer(self):
        """Background thread that flushes writes every N seconds."""
        while True:
            time.sleep(self._batch_interval)
            try:
                self._flush_writes()
            except Exception as e:
                logger.error(f"Background writer error: {e}")


# -----------------------------------------------------------------
# Module-level singleton
# -----------------------------------------------------------------
_storage: GistStorage | None = None


def get_cloud_storage() -> GistStorage:
    """Get or create the global cloud storage instance."""
    global _storage
    if _storage is None:
        _storage = GistStorage()
    return _storage
