"""
23 & Claude — lightweight genomic analysis dashboard.
Upload a VCF, pick tests from the checklist, run them sequentially.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import shutil
import subprocess
import time
import traceback
import urllib.parse
import urllib.request
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from threading import Thread, Lock

from fastapi import FastAPI, File, UploadFile, Form, Request, HTTPException, Depends, Cookie
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse, RedirectResponse
import uvicorn

from test_registry import TESTS, TESTS_BY_ID, CATEGORIES
from runners import run_test

logger = logging.getLogger("simple-genomics")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

# ── Config ────────────────────────────────────────────────────────
PORT = int(os.getenv("SIMPLE_GENOMICS_PORT", "8800"))
SG_DATA_ROOT = Path(os.getenv(
    "SIMPLE_GENOMICS_DATA_ROOT",
    "/home/nimrod_rotem/simple-genomics",
))
USERS_DIR = SG_DATA_ROOT / "users"
USERS_FILE = SG_DATA_ROOT / "users.json"
SESSIONS_FILE = SG_DATA_ROOT / "sessions.json"
USERS_DIR.mkdir(parents=True, exist_ok=True)

# Legacy single-namespace paths kept ONLY for the one-time migration to
# the elisabeth user; runtime code never reads from these directly.
LEGACY_FILES_STATE = SG_DATA_ROOT / "files.json"
LEGACY_REPORTS_DIR = SG_DATA_ROOT / "reports"
LEGACY_UPLOAD_DIR  = SG_DATA_ROOT / "uploads"
LEGACY_CUSTOM_PGS  = SG_DATA_ROOT / "custom_pgs.json"
LEGACY_ERRORS_LOG  = SG_DATA_ROOT / "errors.log"
LEGACY_CHAT_MSGS   = SG_DATA_ROOT / "chat_messages.json"

# Number of concurrent test workers. The 44-core box can comfortably
# handle 4 workers in parallel — each plink2/bcftools sub-process inside
# a worker uses ~4 threads, so 4×4=16 cores during scoring, plus a
# transient ~16 cores during the one-time pgen build.
NUM_WORKERS = int(os.getenv("SIMPLE_GENOMICS_WORKERS", "4"))

PGS_CATALOG_API = "https://www.pgscatalog.org/rest"

DEFAULT_USER_USERNAME = os.getenv("DEFAULT_USER_USERNAME", "admin@example.com")
DEFAULT_USER_PASSWORD = os.getenv("DEFAULT_USER_PASSWORD", "changeme123456")

SESSION_COOKIE = "sg_session"
SESSION_TTL_SECONDS = 60 * 60 * 24 * 30  # 30 days


# ── Auth: users.json + sessions.json + cookie helpers ────────────
users_lock = Lock()
users_state = {}   # {username_lc: {pwd_hash, salt, created_at}}
sessions_lock = Lock()
sessions = {}      # {session_id: {username, expires_at}}


def _norm_username(u):
    return (u or "").strip().lower()


def _hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    pwd_hash = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("ascii"), 200_000
    ).hex()
    return salt, pwd_hash


def _verify_password(password, salt, expected_hash):
    _, candidate = _hash_password(password, salt)
    return hmac.compare_digest(candidate, expected_hash)


def _load_users():
    global users_state
    if USERS_FILE.exists():
        try:
            with open(USERS_FILE) as f:
                users_state = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load users.json: {e}")
            users_state = {}


def _save_users():
    """Caller must hold users_lock."""
    try:
        tmp = USERS_FILE.with_suffix(".json.tmp")
        with open(tmp, "w") as f:
            json.dump(users_state, f, indent=2)
        tmp.replace(USERS_FILE)
    except Exception as e:
        logger.error(f"Failed to save users.json: {e}")


def _load_sessions():
    global sessions
    if SESSIONS_FILE.exists():
        try:
            with open(SESSIONS_FILE) as f:
                sessions = json.load(f)
            # Drop expired
            now = time.time()
            sessions = {sid: s for sid, s in sessions.items()
                        if s.get("expires_at", 0) > now}
        except Exception as e:
            logger.error(f"Failed to load sessions.json: {e}")
            sessions = {}


def _save_sessions():
    """Caller must hold sessions_lock."""
    try:
        tmp = SESSIONS_FILE.with_suffix(".json.tmp")
        with open(tmp, "w") as f:
            json.dump(sessions, f, indent=2)
        tmp.replace(SESSIONS_FILE)
    except Exception as e:
        logger.error(f"Failed to save sessions.json: {e}")


def _create_user(username, password):
    """Add a new user. Returns (ok, error). Idempotent fail on duplicate."""
    u = _norm_username(username)
    if not u or "@" not in u or len(password) < 6:
        return False, "Username must be an email and password must be at least 6 characters"
    with users_lock:
        if u in users_state:
            return False, "User already exists"
        salt, pwd_hash = _hash_password(password)
        users_state[u] = {
            "username": u,
            "salt": salt,
            "pwd_hash": pwd_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _save_users()
    # Eagerly create the user's directory tree
    user_dir(u)
    return True, None


def _authenticate(username, password):
    u = _norm_username(username)
    with users_lock:
        rec = users_state.get(u)
    if not rec:
        return False
    return _verify_password(password, rec["salt"], rec["pwd_hash"])


def _create_session(username):
    sid = secrets.token_urlsafe(32)
    with sessions_lock:
        sessions[sid] = {
            "username": _norm_username(username),
            "expires_at": time.time() + SESSION_TTL_SECONDS,
        }
        _save_sessions()
    return sid


def _resolve_session(sid):
    if not sid:
        return None
    with sessions_lock:
        s = sessions.get(sid)
        if not s:
            return None
        if s.get("expires_at", 0) < time.time():
            sessions.pop(sid, None)
            _save_sessions()
            return None
        return s["username"]


def _drop_session(sid):
    with sessions_lock:
        if sid in sessions:
            sessions.pop(sid, None)
            _save_sessions()


def current_user(request: Request) -> str:
    """FastAPI dependency: extract username from session cookie or 401."""
    sid = request.cookies.get(SESSION_COOKIE)
    username = _resolve_session(sid)
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return username


def current_user_optional(request: Request) -> str | None:
    """Same as current_user but returns None instead of 401."""
    sid = request.cookies.get(SESSION_COOKIE)
    return _resolve_session(sid)


# ── Per-user storage paths ───────────────────────────────────────
def _user_hash(username):
    return hashlib.sha1(_norm_username(username).encode("utf-8")).hexdigest()[:16]


def user_dir(username):
    d = USERS_DIR / _user_hash(username)
    d.mkdir(parents=True, exist_ok=True)
    return d


def user_files_path(username):
    return user_dir(username) / "files.json"


def user_reports_root(username):
    d = user_dir(username) / "reports"
    d.mkdir(parents=True, exist_ok=True)
    return d


def user_uploads_dir(username):
    d = user_dir(username) / "uploads"
    d.mkdir(parents=True, exist_ok=True)
    return d


def user_custom_pgs_path(username):
    return user_dir(username) / "custom_pgs.json"


def user_errors_log(username):
    return user_dir(username) / "errors.log"


def _user_report_dir(username, file_id):
    d = user_reports_root(username) / file_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── Per-user in-memory state ─────────────────────────────────────
class UserState:
    """Bundle of per-user mutable state, lazy-loaded from disk on first
    access. The frontend's "files registry", "active file" pointer, and
    "custom PGS list" all live here. The global task queue tags every
    task with `username` so workers can route results back to the right
    UserState's report dir on disk."""

    def __init__(self, username):
        self.username = _norm_username(username)
        self.lock = Lock()
        self.files_state = {"files": {}, "active_file_id": None}
        self.custom_pgs_list = []
        self._load()

    def _load(self):
        fp = user_files_path(self.username)
        if fp.exists():
            try:
                self.files_state = json.loads(fp.read_text())
                self.files_state.setdefault("files", {})
                self.files_state.setdefault("active_file_id", None)
            except Exception as e:
                logger.error(f"Failed to load files for {self.username}: {e}")
        cp = user_custom_pgs_path(self.username)
        if cp.exists():
            try:
                data = json.loads(cp.read_text())
                self.custom_pgs_list = data.get("pgs", []) or []
            except Exception as e:
                logger.error(f"Failed to load custom PGS for {self.username}: {e}")

    def save_files(self):
        """Caller must hold self.lock."""
        try:
            fp = user_files_path(self.username)
            tmp = fp.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self.files_state, indent=2))
            tmp.replace(fp)
        except Exception as e:
            logger.error(f"Failed to save files for {self.username}: {e}")

    def save_custom_pgs(self):
        """Caller must hold self.lock."""
        try:
            cp = user_custom_pgs_path(self.username)
            tmp = cp.with_suffix(".json.tmp")
            tmp.write_text(json.dumps({"pgs": self.custom_pgs_list}, indent=2))
            tmp.replace(cp)
        except Exception as e:
            logger.error(f"Failed to save custom PGS for {self.username}: {e}")


user_states_lock = Lock()
user_states = {}  # username_lc → UserState


def get_user_state(username) -> "UserState":
    u = _norm_username(username)
    with user_states_lock:
        if u not in user_states:
            user_states[u] = UserState(u)
        return user_states[u]


# ── File registry ops (per-user) ─────────────────────────────────
def _make_file_id(path):
    return hashlib.sha1(str(path).encode()).hexdigest()[:12]


def _register_file(username, path, source, name=None, select=True):
    """Add a file to a user's registry. Returns the entry dict."""
    ctx = get_user_state(username)
    path_str = str(path)
    fid = _make_file_id(path_str)
    with ctx.lock:
        if fid not in ctx.files_state["files"]:
            try:
                size = os.path.getsize(path_str) if os.path.exists(path_str) else 0
            except OSError:
                size = 0
            entry = {
                "id": fid,
                "name": name or Path(path_str).name,
                "path": path_str,
                "source": source,
                "added_at": datetime.now(timezone.utc).isoformat(),
                "size": size,
            }
            ctx.files_state["files"][fid] = entry
        else:
            entry = ctx.files_state["files"][fid]
        if select or ctx.files_state["active_file_id"] is None:
            ctx.files_state["active_file_id"] = fid
        ctx.save_files()
    return entry


def _delete_file(username, file_id):
    """Remove file from a user's registry. If it lived in this user's
    uploads dir, delete the bytes too. Wipes the user's report dir for
    this file and any in-memory task_results owned by this user."""
    ctx = get_user_state(username)
    with ctx.lock:
        entry = ctx.files_state["files"].pop(file_id, None)
        if entry is None:
            return None
        if ctx.files_state["active_file_id"] == file_id:
            remaining = list(ctx.files_state["files"].keys())
            ctx.files_state["active_file_id"] = remaining[0] if remaining else None
        ctx.save_files()

    # Delete the underlying upload bytes if they live in this user's
    # uploads dir (don't touch /data/vcfs/ paths the user only "linked").
    try:
        path = Path(entry["path"])
        ud = user_uploads_dir(username).resolve()
        if path.resolve().is_relative_to(ud) and path.exists():
            path.unlink()
    except (OSError, ValueError):
        pass

    # Per-user reports for this file
    file_reports = user_reports_root(username) / file_id
    if file_reports.exists():
        try:
            shutil.rmtree(file_reports)
        except OSError:
            pass

    # Drop in-memory task_results that belonged to this file (and this user)
    with queue_lock:
        stale = [
            tid for tid, res in list(task_results.items())
            if res.get("file_id") == file_id and res.get("username") == _norm_username(username)
        ]
        for tid in stale:
            task_results.pop(tid, None)

    return entry


def _clear_file_results(username, file_id):
    file_reports = user_reports_root(username) / file_id
    removed = 0
    if file_reports.exists():
        for p in file_reports.glob("*.json"):
            try:
                p.unlink()
                removed += 1
            except OSError:
                pass
    with queue_lock:
        stale = [
            tid for tid, res in list(task_results.items())
            if res.get("file_id") == file_id and res.get("username") == _norm_username(username)
        ]
        for tid in stale:
            task_results.pop(tid, None)
    return removed


def _get_active_file(username):
    ctx = get_user_state(username)
    with ctx.lock:
        fid = ctx.files_state.get("active_file_id")
        if fid and fid in ctx.files_state["files"]:
            return dict(ctx.files_state["files"][fid])
        return None


def log_error(username, task_id, test_id, test_name, error, result=None):
    """Append a failure record to the user's errors.log."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "task_id": task_id,
        "test_id": test_id,
        "test_name": test_name,
        "error": error,
    }
    if result:
        entry["result"] = result
    try:
        path = user_errors_log(username)
        with open(path, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        logger.error(f"Could not write to errors.log for {username}: {e}")

# ── Queue & State ─────────────────────────────────────────────────
# The queue is global (one worker pool serves every user). Each task
# carries its owning username so the worker writes results to the
# correct per-user reports directory.
queue_lock = Lock()
task_queue = deque()
task_results = {}     # task_id -> result dict (carries 'username' + 'file_id')
running_tasks = set() # task_ids currently being executed by any worker

app = FastAPI(title="23 & Claude")


# ── Queue Worker ──────────────────────────────────────────────────
def queue_worker(worker_id):
    """Background thread that pulls tasks off the shared global queue
    and runs them. Multiple workers run in parallel; per-user isolation
    is maintained by tagging each task with `username` and routing the
    resulting report to that user's dir on disk."""
    while True:
        task = None
        with queue_lock:
            if task_queue:
                task = task_queue.popleft()

        if task is None:
            time.sleep(1)
            continue

        task_id = task["id"]
        test_id = task["test_id"]
        vcf_path = task["vcf_path"]
        file_id = task.get("file_id", "_unknown")
        username = task.get("username") or DEFAULT_USER_USERNAME
        test_def = TESTS_BY_ID.get(test_id)

        if not test_def:
            task_results[task_id] = {
                "status": "error",
                "error": f"Unknown test: {test_id}",
                "file_id": file_id,
                "username": username,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
            continue

        # Mark as running
        with queue_lock:
            running_tasks.add(task_id)
        task_results[task_id] = {
            "status": "running",
            "test_id": test_id,
            "test_name": test_def["name"],
            "file_id": file_id,
            "username": username,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            logger.info(f"Running [{username}]: {test_def['name']} ({test_id})")
            start = time.time()
            result = run_test(vcf_path, test_def)
            elapsed = time.time() - start

            # Save report under the user's per-file reports dir
            report = {
                "task_id": task_id,
                "test_id": test_id,
                "test_name": test_def["name"],
                "category": test_def["category"],
                "description": test_def["description"],
                "vcf_path": vcf_path,
                "file_id": file_id,
                "username": username,
                "result": result,
                "elapsed_seconds": round(elapsed, 1),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }

            report_path = _user_report_dir(username, file_id) / f"{task_id}.json"
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)

            runner_status = result.get("status", "passed")
            headline = result.get("headline", "")
            error_msg = result.get("error")

            if runner_status == "failed":
                task_outcome = "failed"
                log_error(username, task_id, test_id, test_def["name"],
                          error_msg or "Unknown error", result)
            elif runner_status == "warning":
                task_outcome = "warning"
                if error_msg:
                    log_error(username, task_id, test_id, test_def["name"],
                              f"[warning] {error_msg}", result)
            else:
                task_outcome = "passed"

            task_results[task_id] = {
                "status": task_outcome,
                "test_id": test_id,
                "test_name": test_def["name"],
                "file_id": file_id,
                "username": username,
                "headline": headline,
                "error": error_msg,
                "elapsed": round(elapsed, 1),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "report_path": str(report_path),
            }
            logger.info(f"{task_outcome.upper()} [{username}]: {test_def['name']} — {headline} ({elapsed:.1f}s)")

        except Exception as e:
            logger.error(f"Test {test_id} crashed: {e}", exc_info=True)
            err = f"{type(e).__name__}: {e}"
            task_results[task_id] = {
                "status": "failed",
                "test_id": test_id,
                "test_name": test_def["name"],
                "file_id": file_id,
                "username": username,
                "headline": f"Crashed: {err[:80]}",
                "error": err,
                "traceback": traceback.format_exc(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
            log_error(username, task_id, test_id, test_def["name"], err)
        finally:
            with queue_lock:
                running_tasks.discard(task_id)


# ── First-run migration ───────────────────────────────────────────
def _migrate_legacy_to_default_user():
    """One-time on-disk migration. If users.json doesn't exist yet, this
    is a fresh upgrade from the single-namespace layout. Create the
    default elisabeth user, then move every legacy state file into her
    per-user dir.

    Safe to call multiple times — it bails out the moment users.json
    exists, which it does after the first successful run.
    """
    if USERS_FILE.exists():
        return

    logger.info(f"First-run auth migration: creating default user {DEFAULT_USER_USERNAME}")
    ok, err = _create_user(DEFAULT_USER_USERNAME, DEFAULT_USER_PASSWORD)
    if not ok and err != "User already exists":
        logger.error(f"Failed to create default user: {err}")
        return

    udir = user_dir(DEFAULT_USER_USERNAME)

    # files.json
    if LEGACY_FILES_STATE.exists():
        try:
            shutil.move(str(LEGACY_FILES_STATE), str(udir / "files.json"))
            logger.info("Migrated files.json")
        except OSError as e:
            logger.warning(f"files.json migration failed: {e}")

    # custom_pgs.json
    if LEGACY_CUSTOM_PGS.exists():
        try:
            shutil.move(str(LEGACY_CUSTOM_PGS), str(udir / "custom_pgs.json"))
            logger.info("Migrated custom_pgs.json")
        except OSError as e:
            logger.warning(f"custom_pgs.json migration failed: {e}")

    # errors.log
    if LEGACY_ERRORS_LOG.exists():
        try:
            shutil.move(str(LEGACY_ERRORS_LOG), str(udir / "errors.log"))
            logger.info("Migrated errors.log")
        except OSError as e:
            logger.warning(f"errors.log migration failed: {e}")

    # chat_messages.json (consumed by chat.py)
    if LEGACY_CHAT_MSGS.exists():
        try:
            shutil.move(str(LEGACY_CHAT_MSGS), str(udir / "chat_messages.json"))
            logger.info("Migrated chat_messages.json")
        except OSError as e:
            logger.warning(f"chat_messages.json migration failed: {e}")

    # reports/  →  users/<hash>/reports/
    if LEGACY_REPORTS_DIR.exists():
        dst = udir / "reports"
        try:
            if dst.exists():
                # Merge each subdir
                for sub in LEGACY_REPORTS_DIR.iterdir():
                    if sub.is_dir():
                        target = dst / sub.name
                        target.mkdir(parents=True, exist_ok=True)
                        for p in sub.iterdir():
                            try:
                                p.rename(target / p.name)
                            except OSError:
                                pass
                shutil.rmtree(LEGACY_REPORTS_DIR, ignore_errors=True)
            else:
                shutil.move(str(LEGACY_REPORTS_DIR), str(dst))
            logger.info("Migrated reports/")
        except OSError as e:
            logger.warning(f"reports/ migration failed: {e}")

    # uploads/  →  users/<hash>/uploads/  AND rewrite paths in files.json
    if LEGACY_UPLOAD_DIR.exists():
        dst = udir / "uploads"
        try:
            if dst.exists():
                for p in LEGACY_UPLOAD_DIR.iterdir():
                    try:
                        p.rename(dst / p.name)
                    except OSError:
                        pass
                shutil.rmtree(LEGACY_UPLOAD_DIR, ignore_errors=True)
            else:
                shutil.move(str(LEGACY_UPLOAD_DIR), str(dst))
            logger.info("Migrated uploads/")
        except OSError as e:
            logger.warning(f"uploads/ migration failed: {e}")

        # Rewrite path entries in files.json that pointed at the
        # legacy uploads dir.
        files_path = udir / "files.json"
        if files_path.exists():
            try:
                fs = json.loads(files_path.read_text())
                changed = False
                old_prefix = str(LEGACY_UPLOAD_DIR)
                new_prefix = str(udir / "uploads")
                for fid, entry in fs.get("files", {}).items():
                    p = entry.get("path", "")
                    if p.startswith(old_prefix):
                        entry["path"] = p.replace(old_prefix, new_prefix, 1)
                        changed = True
                if changed:
                    files_path.write_text(json.dumps(fs, indent=2))
                    logger.info("Rewrote upload paths in files.json")
            except Exception as e:
                logger.warning(f"Failed to rewrite upload paths: {e}")


# Load auth state from disk and run the migration BEFORE we mount the
# chat router (which depends on per-user paths).
_load_users()
_load_sessions()
_migrate_legacy_to_default_user()
_load_users()       # re-read in case migration just created the file

# Mount chat after the migration so its first call sees per-user paths.
from chat import router as chat_router
app.include_router(chat_router, prefix="/api/chat", tags=["chat"])


# ── Custom PGS registry (per-user) ────────────────────────────────
def _add_custom_pgs_to_tests(pgs_info):
    """Inject a custom PGS into the in-memory TESTS list so it shows up
    alongside the built-ins in the dashboard. Idempotent — a duplicate
    pgs_id is a no-op. Note: TESTS is a *shared* registry across users
    (the test catalog itself is global), but each user's `custom_pgs.json`
    is independent and only loaded entries get injected."""
    pgs_id = pgs_info["pgs_id"]
    test_id = f"custom_{pgs_id.lower()}"
    if test_id in TESTS_BY_ID:
        return False
    test_def = {
        "id": test_id,
        "category": "PGS - Custom",
        "name": pgs_info.get("name") or f"{pgs_info.get('trait', pgs_id)} ({pgs_id})",
        "description": pgs_info.get("description", ""),
        "test_type": "pgs_score",
        "params": {"pgs_id": pgs_id, "trait": pgs_info.get("trait", pgs_id)},
    }
    TESTS.append(test_def)
    TESTS_BY_ID[test_id] = test_def
    if "PGS - Custom" not in CATEGORIES:
        CATEGORIES.append("PGS - Custom")
    return True


def _remove_custom_pgs_from_tests(pgs_id):
    test_id = f"custom_{pgs_id.lower()}"
    if test_id not in TESTS_BY_ID:
        return False
    TESTS_BY_ID.pop(test_id, None)
    # Filter in place so other module users keep seeing the same list.
    TESTS[:] = [t for t in TESTS if t["id"] != test_id]
    if not any(t["category"] == "PGS - Custom" for t in TESTS):
        if "PGS - Custom" in CATEGORIES:
            CATEGORIES.remove("PGS - Custom")
    return True


def _eager_inject_custom_pgs_for_user(username):
    """When a user is first touched (login or auth dep), inject their
    custom PGS into the global TESTS catalog so all the run/list endpoints
    see them. Idempotent — duplicates are no-ops."""
    ctx = get_user_state(username)
    with ctx.lock:
        for p in list(ctx.custom_pgs_list):
            _add_custom_pgs_to_tests(p)


def _pgs_catalog_get(path, params=None, timeout=15):
    """Sync GET against the PGS Catalog REST API. Returns parsed JSON or
    raises urllib.error.URLError. Kept sync (urllib) to match the rest of
    the codebase — low volume, no need to bring in httpx."""
    url = PGS_CATALOG_API + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "simple-genomics/1.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


# Inject the default user's custom PGS into TESTS at startup so they
# show up in the catalog immediately.
if DEFAULT_USER_USERNAME in users_state or any(USERS_DIR.iterdir()):
    try:
        _eager_inject_custom_pgs_for_user(DEFAULT_USER_USERNAME)
    except Exception as e:
        logger.warning(f"Could not pre-inject default user's custom PGS: {e}")


# Spin up a pool of worker threads. Each pulls from the same shared
# task_queue, so concurrency is automatically load-balanced — slow tests
# don't block fast ones.
worker_threads = []
for i in range(NUM_WORKERS):
    t = Thread(target=queue_worker, args=(i,), name=f"sg-worker-{i}", daemon=True)
    t.start()
    worker_threads.append(t)
logger.info(f"Started {NUM_WORKERS} queue workers")


# ── API Routes ────────────────────────────────────────────────────

@app.get("/api/files")
async def list_files(username: str = Depends(current_user)):
    """List the calling user's registered files + their active file id."""
    ctx = get_user_state(username)
    with ctx.lock:
        files = list(ctx.files_state["files"].values())
        active_id = ctx.files_state.get("active_file_id")
    files.sort(key=lambda f: f.get("added_at", ""), reverse=True)
    return {"files": files, "active_file_id": active_id}


@app.post("/api/files/upload")
async def upload_file(
    file: UploadFile = File(...),
    username: str = Depends(current_user),
):
    """Upload a VCF/gVCF and register it under the calling user."""
    filename = file.filename or "uploaded.vcf.gz"
    udir = user_uploads_dir(username)
    dest = udir / filename
    if dest.exists():
        stem = dest.name
        i = 1
        while (udir / f"{i}_{stem}").exists():
            i += 1
        dest = udir / f"{i}_{stem}"
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)
    entry = _register_file(username, dest, source="upload", name=filename)
    return {"ok": True, "file": entry}


@app.post("/api/files/add-path")
async def add_file_from_path(
    request: Request,
    username: str = Depends(current_user),
):
    """Register a file that already lives on the server at an absolute path."""
    data = await request.json()
    path = (data.get("path") or "").strip()
    if not path:
        return JSONResponse({"ok": False, "error": "No path provided"}, status_code=400)
    if not os.path.exists(path):
        return JSONResponse({"ok": False, "error": f"File not found: {path}"}, status_code=404)
    if not os.path.isfile(path):
        return JSONResponse({"ok": False, "error": f"Not a file: {path}"}, status_code=400)
    entry = _register_file(username, path, source="local_path")
    return {"ok": True, "file": entry}


@app.post("/api/files/add-url")
async def add_file_from_url(
    request: Request,
    username: str = Depends(current_user),
):
    """Download a file from a URL into the calling user's uploads dir
    and register it. Blocking — long downloads tie up the request."""
    data = await request.json()
    url = (data.get("url") or "").strip()
    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return JSONResponse({"ok": False, "error": "URL must start with http:// or https://"}, status_code=400)

    udir = user_uploads_dir(username)
    name = data.get("name") or Path(urllib.parse.urlparse(url).path).name or "downloaded.vcf.gz"
    dest = udir / name
    if dest.exists():
        stem = dest.name
        i = 1
        while (udir / f"{i}_{stem}").exists():
            i += 1
        dest = udir / f"{i}_{stem}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "simple-genomics/1.0"})
        with urllib.request.urlopen(req, timeout=600) as resp:
            with open(dest, "wb") as f:
                shutil.copyfileobj(resp, f, length=1024 * 1024)
    except Exception as e:
        try:
            if dest.exists():
                dest.unlink()
        except OSError:
            pass
        return JSONResponse({"ok": False, "error": f"Download failed: {e}"}, status_code=500)

    entry = _register_file(username, dest, source="url", name=name)
    return {"ok": True, "file": entry}


@app.post("/api/files/{file_id}/select")
async def select_file(file_id: str, username: str = Depends(current_user)):
    """Switch the calling user's active file."""
    ctx = get_user_state(username)
    with ctx.lock:
        if file_id not in ctx.files_state["files"]:
            return JSONResponse({"ok": False, "error": "Unknown file_id"}, status_code=404)
        ctx.files_state["active_file_id"] = file_id
        ctx.save_files()
        entry = dict(ctx.files_state["files"][file_id])
    return {"ok": True, "file": entry}


@app.delete("/api/files/{file_id}")
async def delete_file(file_id: str, username: str = Depends(current_user)):
    entry = _delete_file(username, file_id)
    if entry is None:
        return JSONResponse({"ok": False, "error": "Unknown file_id"}, status_code=404)
    ctx = get_user_state(username)
    with ctx.lock:
        active_id = ctx.files_state.get("active_file_id")
    return {"ok": True, "deleted": entry, "active_file_id": active_id}


@app.post("/api/files/{file_id}/clear-results")
async def clear_file_results(file_id: str, username: str = Depends(current_user)):
    ctx = get_user_state(username)
    with ctx.lock:
        if file_id not in ctx.files_state["files"]:
            return JSONResponse({"ok": False, "error": "Unknown file_id"}, status_code=404)
    removed = _clear_file_results(username, file_id)
    return {"ok": True, "removed": removed}


@app.get("/api/files/{file_id}/download")
async def download_file(file_id: str, username: str = Depends(current_user)):
    ctx = get_user_state(username)
    with ctx.lock:
        entry = ctx.files_state["files"].get(file_id)
    if not entry:
        return JSONResponse({"ok": False, "error": "Unknown file_id"}, status_code=404)
    path = entry.get("path", "")
    if not path or not os.path.exists(path):
        return JSONResponse({"ok": False, "error": "File not found on disk"}, status_code=404)
    return FileResponse(
        path,
        filename=entry.get("name") or os.path.basename(path),
        media_type="application/octet-stream",
    )


@app.post("/api/files/{file_id}/rename")
async def rename_file(file_id: str, request: Request, username: str = Depends(current_user)):
    """Change the display name of a file in the user's registry. Does not
    touch the file on disk — rename only affects how the UI shows it."""
    data = await request.json()
    new_name = (data.get("name") or "").strip()
    if not new_name:
        return JSONResponse({"ok": False, "error": "Name cannot be empty"}, status_code=400)
    if "/" in new_name or "\\" in new_name:
        return JSONResponse({"ok": False, "error": "Name cannot contain / or \\"}, status_code=400)
    ctx = get_user_state(username)
    with ctx.lock:
        entry = ctx.files_state["files"].get(file_id)
        if not entry:
            return JSONResponse({"ok": False, "error": "Unknown file_id"}, status_code=404)
        entry["name"] = new_name
        ctx.save_files()
        result = dict(entry)
    return {"ok": True, "file": result}


@app.get("/api/tests")
async def get_tests(username: str = Depends(current_user)):
    """Return the global test catalog + the calling user's active file."""
    # Inject this user's custom PGS into the global TESTS list (idempotent
    # — duplicates are ignored).
    _eager_inject_custom_pgs_for_user(username)
    active = _get_active_file(username)
    return {
        "categories": CATEGORIES,
        "tests": TESTS,
        "active_file": active,
        "active_vcf": active["path"] if active else None,
    }


def _queue_task(username, test_def, active):
    """Build + enqueue a task. Each task carries `username` so the worker
    routes the resulting report to the right per-user dir on disk."""
    test_id = test_def["id"]
    task_id = f"{test_id}_{uuid.uuid4().hex[:8]}"
    task = {
        "id": task_id,
        "test_id": test_id,
        "vcf_path": active["path"],
        "file_id": active["id"],
        "username": _norm_username(username),
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }
    with queue_lock:
        task_queue.append(task)
    task_results[task_id] = {
        "status": "queued",
        "test_id": test_id,
        "test_name": test_def["name"],
        "file_id": active["id"],
        "username": _norm_username(username),
        "queued_at": task["queued_at"],
    }
    return task_id


def _resolve_target_file(username, file_id):
    """Pick the file to queue against. Explicit file_id wins over the
    user's active file. The file must belong to this user."""
    ctx = get_user_state(username)
    if file_id:
        with ctx.lock:
            entry = ctx.files_state["files"].get(file_id)
        return dict(entry) if entry else None
    return _get_active_file(username)


@app.post("/api/run/{test_id}")
async def run_single_test(
    test_id: str,
    file_id: str = "",
    username: str = Depends(current_user),
):
    target = _resolve_target_file(username, file_id)
    if not target:
        return JSONResponse({"ok": False, "error": "No file selected. Upload or add a file first."}, status_code=400)
    if test_id not in TESTS_BY_ID:
        return JSONResponse({"ok": False, "error": f"Unknown test: {test_id}"}, status_code=404)
    task_id = _queue_task(username, TESTS_BY_ID[test_id], target)
    return {"ok": True, "task_id": task_id, "file_id": target["id"]}


@app.post("/api/run-category/{category}")
async def run_category(
    category: str,
    file_id: str = "",
    username: str = Depends(current_user),
):
    target = _resolve_target_file(username, file_id)
    if not target:
        return JSONResponse({"ok": False, "error": "No file selected."}, status_code=400)

    task_ids = []
    for test in TESTS:
        if test["category"] == category:
            task_ids.append(_queue_task(username, test, target))
    return {"ok": True, "task_ids": task_ids, "count": len(task_ids), "file_id": target["id"]}


@app.post("/api/run-all")
async def run_all_tests(file_id: str = "", username: str = Depends(current_user)):
    target = _resolve_target_file(username, file_id)
    if not target:
        return JSONResponse({"ok": False, "error": "No file selected."}, status_code=400)

    task_ids = [_queue_task(username, test, target) for test in TESTS]
    return {"ok": True, "task_ids": task_ids, "count": len(task_ids), "file_id": target["id"]}


def _load_reports_for_file(username, file_id):
    """Scan a user's per-file reports dir and return {test_id: latest_summary_dict}."""
    latest = {}
    d = user_reports_root(username) / file_id
    if not d.exists():
        return {}
    for p in d.glob("*.json"):
        try:
            with open(p) as f:
                rep = json.load(f)
        except Exception:
            continue
        test_id = rep.get("test_id")
        if not test_id:
            continue
        completed = rep.get("completed_at", "")
        if test_id in latest and latest[test_id][0] > completed:
            continue
        result = rep.get("result") or {}
        latest[test_id] = (completed, {
            "task_id": rep.get("task_id"),
            "test_id": test_id,
            "test_name": rep.get("test_name"),
            "file_id": file_id,
            "status": result.get("status", "passed"),
            "headline": result.get("headline", ""),
            "error": result.get("error"),
            "elapsed": rep.get("elapsed_seconds"),
            "completed_at": completed,
            # Forward PGS quality fields so the UI can color the match-rate
            # chip and decide whether a report is viewable.
            "match_rate": result.get("match_rate"),
            "match_rate_value": result.get("match_rate_value"),
            "percentile": result.get("percentile"),
            "no_report": result.get("no_report", False),
        })
    return {tid: entry for tid, (_, entry) in latest.items()}


@app.get("/api/status")
async def get_status(username: str = Depends(current_user)):
    """Queue status + task results scoped to the calling user's active file.

    Filters the global queue and task_results by username so users see
    only their own tasks.
    """
    user_lc = _norm_username(username)
    active = _get_active_file(username)
    active_id = active["id"] if active else None

    with queue_lock:
        queued = [
            {"id": t["id"], "test_id": t["test_id"], "file_id": t.get("file_id")}
            for t in task_queue
            if t.get("username") == user_lc and t.get("file_id") == active_id
        ]

    results = {}
    if active_id:
        latest_per_test = _load_reports_for_file(username, active_id)
        for entry in latest_per_test.values():
            results[entry["task_id"]] = entry

    for task_id, res in task_results.items():
        if res.get("username") == user_lc and res.get("file_id") == active_id:
            results[task_id] = res

    with queue_lock:
        running_snapshot = [
            tid for tid in running_tasks
            if task_results.get(tid, {}).get("username") == user_lc
        ]
    return {
        "active_file": active,
        "active_vcf": active["path"] if active else None,
        "queue_length": len(queued),
        "queued_tasks": queued,
        "running_count": len(running_snapshot),
        "running_tasks": running_snapshot,
        "current_task": running_snapshot[0] if running_snapshot else None,
        "results": results,
    }


@app.get("/api/report/{task_id}")
async def get_report(task_id: str, username: str = Depends(current_user)):
    """Get a completed report. Looks under the calling user's reports root."""
    user_root = user_reports_root(username)
    for d in user_root.iterdir() if user_root.exists() else []:
        if d.is_dir():
            candidate = d / f"{task_id}.json"
            if candidate.exists():
                with open(candidate) as f:
                    return json.load(f)
    legacy = user_root / f"{task_id}.json"
    if legacy.exists():
        with open(legacy) as f:
            return json.load(f)

    if task_id in task_results:
        return task_results[task_id]

    return JSONResponse({"error": "Report not found"}, status_code=404)


def _find_report_file(username, task_id):
    """Return the on-disk path of a stored report under the given user, or None."""
    user_root = user_reports_root(username)
    if user_root.exists():
        for d in user_root.iterdir():
            if not d.is_dir():
                continue
            candidate = d / f"{task_id}.json"
            if candidate.exists():
                return candidate
    return None


@app.get("/api/report/{task_id}/download")
async def download_report(task_id: str, username: str = Depends(current_user)):
    path = _find_report_file(username, task_id)
    if path is None:
        return JSONResponse({"error": "Report not found"}, status_code=404)
    return FileResponse(
        str(path),
        filename=f"{task_id}.json",
        media_type="application/json",
    )


@app.get("/api/reports/download")
async def download_reports_zip(file_id: str = "", username: str = Depends(current_user)):
    """Stream a zip of the calling user's reports."""
    import io
    import zipfile

    ctx = get_user_state(username)
    with ctx.lock:
        file_names = {
            fid: entry.get("name", "") or os.path.basename(entry.get("path", ""))
            for fid, entry in ctx.files_state["files"].items()
        }

    user_root = user_reports_root(username)
    if not user_root.exists():
        return JSONResponse({"error": "No reports yet"}, status_code=404)

    buf = io.BytesIO()
    added = 0
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fdir in user_root.iterdir():
            if not fdir.is_dir():
                continue
            if file_id and fdir.name != file_id:
                continue
            safe_name = (file_names.get(fdir.name) or fdir.name).replace("/", "_")
            for p in fdir.glob("*.json"):
                try:
                    zf.write(p, arcname=f"{safe_name}/{p.name}")
                    added += 1
                except OSError:
                    continue

    if added == 0:
        return JSONResponse({"error": "No reports to download"}, status_code=404)

    buf.seek(0)
    label = file_names.get(file_id, "all") if file_id else "all"
    safe_label = label.replace("/", "_")
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="reports-{safe_label}.zip"'
        },
    )


@app.delete("/api/report/{task_id}")
async def delete_report(task_id: str, username: str = Depends(current_user)):
    """Delete a single report under the calling user's reports tree."""
    user_lc = _norm_username(username)
    removed = False
    user_root = user_reports_root(username)
    if user_root.exists():
        for d in user_root.iterdir():
            if not d.is_dir():
                continue
            candidate = d / f"{task_id}.json"
            if candidate.exists():
                try:
                    candidate.unlink()
                    removed = True
                except OSError as e:
                    return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    # Drop in-memory entry only if it belongs to this user
    if task_id in task_results and task_results[task_id].get("username") == user_lc:
        del task_results[task_id]
        removed = True
    return {"ok": True, "removed": removed}


# ── System stats (status bar) ─────────────────────────────────────

def _sh(cmd, timeout=5):
    """Run a shell command and return stdout (empty string on any error)."""
    try:
        r = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return r.stdout.strip()
    except Exception:
        return ""


def _gather_system_stats():
    """Htop-like snapshot of the host: CPU%, memory, load, top processes, GPU.

    Deliberately shells out (top/ps/proc/meminfo/nvidia-smi) to avoid adding
    a psutil dependency. Keeps the payload small — only what the status-bar
    UI needs to render.
    """
    hostname = _sh("hostname")
    uptime_raw = _sh("uptime -p")

    # Load average + CPU topology
    load_raw = _sh("cat /proc/loadavg").split()
    load_avg = (
        [float(load_raw[0]), float(load_raw[1]), float(load_raw[2])]
        if len(load_raw) >= 3 else [0.0, 0.0, 0.0]
    )
    threads = int(_sh("nproc") or "1")

    # CPU% via `top -bn1`, parsing the " XX.X id" idle field.
    cpu_usage = 0.0
    top_out = _sh("top -bn1 | head -5")
    for line in top_out.split("\n"):
        if "Cpu(s)" in line or "%Cpu" in line:
            m = re.search(r"(\d+[\.,]\d+)\s*id", line)
            if m:
                try:
                    cpu_usage = 100.0 - float(m.group(1).replace(",", "."))
                except ValueError:
                    pass
            break

    # Memory via /proc/meminfo (KB → GB)
    mem = {}
    for line in _sh("cat /proc/meminfo").split("\n"):
        parts = line.split()
        if len(parts) >= 2:
            try:
                mem[parts[0].rstrip(":")] = int(parts[1]) / 1024 / 1024
            except ValueError:
                pass
    total_gb = mem.get("MemTotal", 0.0)
    available_gb = mem.get("MemAvailable", 0.0)
    used_gb = max(total_gb - available_gb, 0.0)
    mem_pct = (used_gb / total_gb * 100) if total_gb > 0 else 0.0

    # GPU (optional, quietly falls back to none)
    gpu_available = False
    gpu_devices = []
    nvidia = _sh(
        "nvidia-smi --query-gpu=name,memory.total,memory.used,utilization.gpu,temperature.gpu "
        "--format=csv,noheader,nounits 2>/dev/null"
    )
    if nvidia:
        gpu_available = True
        for gline in nvidia.split("\n"):
            parts = [p.strip() for p in gline.split(",")]
            if len(parts) >= 4:
                try:
                    gpu_devices.append({
                        "name": parts[0],
                        "memory_total_mb": float(parts[1]),
                        "memory_used_mb": float(parts[2]),
                        "utilization_pct": float(parts[3]),
                        "temperature_c": float(parts[4]) if len(parts) >= 5 else None,
                    })
                except ValueError:
                    pass

    # Top 50 processes by CPU% — the UI only shows ~10 but we include more
    # so the "aggregated groups" view can count things like many claude
    # workers across a larger sample.
    processes = []
    ps_out = _sh("ps aux --sort=-%cpu | head -51")
    for line in ps_out.split("\n")[1:]:
        parts = line.split(None, 10)
        if len(parts) >= 11:
            try:
                processes.append({
                    "pid": int(parts[1]),
                    "user": parts[0],
                    "cpu_pct": float(parts[2]),
                    "mem_pct": float(parts[3]),
                    "rss_mb": round(int(parts[5]) / 1024, 1),
                    "command": parts[10][:200],
                })
            except (ValueError, IndexError):
                pass

    return {
        "hostname": hostname,
        "uptime": uptime_raw.replace("up ", "") if uptime_raw.startswith("up ") else uptime_raw,
        "load_avg": load_avg,
        "cpu": {
            "threads": threads,
            "usage_pct": round(cpu_usage, 1),
        },
        "memory": {
            "total_gb": round(total_gb, 1),
            "used_gb": round(used_gb, 1),
            "usage_pct": round(mem_pct, 1),
        },
        "gpu": {"available": gpu_available, "devices": gpu_devices},
        "processes": processes,
        "timestamp": time.time(),
    }


@app.get("/api/system/stats")
async def system_stats():
    return _gather_system_stats()


@app.post("/api/clear-queue")
async def clear_queue(username: str = Depends(current_user)):
    """Clear queued (not running) tasks owned by the calling user."""
    user_lc = _norm_username(username)
    with queue_lock:
        before = len(task_queue)
        # Drop only this user's queued tasks
        kept = deque(t for t in task_queue if t.get("username") != user_lc)
        cleared = before - len(kept)
        task_queue.clear()
        task_queue.extend(kept)
    return {"ok": True, "cleared": cleared}


# ── PGS Catalog search / custom PGS management ───────────────────

def _pgs_already_added(pgs_id):
    return any(
        t.get("test_type") == "pgs_score" and
        (t.get("params") or {}).get("pgs_id", "").upper() == pgs_id.upper()
        for t in TESTS
    )


def _normalize_pgs_hit(raw):
    """Flatten a PGS Catalog /score result into the minimal fields the
    search UI needs."""
    pub = raw.get("publication") or {}
    return {
        "id": raw.get("id", ""),
        "name": raw.get("name", ""),
        "trait_reported": raw.get("trait_reported", ""),
        "variants_number": raw.get("variants_number", 0),
        "weight_type": raw.get("weight_type", ""),
        "first_author": pub.get("firstauthor", ""),
        "year": (pub.get("date_publication") or "")[:4],
        "journal": pub.get("journal", ""),
        "pmid": pub.get("PMID", ""),
        "doi": pub.get("doi", ""),
        "already_added": _pgs_already_added(raw.get("id", "")),
    }


@app.get("/api/pgs/search")
async def pgs_search(q: str = "", limit: int = 20):
    """Search the PGS Catalog. Accepts either a free-text trait query
    ("breast cancer") or a direct PGS ID ("PGS000335").

    Free-text searches hit both /score/search (matches score name/id)
    and /trait/search (matches trait label/synonyms → list of
    associated_pgs_ids which we then expand into full score records).
    The two result sets are merged and deduped by PGS ID.
    """
    q = (q or "").strip()
    if len(q) < 2:
        return {"results": [], "count": 0}

    # Direct PGS ID lookup — one round-trip, single result.
    if re.match(r"^PGS\d{6,}$", q, re.IGNORECASE):
        try:
            raw = _pgs_catalog_get(f"/score/{q.upper()}")
        except Exception as e:
            return JSONResponse(
                {"error": f"PGS Catalog lookup failed: {e}", "results": []},
                status_code=502,
            )
        return {"results": [_normalize_pgs_hit(raw)], "count": 1}

    # ── Free-text search: combine /score/search + /trait/search ───
    score_raw = []
    try:
        data = _pgs_catalog_get("/score/search",
                                {"term": q, "limit": min(limit, 100)})
        score_raw = data.get("results") or []
    except Exception as e:
        logger.warning(f"PGS /score/search failed for {q!r}: {e}")

    trait_pgs_ids = []
    try:
        trait_data = _pgs_catalog_get("/trait/search",
                                      {"term": q, "limit": 10})
        for trait in (trait_data.get("results") or []):
            trait_pgs_ids.extend(trait.get("associated_pgs_ids") or [])
            trait_pgs_ids.extend(trait.get("child_associated_pgs_ids") or [])
    except Exception as e:
        logger.warning(f"PGS /trait/search failed for {q!r}: {e}")

    # Dedupe: prefer the score-search hit (already has metadata).
    have = {s.get("id", "") for s in score_raw if s.get("id")}
    fetch_ids = []
    seen_trait = set()
    for pid in trait_pgs_ids:
        if pid and pid not in have and pid not in seen_trait:
            seen_trait.add(pid)
            fetch_ids.append(pid)

    # Cap how many per-ID fetches we do to keep latency reasonable.
    max_trait_fetch = max(0, limit - len(score_raw))
    fetch_ids = fetch_ids[:max_trait_fetch]

    if fetch_ids:
        from concurrent.futures import ThreadPoolExecutor
        def _fetch_one(pid):
            try:
                return _pgs_catalog_get(f"/score/{pid}")
            except Exception:
                return None
        with ThreadPoolExecutor(max_workers=min(len(fetch_ids), 8)) as ex:
            trait_raw = [r for r in ex.map(_fetch_one, fetch_ids) if r]
    else:
        trait_raw = []

    merged = {}
    for raw in score_raw + trait_raw:
        pid = raw.get("id", "")
        if pid and pid not in merged:
            merged[pid] = _normalize_pgs_hit(raw)

    results = list(merged.values())[:limit]
    return {"results": results, "count": len(results)}


@app.get("/api/pgs/custom")
async def list_custom_pgs(username: str = Depends(current_user)):
    """List the calling user's custom-PGS entries."""
    ctx = get_user_state(username)
    with ctx.lock:
        return {"pgs": list(ctx.custom_pgs_list)}


@app.post("/api/pgs/add")
async def add_custom_pgs(request: Request, username: str = Depends(current_user)):
    """Add a PGS to the calling user's custom list. The PGS test
    definition itself is shared (a global TESTS catalog entry) since the
    runners are stateless w.r.t. user — only the per-user `custom_pgs.json`
    list controls whether the test shows up in this user's UI."""
    data = await request.json()
    pgs_id = (data.get("pgs_id") or "").strip().upper()
    if not re.match(r"^PGS\d{6,}$", pgs_id):
        return JSONResponse(
            {"ok": False, "error": "Invalid PGS ID (expected e.g. PGS000335)"},
            status_code=400,
        )

    ctx = get_user_state(username)
    with ctx.lock:
        if any(p.get("pgs_id", "").upper() == pgs_id for p in ctx.custom_pgs_list):
            return {"ok": True, "already_exists": True, "pgs_id": pgs_id}

    # Fetch metadata from PGS Catalog
    try:
        raw = _pgs_catalog_get(f"/score/{pgs_id}")
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"PGS Catalog lookup failed: {e}"},
            status_code=502,
        )

    trait = raw.get("trait_reported", "") or pgs_id
    nvar = raw.get("variants_number", 0) or 0
    pub = raw.get("publication") or {}
    author = pub.get("firstauthor", "")
    year = (pub.get("date_publication") or "")[:4]
    cite = f"{author} et al., {year}" if author and year else (author or year or "")
    desc_parts = [f"{nvar:,} variants"] if nvar else []
    if cite:
        desc_parts.append(cite)
    if pub.get("journal"):
        desc_parts.append(pub["journal"])

    pgs_info = {
        "pgs_id": pgs_id,
        "trait": trait,
        "name": f"{trait} ({pgs_id})",
        "description": ". ".join(desc_parts) or f"Custom PGS {pgs_id}",
        "variants_number": nvar,
        "added_at": datetime.now(timezone.utc).isoformat(),
    }

    with ctx.lock:
        if not any(p.get("pgs_id", "").upper() == pgs_id for p in ctx.custom_pgs_list):
            ctx.custom_pgs_list.append(pgs_info)
            _add_custom_pgs_to_tests(pgs_info)
            ctx.save_custom_pgs()

    return {"ok": True, "pgs": pgs_info}


@app.delete("/api/pgs/custom/{pgs_id}")
async def remove_custom_pgs(pgs_id: str, username: str = Depends(current_user)):
    pgs_id = pgs_id.upper()
    ctx = get_user_state(username)
    with ctx.lock:
        before = len(ctx.custom_pgs_list)
        ctx.custom_pgs_list[:] = [
            p for p in ctx.custom_pgs_list if p.get("pgs_id", "").upper() != pgs_id
        ]
        removed = before - len(ctx.custom_pgs_list)
        if removed:
            ctx.save_custom_pgs()
    # NOTE: we deliberately don't yank the test from TESTS — another user
    # may still have it in their list. The PGS test catalog is global.
    return {"ok": True, "removed": bool(removed)}


@app.get("/api/errors")
async def get_errors(limit: int = 200, username: str = Depends(current_user)):
    """Return recent failures from the calling user's errors.log."""
    log_path = user_errors_log(username)
    if not log_path.exists():
        return {"errors": [], "count": 0}
    entries = []
    try:
        with open(log_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    entries.reverse()
    return {"errors": entries[:limit], "count": len(entries)}


@app.post("/api/clear-errors")
async def clear_errors(username: str = Depends(current_user)):
    log_path = user_errors_log(username)
    if log_path.exists():
        log_path.unlink()
    return {"ok": True}


@app.get("/api/reports")
async def list_reports(limit: int = 500, username: str = Depends(current_user)):
    """List the calling user's reports, newest first. Enriches each entry
    with the human-readable file name from the user's file registry."""
    ctx = get_user_state(username)
    with ctx.lock:
        file_names = {
            fid: entry.get("name", "") or os.path.basename(entry.get("path", ""))
            for fid, entry in ctx.files_state["files"].items()
        }

    reports = []
    user_root = user_reports_root(username)
    if user_root.exists():
        for fdir in user_root.iterdir():
            if not fdir.is_dir():
                continue
            file_id = fdir.name
            for p in fdir.glob("*.json"):
                try:
                    data = json.loads(p.read_text())
                except Exception:
                    continue
                result = data.get("result") or {}
                reports.append({
                    "task_id":      data.get("task_id"),
                    "test_id":      data.get("test_id"),
                    "test_name":    data.get("test_name"),
                    "category":     data.get("category"),
                    "file_id":      file_id,
                    "file_name":    file_names.get(file_id, f"(unknown: {file_id})"),
                    "completed_at": data.get("completed_at"),
                    "elapsed":      data.get("elapsed_seconds"),
                    "status":       result.get("status", "passed"),
                    "headline":     result.get("headline", ""),
                    "error":        result.get("error"),
                    "match_rate":   result.get("match_rate"),
                    "match_rate_value": result.get("match_rate_value"),
                    "percentile":   result.get("percentile"),
                    "no_report":    result.get("no_report", False),
                })

    reports.sort(key=lambda r: r.get("completed_at") or "", reverse=True)
    return {"reports": reports[:limit], "count": len(reports)}


# ── Auth API ──────────────────────────────────────────────────────

def _set_session_cookie(resp, sid):
    # SameSite=Lax is fine for our use (we never POST cross-site).
    # Secure is omitted because we sit behind nginx and may serve
    # over plain HTTP locally; the proxy adds Secure when appropriate.
    resp.set_cookie(
        SESSION_COOKIE,
        sid,
        max_age=SESSION_TTL_SECONDS,
        httponly=True,
        samesite="lax",
        path="/",
    )


@app.post("/api/auth/signup")
async def auth_signup(request: Request):
    data = await request.json()
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    ok, err = _create_user(username, password)
    if not ok:
        return JSONResponse({"ok": False, "error": err}, status_code=400)
    sid = _create_session(username)
    resp = JSONResponse({"ok": True, "username": _norm_username(username)})
    _set_session_cookie(resp, sid)
    return resp


@app.post("/api/auth/login")
async def auth_login(request: Request):
    data = await request.json()
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not _authenticate(username, password):
        return JSONResponse(
            {"ok": False, "error": "Invalid username or password"},
            status_code=401,
        )
    sid = _create_session(username)
    # Pre-load this user's custom PGS into the global TESTS list.
    try:
        _eager_inject_custom_pgs_for_user(username)
    except Exception:
        pass
    resp = JSONResponse({"ok": True, "username": _norm_username(username)})
    _set_session_cookie(resp, sid)
    return resp


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    sid = request.cookies.get(SESSION_COOKIE)
    if sid:
        _drop_session(sid)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp


@app.get("/api/auth/me")
async def auth_me(request: Request):
    sid = request.cookies.get(SESSION_COOKIE)
    username = _resolve_session(sid)
    if not username:
        return JSONResponse({"authenticated": False}, status_code=401)
    return {"authenticated": True, "username": username}


# ── Frontend ──────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    # The dashboard is a SPA. We always serve the frontend; the JS
    # bootstrap calls /api/auth/me and redirects to the login page on
    # 401 using its prefix-aware BASE constant. This sidesteps any
    # confusion about the nginx /simple/ prefix not being visible to
    # FastAPI here.
    return HTMLResponse(FRONTEND_HTML)


@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return HTMLResponse(_AUTH_PAGE_HTML.replace("__MODE__", "login"))


@app.get("/signup", response_class=HTMLResponse)
async def signup_page():
    return HTMLResponse(_AUTH_PAGE_HTML.replace("__MODE__", "signup"))


_AUTH_PAGE_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sign in — 23 &amp; Claude</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'><defs><linearGradient id='cg' x1='0%25' y1='0%25' x2='100%25' y2='100%25'><stop offset='0%25' stop-color='%2360a5fa'/><stop offset='55%25' stop-color='%238b5cf6'/><stop offset='100%25' stop-color='%23c084fc'/></linearGradient></defs><g transform='translate(32 32)'><rect x='-7' y='-26' width='14' height='52' rx='7' ry='7' fill='url(%23cg)' transform='rotate(-25)'/><rect x='-7' y='-26' width='14' height='52' rx='7' ry='7' fill='url(%23cg)' transform='rotate(25)' opacity='0.85'/><circle r='3' fill='%230a0e17'/></g></svg>">
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  background: #0a0e17;
  color: #e2e8f0;
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
}
.card {
  background: #111827;
  border: 1px solid #2d3748;
  border-radius: 16px;
  padding: 32px 36px;
  width: 100%;
  max-width: 380px;
  box-shadow: 0 12px 50px rgba(0, 0, 0, 0.5);
}
.brand {
  font-size: 1.6rem;
  font-weight: 700;
  background: linear-gradient(135deg, #3b82f6, #8b5cf6);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  margin-bottom: 4px;
  text-align: center;
}
.subtitle {
  text-align: center;
  font-size: 0.85rem;
  color: #94a3b8;
  margin-bottom: 24px;
}
label {
  display: block;
  font-size: 0.75rem;
  color: #94a3b8;
  margin-bottom: 6px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-weight: 600;
}
input {
  width: 100%;
  padding: 10px 14px;
  border: 1px solid #2d3748;
  border-radius: 8px;
  background: #1a2332;
  color: #e2e8f0;
  font-size: 0.9rem;
  margin-bottom: 14px;
}
input:focus { outline: none; border-color: #3b82f6; }
button {
  width: 100%;
  padding: 11px;
  border: none;
  border-radius: 8px;
  background: #3b82f6;
  color: white;
  font-size: 0.9rem;
  font-weight: 600;
  cursor: pointer;
  margin-top: 4px;
}
button:hover { opacity: 0.9; }
button:disabled { opacity: 0.5; cursor: not-allowed; }
.switch {
  text-align: center;
  margin-top: 18px;
  font-size: 0.8rem;
  color: #94a3b8;
}
.switch a { color: #60a5fa; text-decoration: none; }
.switch a:hover { text-decoration: underline; }
.error {
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid #ef4444;
  color: #fca5a5;
  padding: 10px 12px;
  border-radius: 8px;
  font-size: 0.8rem;
  margin-bottom: 14px;
  display: none;
}
.error.show { display: block; }
</style>
</head>
<body>
<div class="card">
  <h1 class="brand">23 &amp; Claude</h1>
  <div class="subtitle" id="subtitle">Sign in to continue</div>
  <div class="error" id="errorBox"></div>
  <form id="authForm" onsubmit="submitForm(event)">
    <label for="username">Email</label>
    <input type="email" id="username" name="username" autocomplete="email" required>
    <label for="password">Password</label>
    <input type="password" id="password" name="password" autocomplete="current-password" required>
    <button type="submit" id="submitBtn">Sign in</button>
  </form>
  <div class="switch" id="switchLink"></div>
</div>
<script>
const BASE = window.location.pathname.startsWith('/simple') ? '/simple' : '';
const MODE = '__MODE__';   // 'login' | 'signup'

(function init() {
  if (MODE === 'signup') {
    document.title = 'Sign up — 23 & Claude';
    document.getElementById('subtitle').textContent = 'Create your account';
    document.getElementById('submitBtn').textContent = 'Sign up';
    document.getElementById('password').setAttribute('autocomplete', 'new-password');
    document.getElementById('switchLink').innerHTML =
      'Already have an account? <a href="' + BASE + '/login">Sign in</a>';
  } else {
    document.getElementById('switchLink').innerHTML =
      "Don't have an account? <a href=\"" + BASE + '/signup">Sign up</a>';
  }
})();

function showError(msg) {
  const box = document.getElementById('errorBox');
  box.textContent = msg;
  box.classList.add('show');
}

async function submitForm(e) {
  e.preventDefault();
  const btn = document.getElementById('submitBtn');
  btn.disabled = true;
  document.getElementById('errorBox').classList.remove('show');
  const username = document.getElementById('username').value.trim();
  const password = document.getElementById('password').value;
  try {
    const resp = await fetch(BASE + '/api/auth/' + MODE, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    const data = await resp.json();
    if (!resp.ok || !data.ok) {
      showError(data.error || (MODE === 'signup' ? 'Sign-up failed' : 'Sign-in failed'));
      btn.disabled = false;
      return;
    }
    // Success → land on the dashboard
    window.location.href = BASE + '/';
  } catch (err) {
    showError('Network error: ' + err.message);
    btn.disabled = false;
  }
}
</script>
</body>
</html>
"""


FRONTEND_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>23 &amp; Claude</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'><defs><linearGradient id='cg' x1='0%25' y1='0%25' x2='100%25' y2='100%25'><stop offset='0%25' stop-color='%2360a5fa'/><stop offset='55%25' stop-color='%238b5cf6'/><stop offset='100%25' stop-color='%23c084fc'/></linearGradient></defs><g transform='translate(32 32)'><rect x='-7' y='-26' width='14' height='52' rx='7' ry='7' fill='url(%23cg)' transform='rotate(-25)'/><rect x='-7' y='-26' width='14' height='52' rx='7' ry='7' fill='url(%23cg)' transform='rotate(25)' opacity='0.85'/><circle r='3' fill='%230a0e17'/></g></svg>">
<style>
:root {
  --bg: #0a0e17;
  --surface: #111827;
  --surface2: #1a2332;
  --border: #2d3748;
  --text: #e2e8f0;
  --text2: #94a3b8;
  --accent: #3b82f6;
  --accent2: #60a5fa;
  --green: #10b981;
  --red: #ef4444;
  --yellow: #f59e0b;
  --purple: #8b5cf6;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
}
.container { max-width: 1400px; margin: 0 auto; padding: 20px; }

/* ── Fixed top stack (header + active file + status bar) ────── */
.top-stack {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  z-index: 100;
  background: var(--bg);
}
.app-header {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
}
.app-header-inner {
  max-width: 1400px;
  margin: 0 auto;
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 10px 20px;
  flex-wrap: wrap;
}
.app-header .brand {
  font-size: 1.2rem;
  font-weight: 700;
  background: linear-gradient(135deg, var(--accent), var(--purple));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  flex-shrink: 0;
  margin-right: 8px;
}
.app-nav {
  display: flex;
  gap: 2px;
}
.app-nav a {
  text-decoration: none;
  color: var(--text2);
  padding: 7px 14px;
  border-radius: 8px;
  font-size: 0.78rem;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  transition: background 0.15s, color 0.15s;
}
.app-nav a:hover { background: var(--surface2); color: var(--text); }
.app-nav a.active {
  background: var(--surface2);
  color: var(--text);
  border: 1px solid var(--border);
}
.app-nav.right { margin-left: auto; }

/* My Data dropdown */
.nav-dropdown { position: relative; }
.nav-dropdown-toggle {
  text-decoration: none; color: var(--text2);
  padding: 7px 14px; border-radius: 8px;
  font-size: 0.78rem; font-weight: 600;
  letter-spacing: 0.06em; text-transform: uppercase;
  transition: background 0.15s, color 0.15s;
  cursor: pointer; background: none; border: none;
  font-family: inherit; display: inline-flex; align-items: center; gap: 4px;
}
.nav-dropdown-toggle:hover { background: var(--surface2); color: var(--text); }
.nav-dropdown-toggle.active { background: var(--surface2); color: var(--text); border: 1px solid var(--border); }
.nav-dropdown-menu {
  display: none; position: absolute; right: 0; top: 100%;
  margin-top: 4px; background: var(--surface); border: 1px solid var(--border);
  border-radius: 8px; min-width: 180px; padding: 4px;
  box-shadow: 0 8px 24px rgba(0,0,0,.35); z-index: 999;
}
.nav-dropdown.open .nav-dropdown-menu { display: block; }
.nav-dropdown-menu a {
  display: block; padding: 8px 14px; color: var(--text2);
  text-decoration: none; border-radius: 6px; font-size: 0.82rem;
  font-weight: 500; white-space: nowrap;
}
.nav-dropdown-menu a:hover { background: var(--surface2); color: var(--text); }
.nav-dropdown-menu .dd-divider { height: 1px; background: var(--border); margin: 4px 8px; }
.nav-dropdown-menu .dd-label {
  padding: 6px 14px 2px; font-size: 0.68rem; font-weight: 600;
  color: var(--text3); text-transform: uppercase; letter-spacing: 0.08em;
}

.header-active-file {
  display: flex;
  align-items: center;
  flex: 1;
  min-width: 240px;
  max-width: 600px;
  /* Visual breathing room from the REPORTS nav link on the left. */
  margin-left: 24px;
}
.header-active-file .file-select {
  flex: 1;
  min-width: 0;
  padding: 6px 10px;
  font-size: 0.78rem;
}

.header-badges { display: flex; gap: 8px; align-items: center; }
.user-chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 4px 4px 4px 8px;
  font-size: 0.72rem;
  color: var(--text2);
}
.user-chip .user-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--green);
  box-shadow: 0 0 6px rgba(16, 185, 129, 0.55);
}
.user-chip .logout-btn {
  padding: 3px 9px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: transparent;
  color: var(--text2);
  cursor: pointer;
  font-size: 0.7rem;
  font-weight: 500;
}
.user-chip .logout-btn:hover {
  border-color: var(--red);
  color: var(--red);
}

/* ── Status bar (server stats + top processes) ───────────────── */
.status-bar {
  background: #0d1117;
  border-bottom: 1px solid #21262d;
  font-family: 'SF Mono', Menlo, Consolas, monospace;
  font-size: 12px;
  color: #c9d1d9;
}
.status-bar-collapsed {
  max-width: 1400px;
  margin: 0 auto;
  padding: 3px 16px;
  display: flex;
  justify-content: center;
}
.status-bar-collapsed button {
  background: transparent;
  border: none;
  color: #6e7681;
  font-family: inherit;
  font-size: 11px;
  cursor: pointer;
  padding: 2px 14px;
  border-radius: 10px;
  transition: background 0.15s, color 0.15s;
  letter-spacing: 0.04em;
}
.status-bar-collapsed button:hover {
  background: #161b22;
  color: #c9d1d9;
}
.status-bar-collapsed .arrow { margin-left: 5px; font-size: 9px; }
.status-bar-close-btn {
  padding: 3px 10px;
  border-radius: 12px;
  background: #161b22;
  border: 1px solid #30363d;
  color: #8b949e;
  cursor: pointer;
  font-family: inherit;
  font-size: 12px;
  margin-left: 6px;
}
.status-bar-close-btn:hover { border-color: #484f58; color: #c9d1d9; }
.status-bar-inner {
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 6px 16px;
  max-width: 1400px;
  margin: 0 auto;
  flex-wrap: wrap;
}
.status-bar-metrics {
  display: flex;
  gap: 10px;
  align-items: center;
  flex-wrap: wrap;
}
.status-bar-chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 3px 9px;
  border-radius: 12px;
  background: #161b22;
  border: 1px solid #30363d;
  color: #8b949e;
  white-space: nowrap;
  line-height: 1.3;
}
.status-bar-chip strong { font-weight: 600; }
.status-bar-divider {
  width: 1px;
  height: 18px;
  background: #30363d;
}
.status-bar-procs {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  overflow: hidden;
  color: #8b949e;
}
.status-bar-proc {
  white-space: nowrap;
  font-weight: 600;
}
.status-bar-proc-count {
  color: #6e7681;
  margin-left: 2px;
  font-weight: 500;
}
.status-bar-expand-btn {
  margin-left: auto;
  display: inline-flex;
  align-items: center;
  padding: 3px 10px;
  border-radius: 12px;
  background: #161b22;
  border: 1px solid #30363d;
  color: #8b949e;
  cursor: pointer;
  font-family: inherit;
  font-size: 12px;
}
.status-bar-expand-btn:hover { border-color: #484f58; color: #c9d1d9; }
.status-bar-expand-btn .arrow { transition: transform 0.15s; margin-left: 4px; font-size: 10px; }
.status-bar-expand-btn.open .arrow { transform: rotate(180deg); }
.status-bar-top-panel {
  background: #0d1117;
  border-bottom: 1px solid #21262d;
  font-family: 'SF Mono', Menlo, Consolas, monospace;
  font-size: 12px;
  padding: 8px 16px 12px;
  max-width: 1400px;
  margin: 0 auto;
}
.status-bar-top-header,
.status-bar-top-row {
  display: flex;
  align-items: center;
  padding: 3px 0;
  gap: 0;
}
.status-bar-top-header {
  color: #6e7681;
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  border-bottom: 1px solid #21262d;
  padding-bottom: 5px;
  margin-bottom: 4px;
}
.status-bar-top-row {
  color: #c9d1d9;
}
.status-bar-top-row .col-pid   { width: 60px; color: #6e7681; }
.status-bar-top-row .col-user  { width: 90px; color: #c9d1d9; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.status-bar-top-row .col-cpu,
.status-bar-top-row .col-mem   { width: 64px; text-align: right; font-weight: 600; }
.status-bar-top-row .col-res   { width: 70px; text-align: right; color: #c9d1d9; }
.status-bar-top-row .col-cmd   { flex: 1; margin-left: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.status-bar-top-row .col-cmd .proc-name { font-weight: 600; margin-right: 6px; }
.status-bar-top-row .col-cmd .proc-args { color: #6e7681; }
.status-bar-top-header .col-pid  { width: 60px; }
.status-bar-top-header .col-user { width: 90px; }
.status-bar-top-header .col-cpu,
.status-bar-top-header .col-mem  { width: 64px; text-align: right; }
.status-bar-top-header .col-res  { width: 70px; text-align: right; }
.status-bar-top-header .col-cmd  { flex: 1; margin-left: 12px; }
header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 24px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  margin-bottom: 24px;
  border-radius: 12px;
}
header h1 {
  font-size: 1.5rem;
  font-weight: 700;
  background: linear-gradient(135deg, var(--accent), var(--purple));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
.header-status {
  display: flex;
  align-items: center;
  gap: 16px;
  font-size: 0.85rem;
  color: var(--text2);
}
.vcf-badge {
  background: var(--surface2);
  padding: 4px 12px;
  border-radius: 20px;
  border: 1px solid var(--border);
  font-family: monospace;
  max-width: 400px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.queue-badge {
  background: var(--accent);
  color: white;
  padding: 4px 12px;
  border-radius: 20px;
  font-weight: 600;
}

/* File Manager */
.file-manager {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 20px;
  margin-bottom: 24px;
}
.file-manager.has-vcf {
  border-color: var(--green);
}
.fm-row {
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
  margin-bottom: 12px;
}
.fm-row:last-child { margin-bottom: 0; }
.fm-label {
  font-size: 0.85rem;
  color: var(--text2);
  font-weight: 500;
  min-width: 110px;
}
.fm-active-row {
  padding-bottom: 12px;
  border-bottom: 1px solid var(--border);
}
.file-select {
  flex: 1;
  min-width: 250px;
  padding: 8px 12px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--surface2);
  color: var(--text);
  font-family: monospace;
  font-size: 0.85rem;
  cursor: pointer;
}
input[type="file"] { display: none; }
.file-btn, .path-btn, .run-btn, .cat-btn {
  padding: 8px 16px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--surface2);
  color: var(--text);
  cursor: pointer;
  font-size: 0.85rem;
  transition: all 0.15s;
  white-space: nowrap;
}
.file-btn:hover, .path-btn:hover { background: var(--accent); border-color: var(--accent); }
.danger-btn {
  background: transparent;
  border-color: var(--red);
  color: var(--red);
}
.danger-btn:hover { background: var(--red); color: white; }
.warn-btn {
  background: transparent;
  border-color: var(--yellow);
  color: var(--yellow);
}
.warn-btn:hover { background: var(--yellow); color: var(--bg); }
.path-input, .url-input {
  flex: 1;
  padding: 8px 12px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--surface2);
  color: var(--text);
  font-family: monospace;
  font-size: 0.85rem;
  min-width: 300px;
}
.fm-status {
  font-size: 0.75rem;
  color: var(--text2);
  font-style: italic;
  flex-basis: 100%;
  padding-top: 4px;
}
.fm-status.error { color: var(--red); font-style: normal; }
.fm-status.ok { color: var(--green); font-style: normal; }
.divider { color: var(--text2); font-size: 0.8rem; }

/* Category sections */
.category {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  margin-bottom: 16px;
  overflow: hidden;
}
.category-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 20px;
  background: var(--surface2);
  cursor: pointer;
  user-select: none;
}
.category-header h2 {
  font-size: 1rem;
  font-weight: 600;
  display: flex;
  align-items: center;
  gap: 8px;
}
.category-header .toggle { color: var(--text2); font-size: 0.85rem; }
.cat-actions { display: flex; gap: 8px; align-items: center; }
.cat-btn {
  font-size: 0.75rem;
  padding: 4px 12px;
  background: var(--accent);
  border-color: var(--accent);
  color: white;
}
.cat-btn:hover { opacity: 0.85; }
.cat-count {
  font-size: 0.75rem;
  color: var(--text2);
  background: var(--bg);
  padding: 2px 8px;
  border-radius: 10px;
}
.cat-counts {
  font-size: 0.75rem;
  color: var(--text2);
  display: inline-flex;
  gap: 6px;
  flex-wrap: wrap;
}
.cat-counts .cnt {
  padding: 2px 8px;
  border-radius: 10px;
  background: var(--bg);
  border: 1px solid var(--border);
  font-weight: 500;
}
.cat-counts .cnt.queued { color: var(--yellow); border-color: var(--yellow); opacity: 0.85; }
.cat-counts .cnt.running { color: var(--accent); border-color: var(--accent); animation: pulse 1.4s infinite; }
.cat-counts .cnt.passed { color: var(--green); border-color: var(--green); }
.cat-counts .cnt.warning { color: var(--yellow); border-color: var(--yellow); }
.cat-counts .cnt.failed { color: var(--red); border-color: var(--red); }
.match-chip {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 0.7rem;
  font-weight: 600;
  font-family: monospace;
  border: 1px solid var(--border);
  margin-left: 6px;
  white-space: nowrap;
}
.match-chip.match-green  { color: var(--green);  border-color: var(--green); }
.match-chip.match-yellow { color: var(--yellow); border-color: var(--yellow); }
.match-chip.match-red    { color: var(--red);    border-color: var(--red); }
.meta-item.match-green  span { color: var(--green); }
.meta-item.match-yellow span { color: var(--yellow); }
.meta-item.match-red    span { color: var(--red); }

/* Test rows */
.tests-body { display: none; }
.tests-body.open { display: block; }

/* Sub-categories within PGS sections */
.subcategory { }
.subcategory-header {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 20px;
  background: var(--bg);
  border-top: 1px solid var(--border);
  border-bottom: 1px solid var(--border);
  font-size: 0.75rem;
  font-weight: 600;
  color: var(--text2);
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.subcategory:first-child .subcategory-header { border-top: none; }
.subcategory-header .sub-name { color: var(--accent2); }
.subcategory-header .sub-count {
  font-size: 0.7rem;
  color: var(--text2);
  background: var(--surface);
  padding: 1px 7px;
  border-radius: 8px;
  border: 1px solid var(--border);
  font-weight: 500;
}
.test-row {
  display: grid;
  grid-template-columns: 1fr 380px 100px;
  align-items: center;
  padding: 10px 20px;
  border-top: 1px solid var(--border);
  transition: background 0.15s;
  gap: 12px;
}
.test-row:hover { background: var(--surface2); }
.test-info h3 { font-size: 0.9rem; font-weight: 500; }
.test-info p { font-size: 0.75rem; color: var(--text2); margin-top: 2px; }
.test-status {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.8rem;
  min-width: 0;
}
.test-status .headline {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  flex: 1;
}
.test-status.passed .headline { color: var(--green); font-weight: 500; }
.test-status.warning .headline { color: var(--yellow); font-weight: 500; }
.test-status.failed .headline {
  color: var(--red);
  font-weight: 500;
  cursor: help;
}
.status-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  display: inline-block;
  flex-shrink: 0;
}
.status-dot.idle { background: var(--border); }
.status-dot.queued { background: var(--yellow); opacity: 0.6; }
.status-dot.running { background: var(--accent); animation: pulse 1s infinite; }
.status-dot.passed { background: var(--green); }
.status-dot.completed { background: var(--green); }
.status-dot.warning { background: var(--yellow); }
.status-dot.failed, .status-dot.error { background: var(--red); }
@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.4; }
}
.run-btn {
  background: var(--accent);
  border-color: var(--accent);
  color: white;
  font-weight: 500;
}
.run-btn:hover { opacity: 0.85; }
.run-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.view-btn {
  padding: 6px 12px;
  border: 1px solid var(--green);
  border-radius: 8px;
  background: transparent;
  color: var(--green);
  cursor: pointer;
  font-size: 0.8rem;
}
.view-btn:hover { background: var(--green); color: white; }
.clear-row-btn {
  padding: 6px 10px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: transparent;
  color: var(--text2);
  cursor: pointer;
  font-size: 0.8rem;
  margin-right: 4px;
}
.clear-row-btn:hover {
  border-color: var(--red);
  color: var(--red);
  background: transparent;
}

/* PGS search modal */
.pgs-search-input {
  width: 100%;
  padding: 12px 14px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--surface2);
  color: var(--text);
  font-size: 0.95rem;
  margin-bottom: 12px;
}
.pgs-search-input:focus { outline: none; border-color: var(--accent); }
.pgs-search-status {
  font-size: 0.8rem;
  color: var(--text2);
  margin-bottom: 10px;
}
.pgs-search-status.error { color: var(--red); }
.pgs-results {
  max-height: 60vh;
  overflow-y: auto;
  border: 1px solid var(--border);
  border-radius: 8px;
}
.pgs-result {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 12px 14px;
  border-bottom: 1px solid var(--border);
}
.pgs-result:last-child { border-bottom: none; }
.pgs-result:hover { background: var(--surface2); }
.pgs-result-main { flex: 1; min-width: 0; }
.pgs-result-title {
  font-size: 0.9rem;
  font-weight: 500;
  color: var(--text);
  margin-bottom: 3px;
}
.pgs-result-id {
  font-family: monospace;
  font-size: 0.75rem;
  color: var(--accent);
  margin-left: 6px;
}
.pgs-result-meta {
  font-size: 0.75rem;
  color: var(--text2);
}
.add-pgs-btn {
  padding: 6px 14px;
  border: 1px solid var(--accent);
  border-radius: 8px;
  background: var(--accent);
  color: white;
  cursor: pointer;
  font-size: 0.8rem;
  font-weight: 500;
  white-space: nowrap;
}
.add-pgs-btn:hover { opacity: 0.85; }
.add-pgs-btn:disabled {
  background: transparent;
  color: var(--green);
  border-color: var(--green);
  cursor: default;
}
.top-controls .add-pgs-top-btn {
  background: var(--purple);
  border-color: var(--purple);
  color: white;
  font-weight: 500;
}
.top-controls .add-pgs-top-btn:hover { opacity: 0.85; }

/* Modal */
.modal-overlay {
  display: none;
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.7);
  z-index: 1000;
  justify-content: center;
  align-items: center;
}
.modal-overlay.open { display: flex; }
.modal {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 16px;
  max-width: 800px;
  width: 90%;
  max-height: 85vh;
  overflow-y: auto;
  padding: 24px;
}
.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}
.modal-header h2 { font-size: 1.2rem; }
.modal-close {
  background: none;
  border: none;
  color: var(--text2);
  font-size: 1.5rem;
  cursor: pointer;
}
.report-content {
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 16px;
  font-family: monospace;
  font-size: 0.85rem;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-word;
}
.report-meta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 12px;
  margin-bottom: 16px;
}
.meta-item {
  background: var(--surface2);
  padding: 8px 12px;
  border-radius: 8px;
}
.meta-item label { font-size: 0.7rem; color: var(--text2); display: block; text-transform: uppercase; letter-spacing: 0.5px; }
.meta-item span { font-size: 0.9rem; font-weight: 500; }

/* Top bar controls */
.top-controls {
  display: flex;
  gap: 8px;
  margin-bottom: 16px;
  flex-wrap: wrap;
}
.top-controls button {
  padding: 8px 16px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--surface);
  color: var(--text);
  cursor: pointer;
  font-size: 0.85rem;
}
.top-controls button:hover { background: var(--surface2); }
.top-controls .run-all-btn { background: var(--green); border-color: var(--green); color: white; font-weight: 600; }
.top-controls .clear-btn { border-color: var(--red); color: var(--red); }
.search-box {
  padding: 8px 12px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--surface);
  color: var(--text);
  font-size: 0.85rem;
  flex-grow: 1;
  min-width: 200px;
}

/* ── View switching (Tests / My Data / Reports) ───────────── */
.view { display: none; }
.view.active { display: block; }

/* ── AI Assistant view ─────────────────────────────────────────────
   Styled to feel like a chat app: header + sub-tabs + scrollable
   message log + sticky input bar. The chat-view-wrap is what fills
   the available height (the dashboard's container has padding from
   the fixed top stack, so we use a viewport calc here). */
.chat-view-wrap.active {
  display: flex;
  flex-direction: column;
  height: calc(100vh - var(--top-stack-h, 200px));
  margin: -24px;
  padding: 0;
}
.chat-panel {
  display: flex;
  flex-direction: column;
  flex: 1;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 0;
  overflow: hidden;
  min-height: 0;
}
.chat-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 20px;
  border-bottom: 1px solid var(--border);
  background: var(--surface2);
  flex-shrink: 0;
}
.chat-header-left { display: flex; align-items: center; gap: 14px; }
.chat-header-left h2 { margin: 0; font-size: 1.05rem; font-weight: 600; color: var(--text); }
.chat-header-actions { display: flex; gap: 8px; }
.chat-header-actions button {
  background: var(--surface);
  color: var(--text2);
  border: 1px solid var(--border);
  border-radius: 4px;
  padding: 5px 12px;
  font-size: 0.78rem;
  cursor: pointer;
}
.chat-header-actions button:hover { background: var(--surface3, var(--border)); color: var(--text); }
.chat-status-badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 3px 10px;
  border-radius: 12px;
  font-size: 0.72rem;
  font-weight: 500;
  background: rgba(127, 127, 127, 0.15);
  color: var(--text2);
}
.chat-status-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: #8b949e;
}
.chat-status-badge.idle .chat-status-dot { background: #3fb950; }
.chat-status-badge.busy .chat-status-dot { background: #d29922; animation: chat-pulse 1.5s infinite; }
.chat-status-badge.stopped .chat-status-dot { background: #f85149; }
@keyframes chat-pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }

.chat-tab-bar {
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 6px 20px;
  border-bottom: 1px solid var(--border);
  background: var(--surface);
  flex-shrink: 0;
}
.chat-tab {
  background: transparent;
  color: var(--text2);
  border: none;
  border-bottom: 2px solid transparent;
  padding: 6px 14px;
  font-size: 0.82rem;
  cursor: pointer;
  font-weight: 500;
}
.chat-tab:hover { color: var(--text); }
.chat-tab.active {
  color: var(--text);
  border-bottom-color: var(--green, #3fb950);
}
.chat-tab-stop {
  margin-left: auto;
  background: rgba(248, 81, 73, 0.15);
  color: #f85149;
  border: 1px solid rgba(248, 81, 73, 0.4);
  border-radius: 4px;
  padding: 4px 12px;
  font-size: 0.75rem;
  cursor: pointer;
}

.chat-sub {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-height: 0;
}

.chat-messages {
  flex: 1;
  overflow-y: auto;
  padding: 20px;
  background: var(--bg, #0d1117);
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.chat-welcome {
  color: var(--text2);
  font-size: 0.9rem;
  line-height: 1.5;
  max-width: 580px;
  margin: 30px auto;
}
.chat-welcome h3 { color: var(--text); font-size: 1.1rem; margin: 0 0 12px 0; font-weight: 600; }
.chat-welcome ul { padding-left: 20px; margin: 8px 0; }
.chat-welcome li { margin-bottom: 4px; }

.chat-bubble {
  max-width: 85%;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.chat-bubble.user { align-self: flex-end; align-items: flex-end; }
.chat-bubble.assistant { align-self: flex-start; align-items: flex-start; }
.chat-bubble-content {
  padding: 10px 14px;
  border-radius: 10px;
  font-size: 0.88rem;
  line-height: 1.5;
  word-break: break-word;
  white-space: normal;
}
.chat-bubble.user .chat-bubble-content {
  background: #1f6feb;
  color: white;
  border-bottom-right-radius: 3px;
}
.chat-bubble.assistant .chat-bubble-content {
  background: var(--surface2);
  color: var(--text);
  border: 1px solid var(--border);
  border-bottom-left-radius: 3px;
}
.chat-bubble-time { font-size: 0.68rem; color: var(--text2); padding: 0 4px; }
.chat-inline-code {
  background: rgba(110, 118, 129, 0.4);
  padding: 1px 5px;
  border-radius: 3px;
  font-family: ui-monospace, "SF Mono", Consolas, monospace;
  font-size: 0.85em;
}
.chat-code-block {
  background: var(--bg, #0d1117);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 10px 12px;
  overflow-x: auto;
  margin: 6px 0;
  font-size: 0.8rem;
  font-family: ui-monospace, "SF Mono", Consolas, monospace;
}
.chat-code-block code { color: var(--text); }

.chat-typing {
  display: flex;
  align-items: center;
  gap: 8px;
  color: var(--text2);
  font-size: 0.82rem;
  padding: 4px 8px;
}
.typing-dots { display: inline-flex; gap: 3px; }
.typing-dots span {
  width: 5px;
  height: 5px;
  border-radius: 50%;
  background: var(--text2);
  animation: chat-bounce 1.2s infinite;
}
.typing-dots span:nth-child(2) { animation-delay: 0.2s; }
.typing-dots span:nth-child(3) { animation-delay: 0.4s; }
@keyframes chat-bounce { 0%,80%,100% { transform: scale(0.7); opacity: 0.4; } 40% { transform: scale(1); opacity: 1; } }

.chat-input-bar {
  display: flex;
  align-items: flex-end;
  gap: 8px;
  padding: 12px 16px;
  border-top: 1px solid var(--border);
  background: var(--surface2);
  flex-shrink: 0;
}
.chat-input-bar textarea {
  flex: 1;
  resize: none;
  background: var(--bg, #0d1117);
  color: var(--text);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px 12px;
  font-family: inherit;
  font-size: 0.88rem;
  line-height: 1.4;
  max-height: 150px;
  min-height: 36px;
  outline: none;
}
.chat-input-bar textarea:focus { border-color: var(--green, #3fb950); }
.chat-send-btn {
  background: var(--green, #3fb950);
  color: white;
  border: none;
  border-radius: 6px;
  padding: 8px 18px;
  font-size: 0.85rem;
  font-weight: 600;
  cursor: pointer;
}
.chat-send-btn:disabled { opacity: 0.5; cursor: not-allowed; }
.chat-stop-btn {
  background: rgba(248, 81, 73, 0.15);
  color: #f85149;
  border: 1px solid rgba(248, 81, 73, 0.4);
  border-radius: 6px;
  padding: 8px 18px;
  font-size: 0.85rem;
  cursor: pointer;
}
.chat-raw-prompt {
  color: var(--green, #3fb950);
  font-family: ui-monospace, monospace;
  align-self: center;
  padding: 0 4px;
  font-weight: 700;
}

.chat-raw-output {
  flex: 1;
  overflow: auto;
  background: var(--bg, #0d1117);
  font-family: ui-monospace, "SF Mono", Consolas, monospace;
  font-size: 0.78rem;
  line-height: 1.4;
  padding: 12px 16px;
}
.chat-raw-pre {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-word;
  color: var(--text);
}
.chat-raw-empty {
  color: var(--text2);
  font-style: italic;
  text-align: center;
  margin-top: 40px;
}
.view h2 {
  font-size: 1.3rem;
  font-weight: 600;
  margin-bottom: 16px;
  color: var(--text);
}
.view h3 {
  font-size: 0.9rem;
  font-weight: 500;
  color: var(--text2);
  margin-top: 24px;
  margin-bottom: 10px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

/* Reports list */
.reports-table {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
}
.reports-header,
.reports-row {
  display: grid;
  grid-template-columns: 90px 130px 1.4fr 1.6fr 70px 60px 130px 200px;
  gap: 10px;
  padding: 10px 14px;
  align-items: center;
  font-size: 0.82rem;
}
.reports-row .rep-actions {
  display: flex;
  gap: 6px;
  justify-content: flex-end;
}
.reports-header {
  background: var(--surface2);
  color: var(--text2);
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  font-size: 0.7rem;
}
.reports-row {
  border-top: 1px solid var(--border);
}
.reports-row:hover { background: var(--surface2); }
.reports-row .rep-when {
  color: var(--text2);
  font-family: monospace;
  font-size: 0.72rem;
  white-space: nowrap;
  display: flex;
  flex-direction: column;
  line-height: 1.25;
}
.reports-row .rep-when .date { color: var(--text); }
.reports-row .rep-when .time { color: var(--text2); font-size: 0.68rem; }

/* PGS rate cells */
.reports-row .rep-match,
.reports-row .rep-pct {
  font-family: monospace;
  font-size: 0.78rem;
  text-align: right;
  font-weight: 600;
}
.reports-row .rep-match.match-green  { color: var(--green); }
.reports-row .rep-match.match-yellow { color: var(--yellow); }
.reports-row .rep-match.match-red    { color: var(--red); }
.reports-row .rep-match.match-none,
.reports-row .rep-pct.dim            { color: #4b5563; }

/* Sortable headers */
.reports-header > div {
  cursor: pointer;
  user-select: none;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 2px 6px;
  margin: -2px -6px;
  border-radius: 6px;
  transition: background 0.12s, color 0.12s;
}
.reports-header > div:hover {
  color: var(--text);
  background: var(--surface);
}
.reports-header > div .sort-arrow {
  font-size: 10px;
  opacity: 0.55;
}
.reports-header > div:hover .sort-arrow { opacity: 0.85; }
.reports-header > div.sort-active {
  color: var(--accent2);
  background: rgba(96, 165, 250, 0.12);
}
.reports-header > div.sort-active .sort-arrow {
  opacity: 1;
  color: var(--accent2);
}
.reports-row .rep-file {
  color: var(--text2);
  font-family: monospace;
  font-size: 0.75rem;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.reports-row .rep-test {
  color: var(--text);
  font-weight: 500;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.reports-row .rep-headline {
  color: var(--text2);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 0.78rem;
}
.reports-row .rep-cat {
  color: var(--text2);
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.03em;
}
.reports-row .rep-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
.reports-row .rep-status.passed   { color: var(--green); }
.reports-row .rep-status.warning  { color: var(--yellow); }
.reports-row .rep-status.failed   { color: var(--red); }
.reports-row .rep-elapsed {
  color: var(--text2);
  font-size: 0.75rem;
  text-align: right;
}
.reports-row .rep-actions { text-align: right; }
.reports-empty {
  padding: 32px;
  text-align: center;
  color: var(--text2);
  font-size: 0.9rem;
}
.reports-filter-row {
  display: flex;
  gap: 10px;
  align-items: center;
  margin-bottom: 14px;
  flex-wrap: wrap;
}
.reports-filter-row .count {
  color: var(--text2);
  font-size: 0.8rem;
}

/* My Data: existing file list */
.data-files-table {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
  margin-top: 12px;
}
.data-files-row {
  display: grid;
  grid-template-columns: 1fr 100px 140px 180px 200px;
  gap: 12px;
  padding: 10px 16px;
  align-items: center;
  border-top: 1px solid var(--border);
  font-size: 0.82rem;
}
.data-files-row.header {
  background: var(--surface2);
  color: var(--text2);
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  font-size: 0.7rem;
  border-top: none;
}
.data-files-row .df-name {
  font-family: monospace;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--text);
}
.data-files-row .df-size,
.data-files-row .df-src,
.data-files-row .df-added {
  color: var(--text2);
  font-size: 0.78rem;
}
.data-files-row .df-actions {
  display: flex;
  gap: 6px;
  justify-content: flex-end;
}
.data-files-row.active-row {
  background: rgba(59, 130, 246, 0.08);
}
.data-files-row.active-row .df-name { color: var(--accent2); font-weight: 600; }
</style>
</head>
<body>
<!-- Fixed top stack: brand + nav (with active file inline), server stats -->
<div class="top-stack" id="topStack">
  <div class="app-header">
    <div class="app-header-inner">
      <h1 class="brand">23 &amp; Claude</h1>
      <nav class="app-nav left" id="appNav">
        <a href="#/chat" data-view="chat">AI</a>
        <a href="#/tests" data-view="tests">Runs</a>
        <a href="#/reports" data-view="reports">Reports</a>
      </nav>
      <div class="header-active-file">
        <select class="file-select" id="fileSelect" onchange="selectFile(this.value)">
          <option value="">— no file loaded —</option>
        </select>
      </div>
      <div class="nav-dropdown" id="myDataDropdown">
        <button class="nav-dropdown-toggle" id="myDataToggle" onclick="toggleMyDataDropdown(event)">My Data <span style="font-size:0.6em">&#9662;</span></button>
        <div class="nav-dropdown-menu">
          <a href="#/data" data-view="data" onclick="closeMyDataDropdown()">My Files</a>
          <div class="dd-divider"></div>
          <div class="dd-label">Tools</div>
          <a href="/ancestry/" target="_blank">Ancestry Analysis</a>
          <a href="/convert" target="_blank">File Converter</a>
        </div>
      </div>
      <div class="header-badges">
        <div class="user-chip" id="userChip" style="display:none" title="Signed in">
          <span class="user-dot"></span>
          <button class="logout-btn" onclick="doLogout()" title="Sign out">Logout</button>
        </div>
      </div>
    </div>
  </div>
  <span id="vcfBadge" style="display:none"></span>

  <div class="status-bar" id="statusBar">
    <div class="status-bar-collapsed" id="statusBarCollapsed">
      <button type="button" onclick="setStatusLevel(1)">Server stats<span class="arrow">&#9662;</span></button>
    </div>
    <div class="status-bar-inner" id="statusBarInner" style="display:none"></div>
    <div class="status-bar-top-panel" id="statusBarTopPanel" style="display:none"></div>
  </div>
</div>

<div class="container">
  <!-- Tests view -->
  <div id="view-tests" class="view">
    <div class="top-controls">
      <input type="text" class="search-box" id="searchBox" placeholder="Search tests..." oninput="filterTests()">
      <button onclick="expandAll()">Expand All</button>
      <button onclick="collapseAll()">Collapse All</button>
      <button class="run-all-btn" onclick="runAll()">Run All Tests</button>
      <button class="add-pgs-top-btn" onclick="openPgsModal()">+ Add PGS</button>
      <button class="clear-btn" onclick="clearQueue()">Clear Queue</button>
      <button onclick="openErrors()">Error Log</button>
    </div>
    <div id="testsContainer"></div>
  </div>

  <!-- My Data view -->
  <div id="view-data" class="view">
    <h2>My Data</h2>
    <div class="file-manager" id="fileManager">
      <h3>Add a file</h3>
      <div class="fm-row">
        <span class="fm-label">Upload:</span>
        <label class="file-btn" for="fileInput">Choose file…</label>
        <input type="file" id="fileInput" accept=".vcf,.vcf.gz,.gvcf,.gvcf.gz,.g.vcf.gz,.bcf">
        <span class="divider">or</span>
        <span class="fm-label" style="min-width:auto">Local path:</span>
        <input type="text" class="path-input" id="pathInput" placeholder="/data/vcfs/sample.vcf.gz">
        <button class="path-btn" onclick="addPath()">Add</button>
      </div>
      <div class="fm-row">
        <span class="fm-label">Remote URL:</span>
        <input type="text" class="url-input" id="urlInput" placeholder="https://example.com/sample.vcf.gz">
        <button class="path-btn" onclick="addUrl()">Download &amp; Add</button>
      </div>
      <div class="fm-status" id="fmStatus"></div>
    </div>

    <h3>Registered files</h3>
    <div id="dataFilesList"></div>
  </div>

  <!-- Reports view -->
  <div id="view-reports" class="view">
    <h2>Reports</h2>
    <div class="reports-filter-row">
      <input type="text" class="search-box" id="reportsSearch" placeholder="Filter reports…" oninput="renderReportsView()" style="max-width:360px">
      <button onclick="loadReports()">Refresh</button>
      <button onclick="downloadAllReports()" class="file-btn">Download all (zip)</button>
      <span class="count" id="reportsCount"></span>
    </div>
    <div id="reportsScope" style="font-size:0.8rem;color:var(--text2);margin-bottom:10px"></div>
    <div id="reportsList"></div>
  </div>

  <!-- AI Assistant view -->
  <div id="view-chat" class="view chat-view-wrap">
    <div class="chat-panel">
      <div class="chat-header">
        <div class="chat-header-left">
          <h2>AI Assistant</h2>
          <span class="chat-status-badge" id="chatStatusBadge">
            <span class="chat-status-dot"></span>
            <span id="chatStatusText">Loading…</span>
          </span>
        </div>
        <div class="chat-header-actions">
          <button onclick="chatRestart()">Restart</button>
          <button onclick="chatClear()">Clear</button>
        </div>
      </div>

      <div class="chat-tab-bar">
        <button class="chat-tab" id="chatTabChat" onclick="chatSwitchTab('chat')">Chat</button>
        <button class="chat-tab" id="chatTabTerminal" onclick="chatSwitchTab('terminal')">Terminal</button>
        <button class="chat-tab-stop" id="chatStopBtn" onclick="chatInterrupt()" style="display:none">Stop</button>
      </div>

      <!-- Chat sub-tab -->
      <div id="chatSubChat" class="chat-sub">
        <div class="chat-messages" id="chatMessages">
          <div class="chat-welcome">
            <h3>Welcome to the Genomics AI Assistant</h3>
            <p>I can help you with:</p>
            <ul>
              <li>Investigating test results from this dashboard</li>
              <li>Running custom bcftools / plink2 / samtools commands</li>
              <li>Searching the PGS Catalog</li>
              <li>Looking up specific variants in your VCF</li>
              <li>Explaining ancestry / PGS / QC outputs</li>
            </ul>
            <p style="margin-top:16px;font-size:0.85rem">Type a message to get started.</p>
          </div>
        </div>
        <div class="chat-input-bar">
          <textarea id="chatInput" rows="1" placeholder="Ask about your genomic data…"
                    onkeydown="chatInputKey(event)" oninput="chatInputAutosize()"></textarea>
          <button class="chat-send-btn" id="chatSendBtn" onclick="chatSend()">Send</button>
        </div>
      </div>

      <!-- Terminal sub-tab -->
      <div id="chatSubTerminal" class="chat-sub" style="display:none">
        <div class="chat-raw-output" id="chatRawOutput">
          <div class="chat-raw-empty">Loading terminal output…</div>
        </div>
        <div class="chat-input-bar">
          <span class="chat-raw-prompt">$</span>
          <textarea id="chatRawInput" rows="1" placeholder="Type a command and press Enter…"
                    onkeydown="chatRawKey(event)"></textarea>
          <button class="chat-send-btn" onclick="chatRawSend()">Send</button>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- Report Modal -->
<div class="modal-overlay" id="reportModal">
  <div class="modal">
    <div class="modal-header">
      <h2 id="modalTitle">Report</h2>
      <button class="modal-close" onclick="closeModal()">&times;</button>
    </div>
    <div class="report-meta" id="reportMeta"></div>
    <div class="report-content" id="reportContent"></div>
  </div>
</div>

<!-- PGS Catalog Search Modal -->
<div class="modal-overlay" id="pgsSearchModal">
  <div class="modal" style="max-width: 900px;">
    <div class="modal-header">
      <h2>Search PGS Catalog</h2>
      <button class="modal-close" onclick="closePgsModal()">&times;</button>
    </div>
    <input type="text" class="pgs-search-input" id="pgsSearchInput"
           placeholder="Search by trait or PGS ID (e.g. 'breast cancer', 'diabetes', 'PGS000335')"
           oninput="debouncedPgsSearch()">
    <div class="pgs-search-status" id="pgsSearchStatus">Type at least 2 characters to search…</div>
    <div class="pgs-results" id="pgsSearchResults"></div>
  </div>
</div>

<script>
// Detect app base path from current URL (e.g., "/simple" when served via nginx,
// "" when accessed directly on port 8800)
const BASE = window.location.pathname.startsWith('/simple') ? '/simple' : '';
let tests = [];
let categories = [];
let testStatus = {};  // test_id -> { status, headline, error }
let taskMap = {};     // test_id -> task_id (latest for the active file)
let files = [];       // [{id, name, path, source, size, added_at}]
let activeFileId = null;

function fmStatus(msg, kind) {
  const el = document.getElementById('fmStatus');
  el.textContent = msg || '';
  el.className = 'fm-status' + (kind ? ' ' + kind : '');
}

function formatSize(n) {
  if (!n) return '';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return n.toFixed(n < 10 && i > 0 ? 1 : 0) + ' ' + units[i];
}

async function init() {
  const resp = await fetch(BASE + '/api/tests');
  const data = await resp.json();
  tests = data.tests;
  categories = data.categories;
  renderTests();
  await refreshFiles();
  pollStatus();
}

async function refreshFiles() {
  const resp = await fetch(BASE + '/api/files');
  const data = await resp.json();
  files = data.files || [];
  activeFileId = data.active_file_id;
  renderFileSelect();
  updateVcfBadge();
}

// Special pseudo-file id for "All files" mode. Frontend-only — never
// sent to /api/files/<id>/select. Triggers cross-file behavior in the
// reports view (no filter) and run buttons (iterate over every file).
const ALL_FILES = '__all__';

function isAllMode() { return activeFileId === ALL_FILES; }

function renderFileSelect() {
  const sel = document.getElementById('fileSelect');
  if (files.length === 0) {
    sel.innerHTML = '<option value="">— no file loaded —</option>';
    return;
  }
  const allOpt = `<option value="${ALL_FILES}" ${isAllMode() ? 'selected' : ''}>★ All files (${files.length})</option>`;
  const fileOpts = files.map(f => {
    const label = `${f.name}  (${f.source}${f.size ? ', ' + formatSize(f.size) : ''})`;
    const selected = f.id === activeFileId ? 'selected' : '';
    return `<option value="${f.id}" ${selected}>${escapeHtml(label)}</option>`;
  }).join('');
  sel.innerHTML = allOpt + fileOpts;
}

function updateVcfBadge() {
  // The visible representation of the active file is now the
  // <select> in the header itself; the legacy #vcfBadge element is
  // kept hidden so this function can still be called from older code
  // paths without crashing.
  const badge = document.getElementById('vcfBadge');
  const fm = document.getElementById('fileManager');
  let label;
  if (isAllMode()) {
    label = `All files (${files.length})`;
  } else {
    const active = files.find(f => f.id === activeFileId);
    label = active ? active.name : 'No file loaded';
  }
  if (badge) badge.textContent = label;
  if (fm) {
    fm.classList.toggle('has-vcf', !isAllMode() && !!files.find(f => f.id === activeFileId));
  }
}

async function selectFile(fileId) {
  if (!fileId) return;

  // "All files" is a frontend-only mode — don't POST to /select.
  // The server-side active file stays unchanged; we just stop scoping
  // by it locally.
  if (fileId === ALL_FILES) {
    activeFileId = ALL_FILES;
    testStatus = {};
    taskMap = {};
    renderTests();
    updateVcfBadge();
    renderFileSelect();  // re-mark the option as selected
    fmStatus('', '');
    if (currentView() === 'reports') renderReportsView();
    if (currentView() === 'data') renderDataFiles();
    return;
  }

  const resp = await fetch(BASE + `/api/files/${fileId}/select`, { method: 'POST' });
  const data = await resp.json();
  if (!data.ok) {
    fmStatus(data.error || 'Failed to select file', 'error');
    return;
  }
  activeFileId = fileId;
  // Wipe the per-file test view — pollStatus() will immediately rebuild it
  // from the newly-active file's reports on disk.
  testStatus = {};
  taskMap = {};
  renderTests();
  updateVcfBadge();
  fmStatus('', '');
  // If the user was looking at Reports or My Data when they switched
  // files, re-render so the new scope takes effect immediately.
  if (currentView() === 'reports') renderReportsView();
  if (currentView() === 'data') renderDataFiles();
}

async function deleteFile() {
  if (!activeFileId) { fmStatus('No file selected', 'error'); return; }
  const active = files.find(f => f.id === activeFileId);
  const name = active ? active.name : 'this file';
  if (!confirm(`Remove "${name}" from the list?\n\n` +
               `This will delete all reports for this file. ` +
               `If the file was uploaded here, it will also be deleted from disk.`)) return;
  const resp = await fetch(BASE + `/api/files/${activeFileId}`, { method: 'DELETE' });
  const data = await resp.json();
  if (!data.ok) {
    fmStatus(data.error || 'Delete failed', 'error');
    return;
  }
  testStatus = {};
  taskMap = {};
  await refreshFiles();
  renderTests();
  fmStatus(`Deleted "${name}"`, 'ok');
}

async function clearResults() {
  if (!activeFileId) { fmStatus('No file selected', 'error'); return; }
  if (!confirm('Clear all test results for this file? The file itself will stay.')) return;
  const resp = await fetch(BASE + `/api/files/${activeFileId}/clear-results`, { method: 'POST' });
  const data = await resp.json();
  if (!data.ok) {
    fmStatus(data.error || 'Clear failed', 'error');
    return;
  }
  testStatus = {};
  taskMap = {};
  renderTests();
  fmStatus(`Cleared ${data.removed} report(s)`, 'ok');
}

async function addPath() {
  const path = document.getElementById('pathInput').value.trim();
  if (!path) return;
  fmStatus('Adding…', '');
  const resp = await fetch(BASE + '/api/files/add-path', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path }),
  });
  const data = await resp.json();
  if (!data.ok) {
    fmStatus(data.error || 'Failed to add path', 'error');
    return;
  }
  document.getElementById('pathInput').value = '';
  activeFileId = data.file.id;
  testStatus = {};
  taskMap = {};
  await refreshFiles();
  renderTests();
  fmStatus(`Added ${data.file.name}`, 'ok');
}

async function addUrl() {
  const url = document.getElementById('urlInput').value.trim();
  if (!url) return;
  fmStatus('Downloading… (this can take a while for large files)', '');
  const resp = await fetch(BASE + '/api/files/add-url', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url }),
  });
  const data = await resp.json();
  if (!data.ok) {
    fmStatus(data.error || 'Download failed', 'error');
    return;
  }
  document.getElementById('urlInput').value = '';
  activeFileId = data.file.id;
  testStatus = {};
  taskMap = {};
  await refreshFiles();
  renderTests();
  fmStatus(`Downloaded and added ${data.file.name}`, 'ok');
}

function catSlug(cat) {
  return cat.replace(/[^a-zA-Z0-9]+/g, '-');
}

function categoryCounts(cat) {
  const c = { queued: 0, running: 0, passed: 0, warning: 0, failed: 0 };
  for (const t of tests) {
    if (t.category !== cat) continue;
    const st = (testStatus[t.id] || {}).status;
    if (st === 'queued') c.queued++;
    else if (st === 'running') c.running++;
    else if (st === 'passed' || st === 'completed') c.passed++;
    else if (st === 'warning') c.warning++;
    else if (st === 'failed') c.failed++;
  }
  return c;
}

function categoryCountsHtml(cat) {
  const c = categoryCounts(cat);
  const parts = [];
  if (c.running) parts.push(`<span class="cnt running">${c.running} running</span>`);
  if (c.queued)  parts.push(`<span class="cnt queued">${c.queued} queued</span>`);
  if (c.passed)  parts.push(`<span class="cnt passed">${c.passed} ✓</span>`);
  if (c.warning) parts.push(`<span class="cnt warning">${c.warning} ⚠</span>`);
  if (c.failed)  parts.push(`<span class="cnt failed">${c.failed} ✗</span>`);
  return parts.join('');
}

function updateCategoryHeader(cat) {
  const el = document.getElementById('cat-counts-' + catSlug(cat));
  if (el) el.innerHTML = categoryCountsHtml(cat);
}

function matchClass(rate) {
  if (rate == null) return '';
  if (rate >= 95) return 'match-green';
  if (rate >= 85) return 'match-yellow';
  return 'match-red';  // 60–85 range; <60 returns no_report so no chip
}

// ── PGS sub-category grouping ────────────────────────────────────
// Aliases collapse synonym variants into a single group label.
const PGS_TRAIT_ALIASES = {
  'cad': 'Coronary Artery Disease',
  'coronary artery disease': 'Coronary Artery Disease',
  'vte': 'Venous Thromboembolism',
  'venous thromboembolism': 'Venous Thromboembolism',
  'ibd': 'IBD',
  "ibd / crohn's / uc": 'IBD',
  'inflammatory bowel disease': 'IBD',
  "crohn's disease": 'IBD',
  'ulcerative colitis': 'IBD',
  'lupus': 'Lupus (SLE)',
  'lupus (sle)': 'Lupus (SLE)',
  'sle': 'Lupus (SLE)',
  'cll': 'CLL',
  'cll (lymphocytic leukemia)': 'CLL',
  'diastolic bp': 'Diastolic Blood Pressure',
  'diastolic blood pressure': 'Diastolic Blood Pressure',
  'systolic bp': 'Systolic Blood Pressure',
  'systolic blood pressure': 'Systolic Blood Pressure',
  'aortic aneurysm': 'Aortic Aneurysm',
  'ischemic stroke': 'Stroke',
  'stroke': 'Stroke',
  'serum testosterone levels': 'Testosterone',
  'testosterone': 'Testosterone',
  'hypertrophic cm': 'Cardiomyopathy',
  'dilated cardiomyopathy': 'Cardiomyopathy',
  'psoriatic arthropathy': 'Psoriatic Arthritis',
  'hdl': 'HDL',
  'hdl cholesterol': 'HDL',
  'ldl': 'LDL',
  'ldl cholesterol': 'LDL',
  'bmi': 'BMI',
  'bmi / obesity': 'BMI',
  'obesity': 'BMI',
  'kidney cancer': 'Kidney Cancer',
};

const PGS_SUBTYPE_PREFIX = /^(?:ER[\s\-]?positive|ER[\s\-]?negative|Triple[\s\-]?neg(?:ative)?|early[\s\-]?onset|aggressive|prognostic|severe|metastatic)\s+/i;

function pgsTraitGroup(trait) {
  if (!trait) return 'Other';
  let n = trait.trim();
  // Strip trailing parentheticals: "X (Y)" → "X"
  n = n.replace(/\s*\([^)]*\)\s*$/, '');
  // Strip trailing brackets: "X [Y]" → "X"
  n = n.replace(/\s*\[[^\]]*\]\s*$/, '');
  // Strip "in males/females" suffix
  n = n.replace(/\s+in\s+(?:males?|females?)\s*$/i, '');
  // Strip *qualifier* prefixes (subtypes that should fold into the parent)
  n = n.replace(PGS_SUBTYPE_PREFIX, '');
  // Aliases for known synonym sets
  const lc = n.toLowerCase();
  if (PGS_TRAIT_ALIASES[lc]) return PGS_TRAIT_ALIASES[lc];
  // Title-case while preserving short fully-capitalized acronyms
  return n.split(/\s+/).map(w => {
    if (!w) return '';
    if (/^[A-Z]{2,5}$/.test(w)) return w;          // ADHD, PTSD, NAFLD, HbA1c-ish
    if (w.includes('-')) {
      return w.split('-').map(p => p ? p[0].toUpperCase() + p.slice(1) : '').join('-');
    }
    return w[0].toUpperCase() + w.slice(1);
  }).join(' ');
}

function testGroupLabel(t) {
  // Only PGS-style tests get sub-grouping; everything else is null
  // (renders flat under its parent category).
  if (t.test_type === 'pgs_score' || t.test_type === 'rsid_pgs_score') {
    const p = t.params || {};
    return pgsTraitGroup(p.trait || p.title || '');
  }
  return null;
}

function groupSlug(s) {
  return (s || '').replace(/[^a-zA-Z0-9]+/g, '-');
}

function renderTests() {
  const container = document.getElementById('testsContainer');
  const search = document.getElementById('searchBox').value.toLowerCase();
  // Preserve which categories were already expanded so re-renders don't
  // collapse them.
  const wasOpen = new Set();
  document.querySelectorAll('.tests-body.open').forEach(b => wasOpen.add(b.dataset.cat));
  container.innerHTML = '';

  for (const cat of categories) {
    const catTests = tests.filter(t => t.category === cat &&
      (search === '' || t.name.toLowerCase().includes(search) || t.description.toLowerCase().includes(search) || t.category.toLowerCase().includes(search)));
    if (catTests.length === 0) continue;

    const slug = catSlug(cat);
    const div = document.createElement('div');
    div.className = 'category';
    div.id = 'cat-' + slug;
    const openCls = wasOpen.has(cat) ? ' open' : '';
    const arrow = wasOpen.has(cat) ? '&#9660;' : '&#9654;';

    // PGS-style categories get nested sub-sections by trait. Everything
    // else (Sample QC, Sex Check, Monogenic, Carrier Status, etc.)
    // renders flat — those rows are already grouped logically.
    let bodyContent = '';
    const pgsCategory = catTests.some(t => testGroupLabel(t));
    if (pgsCategory) {
      const groups = {};
      for (const t of catTests) {
        const g = testGroupLabel(t) || 'Other';
        (groups[g] = groups[g] || []).push(t);
      }
      const groupNames = Object.keys(groups).sort((a, b) => a.localeCompare(b));
      bodyContent = groupNames.map(name => `
        <div class="subcategory">
          <div class="subcategory-header">
            <span class="sub-name">${escapeHtml(name)}</span>
            <span class="sub-count">${groups[name].length}</span>
          </div>
          ${groups[name].map(t => renderTestRow(t)).join('')}
        </div>
      `).join('');
    } else {
      bodyContent = catTests.map(t => renderTestRow(t)).join('');
    }

    div.innerHTML = `
      <div class="category-header" onclick="toggleCategory(this)">
        <h2><span class="toggle">${arrow}</span> ${cat} <span class="cat-count">${catTests.length} tests</span>
          <span class="cat-counts" id="cat-counts-${slug}">${categoryCountsHtml(cat)}</span>
        </h2>
        <div class="cat-actions">
          <button class="cat-btn" onclick="event.stopPropagation(); runCategory('${cat.replace(/'/g, "\\'")}')">Run All</button>
        </div>
      </div>
      <div class="tests-body${openCls}" data-cat="${cat}">
        ${bodyContent}
      </div>
    `;
    container.appendChild(div);
  }
}

function renderTestRow(t) {
  const info = testStatus[t.id] || { status: 'idle' };
  const st = info.status;
  const headline = info.headline || (st === 'idle' ? '' : st);
  const error = info.error || '';
  const noReport = info.no_report === true;
  const hasReport = !noReport && ['passed', 'warning', 'failed', 'completed'].includes(st);
  const isRunning = st === 'running' || st === 'queued';
  const title = error ? error.replace(/"/g, '&quot;') : '';

  // PGS quality chip — only when match_rate_value is present and we
  // actually have a report (i.e. >= 60%).
  let chip = '';
  if (info.match_rate_value != null && !noReport) {
    chip = `<span class="match-chip ${matchClass(info.match_rate_value)}">match ${info.match_rate_value}%</span>`;
  }

  return `
    <div class="test-row" id="row-${t.id}">
      <div class="test-info">
        <h3>${t.name}</h3>
        <p>${t.description.substring(0, 120)}${t.description.length > 120 ? '...' : ''}</p>
      </div>
      <div class="test-status ${st}" title="${title}">
        <span class="status-dot ${st}"></span>
        <span class="headline">${escapeHtml(headline)}</span>
        ${chip}
      </div>
      <div>
        ${hasReport ? `<button class="clear-row-btn" onclick="clearSingleReport('${t.id}')" title="Delete this report so you can re-run">Clear</button>` : ''}
        ${hasReport ? `<button class="view-btn" onclick="viewReport('${t.id}')">View</button>` : ''}
        <button class="run-btn" onclick="runTest('${t.id}')" ${isRunning ? 'disabled' : ''}>Run</button>
      </div>
    </div>
  `;
}

function escapeHtml(s) {
  if (!s) return '';
  return String(s).replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function toggleCategory(header) {
  const body = header.nextElementSibling;
  body.classList.toggle('open');
  const toggle = header.querySelector('.toggle');
  toggle.innerHTML = body.classList.contains('open') ? '&#9660;' : '&#9654;';
}

function expandAll() {
  document.querySelectorAll('.tests-body').forEach(b => { b.classList.add('open'); });
  document.querySelectorAll('.toggle').forEach(t => { t.innerHTML = '&#9660;'; });
}

function collapseAll() {
  document.querySelectorAll('.tests-body').forEach(b => { b.classList.remove('open'); });
  document.querySelectorAll('.toggle').forEach(t => { t.innerHTML = '&#9654;'; });
}

function filterTests() { renderTests(); }

// Pick the list of file ids the next run should target. In normal mode
// it's a single-element list (so the existing per-row update logic
// continues to work for the active file's status). In All-files mode
// it's every registered file id.
function _runTargetFileIds() {
  if (isAllMode()) {
    if (!files.length) {
      alert('No files registered. Add at least one file in My Data first.');
      return [];
    }
    return files.map(f => f.id);
  }
  return [null];  // null = let the server use its active file
}

async function _postRun(url, fileId) {
  // Append ?file_id when explicit; backend treats missing param as
  // "use server-side active file".
  const fullUrl = fileId ? `${url}${url.includes('?') ? '&' : '?'}file_id=${encodeURIComponent(fileId)}` : url;
  const resp = await fetch(fullUrl, { method: 'POST' });
  return resp.json();
}

async function runTest(testId) {
  const targets = _runTargetFileIds();
  if (!targets.length) return;
  for (const fid of targets) {
    const data = await _postRun(`${BASE}/api/run/${testId}`, fid);
    if (!data.ok) { alert(data.error); continue; }
    // Only mirror the local row state when we ran against the file the
    // user is currently viewing.
    if (!fid || fid === activeFileId) {
      taskMap[testId] = data.task_id;
      testStatus[testId] = { status: 'queued', headline: 'queued' };
      updateRow(testId);
    }
  }
}

function expandCategory(cat) {
  const div = document.getElementById('cat-' + catSlug(cat));
  if (!div) return;
  const body = div.querySelector('.tests-body');
  const toggle = div.querySelector('.toggle');
  if (body && !body.classList.contains('open')) {
    body.classList.add('open');
    if (toggle) toggle.innerHTML = '&#9660;';
  }
}

async function runCategory(cat) {
  const targets = _runTargetFileIds();
  if (!targets.length) return;
  if (isAllMode() && !confirm(
        `Queue all "${cat}" tests against ${files.length} files? ` +
        `That's ${files.length} × N tasks.`)) return;
  for (const fid of targets) {
    const data = await _postRun(
      `${BASE}/api/run-category/${encodeURIComponent(cat)}`, fid);
    if (!data.ok) { alert(data.error); continue; }
    if (!fid || fid === activeFileId) {
      for (const tid of data.task_ids) {
        const testId = tid.split('_').slice(0, -1).join('_');
        taskMap[testId] = tid;
        testStatus[testId] = { status: 'queued', headline: 'queued' };
        updateRow(testId);
      }
      updateCategoryHeader(cat);
      expandCategory(cat);
    }
  }
}

async function runAll() {
  const targets = _runTargetFileIds();
  if (!targets.length) return;
  if (isAllMode()) {
    const total = tests.length * files.length;
    if (!confirm(
          `Queue every test against every file?\n\n` +
          `${tests.length} tests × ${files.length} files = ${total} tasks. ` +
          `This may take a while.`)) return;
  }
  for (const fid of targets) {
    const data = await _postRun(`${BASE}/api/run-all`, fid);
    if (!data.ok) { alert(data.error); continue; }
    if (!fid || fid === activeFileId) {
      for (const tid of data.task_ids) {
        const testId = tid.split('_').slice(0, -1).join('_');
        taskMap[testId] = tid;
        testStatus[testId] = { status: 'queued', headline: 'queued' };
        updateRow(testId);
      }
      for (const cat of categories) updateCategoryHeader(cat);
      expandAll();
    }
  }
}

async function clearQueue() {
  await fetch(BASE + '/api/clear-queue', { method: 'POST' });
}

async function clearSingleReport(testId) {
  const taskId = taskMap[testId];
  if (!taskId) return;
  const resp = await fetch(BASE + `/api/report/${taskId}`, { method: 'DELETE' });
  if (!resp.ok) {
    alert('Failed to clear report');
    return;
  }
  // Wipe local state so the row snaps back to idle; the next poll will
  // confirm the report is gone server-side.
  delete testStatus[testId];
  delete taskMap[testId];
  updateRow(testId);
}

// ── PGS Catalog search ──────────────────────────────────────────
let pgsSearchTimer = null;

function openPgsModal() {
  document.getElementById('pgsSearchModal').classList.add('open');
  const inp = document.getElementById('pgsSearchInput');
  inp.focus();
  if (inp.value.trim().length >= 2) pgsSearch();
}

function closePgsModal() {
  document.getElementById('pgsSearchModal').classList.remove('open');
}

function debouncedPgsSearch() {
  clearTimeout(pgsSearchTimer);
  pgsSearchTimer = setTimeout(pgsSearch, 400);
}

async function pgsSearch() {
  const q = document.getElementById('pgsSearchInput').value.trim();
  const statusEl = document.getElementById('pgsSearchStatus');
  const resultsEl = document.getElementById('pgsSearchResults');
  if (q.length < 2) {
    statusEl.textContent = 'Type at least 2 characters to search…';
    statusEl.className = 'pgs-search-status';
    resultsEl.innerHTML = '';
    return;
  }
  statusEl.textContent = 'Searching PGS Catalog…';
  statusEl.className = 'pgs-search-status';
  try {
    const resp = await fetch(BASE + `/api/pgs/search?q=${encodeURIComponent(q)}`);
    const data = await resp.json();
    if (data.error) {
      statusEl.textContent = data.error;
      statusEl.className = 'pgs-search-status error';
      resultsEl.innerHTML = '';
      return;
    }
    const results = data.results || [];
    statusEl.textContent = results.length === 0
      ? 'No matching scores found.'
      : `${results.length} result${results.length === 1 ? '' : 's'}${data.count > results.length ? ' (of ' + data.count + ')' : ''}`;
    if (results.length === 0) {
      resultsEl.innerHTML = '';
      return;
    }
    resultsEl.innerHTML = results.map(r => {
      const vars = (r.variants_number || 0).toLocaleString();
      const cite = [r.first_author, r.year].filter(Boolean).join(' ');
      const journal = r.journal ? ' · ' + escapeHtml(r.journal) : '';
      const title = escapeHtml(r.trait_reported || r.name || r.id);
      return `
        <div class="pgs-result" data-pgs-id="${r.id}">
          <div class="pgs-result-main">
            <div class="pgs-result-title">${title}<span class="pgs-result-id">${r.id}</span></div>
            <div class="pgs-result-meta">${vars} variants${cite ? ' · ' + escapeHtml(cite) : ''}${journal}</div>
          </div>
          <button class="add-pgs-btn" onclick="addPgs('${r.id}', this)" ${r.already_added ? 'disabled' : ''}>
            ${r.already_added ? 'Added' : '+ Add'}
          </button>
        </div>
      `;
    }).join('');
  } catch (e) {
    statusEl.textContent = 'Search failed: ' + e.message;
    statusEl.className = 'pgs-search-status error';
  }
}

async function addPgs(pgsId, btnEl) {
  if (btnEl) {
    btnEl.disabled = true;
    btnEl.textContent = 'Adding…';
  }
  try {
    const resp = await fetch(BASE + '/api/pgs/add', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pgs_id: pgsId }),
    });
    const data = await resp.json();
    if (!data.ok) {
      alert('Failed to add PGS: ' + (data.error || 'unknown'));
      if (btnEl) { btnEl.disabled = false; btnEl.textContent = '+ Add'; }
      return;
    }
    if (btnEl) btnEl.textContent = 'Added';
    await refreshTestList();
  } catch (e) {
    alert('Failed to add PGS: ' + e.message);
    if (btnEl) { btnEl.disabled = false; btnEl.textContent = '+ Add'; }
  }
}

async function refreshTestList() {
  // Re-fetch the test registry after the server adds a custom PGS.
  const resp = await fetch(BASE + '/api/tests');
  const data = await resp.json();
  tests = data.tests;
  categories = data.categories;
  renderTests();
}

document.getElementById('pgsSearchModal').addEventListener('click', function(e) {
  if (e.target === this) closePgsModal();
});
document.getElementById('pgsSearchInput').addEventListener('keydown', function(e) {
  if (e.key === 'Escape') closePgsModal();
});

function updateRow(testId) {
  const t = tests.find(x => x.id === testId);
  if (!t) return;
  const row = document.getElementById(`row-${testId}`);
  if (row) {
    row.outerHTML = renderTestRow(t);
  }
  updateCategoryHeader(t.category);
}

async function pollStatus() {
  try {
    const resp = await fetch(BASE + '/api/status');
    const data = await resp.json();

    const running = data.running_count != null
      ? data.running_count
      : (data.current_task ? 1 : 0);
    // Queue info now lives in the server stats bar (saving header
    // space). Cache the latest values + repaint just the chip.
    _lastQueueChip.queue_length = data.queue_length || 0;
    _lastQueueChip.running_count = running;
    _lastQueueChip.has_data = true;
    paintQueueChip();

    // Status is scoped server-side to the active file. Results keyed by
    // task_id; for the test-list view we only care about the most recent
    // task per test_id.
    const results = data.results || {};
    const latestPerTest = {};  // test_id -> {taskId, result}
    for (const [taskId, res] of Object.entries(results)) {
      const testId = res.test_id;
      if (!testId) continue;
      const completed = res.completed_at || res.queued_at || res.started_at || '';
      if (!latestPerTest[testId] ||
          (latestPerTest[testId].completed || '') < completed) {
        latestPerTest[testId] = { taskId, res, completed };
      }
    }

    // Work out which rows need a redraw (status or headline changed) AND
    // which previously-known rows have no results now (e.g. after clearing
    // or switching to a file with fewer runs).
    const seen = new Set();
    for (const [testId, { taskId, res }] of Object.entries(latestPerTest)) {
      seen.add(testId);
      const prev = testStatus[testId] || {};
      const newInfo = {
        status: res.status,
        headline: res.headline || (res.status === 'running' ? 'running…' :
                                   res.status === 'queued' ? 'queued' : ''),
        error: res.error,
        match_rate: res.match_rate,
        match_rate_value: res.match_rate_value,
        percentile: res.percentile,
        no_report: res.no_report === true,
      };
      taskMap[testId] = taskId;
      if (prev.status !== newInfo.status || prev.headline !== newInfo.headline ||
          prev.match_rate_value !== newInfo.match_rate_value ||
          prev.no_report !== newInfo.no_report) {
        testStatus[testId] = newInfo;
        updateRow(testId);
      }
    }
    // Drop any stale entries that no longer exist for this file.
    for (const testId of Object.keys(testStatus)) {
      if (!seen.has(testId)) {
        delete testStatus[testId];
        delete taskMap[testId];
        updateRow(testId);
      }
    }
  } catch (e) {}

  setTimeout(pollStatus, 2000);
}

async function openErrors() {
  const resp = await fetch(BASE + '/api/errors');
  const data = await resp.json();
  const modal = document.getElementById('reportModal');
  document.getElementById('modalTitle').textContent = `Error Log (${data.count} entries)`;
  document.getElementById('reportMeta').innerHTML = `
    <div class="meta-item"><label>Total errors</label><span>${data.count}</span></div>
    <div class="meta-item"><label>Showing</label><span>${data.errors.length}</span></div>
  `;
  const content = document.getElementById('reportContent');
  if (data.errors.length === 0) {
    content.textContent = 'No errors logged.';
  } else {
    content.textContent = data.errors.map(e =>
      `[${e.timestamp}]\n  ${e.test_name} (${e.test_id})\n  ${e.error}\n`
    ).join('\n');
  }
  modal.classList.add('open');
}

async function viewReport(testId) {
  const taskId = taskMap[testId];
  if (!taskId) return;
  const resp = await fetch(BASE + `/api/report/${taskId}`);
  const report = await resp.json();
  _openReportModal(report);
}

function closeModal() {
  document.getElementById('reportModal').classList.remove('open');
}

document.getElementById('reportModal').addEventListener('click', function(e) {
  if (e.target === this) closeModal();
});

// File upload via <input type="file">
document.getElementById('fileInput').addEventListener('change', async function(e) {
  const file = e.target.files[0];
  if (!file) return;
  fmStatus(`Uploading ${file.name}…`, '');

  const form = new FormData();
  form.append('file', file);

  try {
    const resp = await fetch(BASE + '/api/files/upload', { method: 'POST', body: form });
    const data = await resp.json();
    if (data.ok) {
      activeFileId = data.file.id;
      testStatus = {};
      taskMap = {};
      await refreshFiles();
      renderTests();
      fmStatus(`Uploaded ${data.file.name}`, 'ok');
    } else {
      fmStatus(data.error || 'Upload failed', 'error');
    }
  } catch (err) {
    fmStatus('Upload failed: ' + err.message, 'error');
  }
  // Allow the same file to be re-selected
  e.target.value = '';
});

// Enter key submits path / url inputs
document.getElementById('pathInput').addEventListener('keypress', function(e) {
  if (e.key === 'Enter') addPath();
});
document.getElementById('urlInput').addEventListener('keypress', function(e) {
  if (e.key === 'Enter') addUrl();
});

// ── Status bar: CPU/MEM/GPU + top processes ───────────────────
// Three levels: 0 = fully collapsed, 1 = chip row, 2 = chips + htop panel.
// Starts at 0 so the page loads clean.
let statusBarLevel = 0;

function metricColor(pct) {
  if (pct > 90) return '#f85149';
  if (pct > 70) return '#d29922';
  return '#3fb950';
}

function fmtMem(mb) {
  if (mb == null) return '';
  if (mb >= 1024) return (mb / 1024).toFixed(1) + 'G';
  return Math.round(mb) + 'M';
}

function procNameFromCommand(command) {
  if (!command) return '';
  // Strip leading "KEY=val KEY=val cmd"
  let cmd = command.replace(/^(\S+=\S+\s+)+/, '');
  const parts = cmd.split(/\s+/);
  let name = parts[0].split('/').pop();
  // Handle "env" prefix
  if (name === 'env' && parts.length > 1) {
    let i = 1;
    while (i < parts.length && parts[i].includes('=')) i++;
    if (i < parts.length) name = parts[i].split('/').pop();
  }
  name = name.replace(/^python\d[\d.]*/i, 'python')
             .replace(/^node\d[\d.]*/i, 'node')
             .replace(/^ruby\d[\d.]*/i, 'ruby');
  if (name === 'python' || name === 'node' || name === 'bash' || name === 'sh') {
    for (let i = 1; i < Math.min(parts.length, 5); i++) {
      const arg = parts[i];
      if (arg && !arg.startsWith('-') &&
          (arg.endsWith('.py') || arg.endsWith('.js') || arg.endsWith('.sh'))) {
        name = arg.split('/').pop();
        break;
      }
    }
  }
  return name;
}

const BIOTOOLS = ['samtools','bcftools','plink2','plink','bwa','minimap2',
                  'deepvariant','gatk','picard','fastqc','trimmomatic',
                  'bowtie','hisat'];
const RUNTIMES = ['python','node','uvicorn','gunicorn','npm','deno'];

function procColor(name) {
  const lc = (name || '').toLowerCase();
  if (lc.includes('claude') || lc.includes('anthropic')) return '#d2a8ff';
  if (BIOTOOLS.some(t => lc.includes(t))) return '#3fb950';
  if (RUNTIMES.some(t => lc.includes(t))) return '#58a6ff';
  if (lc.includes('singularity') || lc.includes('docker')) return '#d29922';
  return '#8b949e';
}

function shortCommand(cmd, max) {
  if (!cmd) return '';
  max = max || 120;
  let c = cmd.replace(/^(\S+=\S+\s+)+/, '');
  return c.length <= max ? c : c.slice(0, max - 1) + '…';
}

function aggregateProcs(processes) {
  const groups = {};
  for (const p of processes || []) {
    const n = procNameFromCommand(p.command);
    if (!n || n === 'ps' || n === 'top' || n === 'head') continue;
    if (!groups[n]) groups[n] = { name: n, count: 0, totalCpu: 0 };
    groups[n].count++;
    groups[n].totalCpu += p.cpu_pct || 0;
  }
  return Object.values(groups)
    .sort((a, b) => b.totalCpu - a.totalCpu)
    .slice(0, 6);
}

function setStatusLevel(n) {
  statusBarLevel = Math.max(0, Math.min(2, n));
  if (_lastSysStats) renderStatusBar(_lastSysStats);
  setTimeout(adjustTopPadding, 0);
}

function toggleStatusBar() {
  // Cycle level 1 ↔ 2; from level 0 go to 1.
  setStatusLevel(statusBarLevel >= 2 ? 1 : statusBarLevel + 1);
}

function renderStatusBar(stats) {
  const collapsed = document.getElementById('statusBarCollapsed');
  const inner = document.getElementById('statusBarInner');
  const panel = document.getElementById('statusBarTopPanel');

  // Level 0: only the thin collapsed strip is visible.
  if (statusBarLevel === 0) {
    collapsed.style.display = 'flex';
    inner.style.display = 'none';
    panel.style.display = 'none';
    return;
  }

  // Level 1+: hide the collapsed strip, show the chip row.
  collapsed.style.display = 'none';
  inner.style.display = 'flex';

  if (!stats) {
    inner.innerHTML = '<span class="status-bar-chip" style="color:#8b949e">Loading…</span>';
    panel.style.display = 'none';
    return;
  }

  const cpu = stats.cpu || {};
  const mem = stats.memory || {};
  const gpu = stats.gpu || {};
  const cpuPct = cpu.usage_pct || 0;
  const threads = cpu.threads || 0;
  const cpuUsed = threads > 0 ? Math.round((cpuPct / 100) * threads) : 0;
  const memUsed = mem.used_gb || 0;
  const memTotal = mem.total_gb || 0;
  const memPct = mem.usage_pct || 0;
  const load = (stats.load_avg || []).map(v => v.toFixed(2)).join(' ');

  let gpuChip = '';
  if (gpu.available && gpu.devices && gpu.devices.length) {
    const d = gpu.devices[0];
    const name = (d.name || 'GPU').replace(/NVIDIA /, '').replace(/GeForce /, '');
    const util = d.utilization_pct || 0;
    const tempC = d.temperature_c;
    const tempColor = tempC == null ? '#8b949e' : tempC > 80 ? '#f85149' : tempC > 60 ? '#d29922' : '#8b949e';
    gpuChip = `<span class="status-bar-chip">GPU ${escapeHtml(name)}
      <strong style="color:${metricColor(util)}">${util.toFixed(0)}%</strong>
      ${tempC != null ? `<span style="color:${tempColor}">${tempC}&deg;C</span>` : ''}
    </span>`;
  }

  const procGroups = aggregateProcs(stats.processes);
  const topOpen = statusBarLevel >= 2;

  inner.innerHTML = `
    <div class="status-bar-metrics">
      <span class="status-bar-chip">CPU
        <strong style="color:${metricColor(cpuPct)}">${cpuPct.toFixed(1)}%</strong>
        ${threads > 0 ? `<span style="color:#8b949e">${cpuUsed}/${threads}</span>` : ''}
      </span>
      <span class="status-bar-chip">MEM
        <strong style="color:${metricColor(memPct)}">${memUsed.toFixed(0)}/${memTotal.toFixed(0)}G</strong>
      </span>
      <span class="status-bar-chip">LOAD <strong style="color:#c9d1d9">${load}</strong></span>
      ${gpuChip}
      <span class="status-bar-chip" id="queueChipInStatus">${queueChipInner()}</span>
    </div>
    ${procGroups.length ? '<div class="status-bar-divider"></div>' : ''}
    <div class="status-bar-procs">
      ${procGroups.map(g => `
        <span class="status-bar-proc" style="color:${procColor(g.name)}">
          ${escapeHtml(g.name)}${g.count > 1 ? `<span class="status-bar-proc-count">&times;${g.count}</span>` : ''}
        </span>
      `).join('')}
    </div>
    <button type="button" class="status-bar-expand-btn${topOpen ? ' open' : ''}" onclick="setStatusLevel(${topOpen ? 1 : 2})">
      top<span class="arrow">&#9660;</span>
    </button>
    <button type="button" class="status-bar-close-btn" onclick="setStatusLevel(0)" title="Collapse server stats">&times;</button>
  `;

  // Level 2: htop process panel
  if (topOpen) {
    const top = (stats.processes || [])
      .filter(p => {
        const n = procNameFromCommand(p.command);
        return n && n !== 'ps' && n !== 'top' && n !== 'head';
      })
      .sort((a, b) => (b.cpu_pct || 0) - (a.cpu_pct || 0))
      .slice(0, 10);

    panel.innerHTML = `
      <div class="status-bar-top-header">
        <span class="col-pid">PID</span>
        <span class="col-user">USER</span>
        <span class="col-cpu">CPU%</span>
        <span class="col-mem">MEM%</span>
        <span class="col-res">RES</span>
        <span class="col-cmd">COMMAND</span>
      </div>
      ${top.length === 0
        ? '<div style="padding:12px 0;color:#8b949e">No process data.</div>'
        : top.map(p => {
            const name = procNameFromCommand(p.command);
            const rest = (p.command || '').replace(/^(\S+=\S+\s+)+/, '').split(/\s+/).slice(1).join(' ');
            return `
              <div class="status-bar-top-row">
                <span class="col-pid">${p.pid}</span>
                <span class="col-user">${escapeHtml(p.user || '')}</span>
                <span class="col-cpu" style="color:${metricColor(p.cpu_pct || 0)}">${(p.cpu_pct || 0).toFixed(1)}</span>
                <span class="col-mem" style="color:${metricColor(p.mem_pct || 0)}">${(p.mem_pct || 0).toFixed(1)}</span>
                <span class="col-res">${fmtMem(p.rss_mb)}</span>
                <span class="col-cmd" title="${escapeHtml(p.command || '')}">
                  <span class="proc-name" style="color:${procColor(name)}">${escapeHtml(name)}</span>
                  <span class="proc-args">${escapeHtml(shortCommand(rest, 200))}</span>
                </span>
              </div>
            `;
          }).join('')
      }
    `;
    panel.style.display = 'block';
  } else {
    panel.style.display = 'none';
  }
}

let _lastSysStats = null;

// Latest queue/running counts. Updated by pollStatus() (every 2 s) and
// painted into the QUEUE chip inside the server stats bar.
let _lastQueueChip = { queue_length: 0, running_count: 0, has_data: false };

function queueChipInner() {
  // Inner HTML of the QUEUE chip in the server stats bar.
  if (!_lastQueueChip.has_data) {
    return 'QUEUE <strong style="color:#8b949e">—</strong>';
  }
  const q = _lastQueueChip.queue_length || 0;
  const r = _lastQueueChip.running_count || 0;
  if (!q && !r) {
    return 'QUEUE <strong style="color:#8b949e">idle</strong>';
  }
  return 'QUEUE '
    + `<strong style="color:${r > 0 ? '#60a5fa' : '#c9d1d9'}">${q}</strong>`
    + (r > 0 ? ` <span style="color:#60a5fa">${r} running</span>` : '');
}

function paintQueueChip() {
  // Direct DOM update so we don't have to re-render the whole status
  // bar every 2 s. Falls back gracefully when the bar is collapsed.
  const el = document.getElementById('queueChipInStatus');
  if (el) el.innerHTML = queueChipInner();
}
async function pollSystemStats() {
  try {
    const resp = await fetch(BASE + '/api/system/stats');
    if (resp.ok) {
      _lastSysStats = await resp.json();
      renderStatusBar(_lastSysStats);
    }
  } catch (e) {}
  setTimeout(pollSystemStats, 5000);
}

// Esc steps the status bar down one level: 2→1→0.
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape' && statusBarLevel > 0) {
    setStatusLevel(statusBarLevel - 1);
  }
});

// ── Router (Tests / My Data / Reports / Chat) ──────────────────────
const VIEWS = ['tests', 'data', 'reports', 'chat'];

function currentView() {
  const h = (window.location.hash || '').replace(/^#\/?/, '');
  return VIEWS.includes(h) ? h : 'tests';
}

function showView(name) {
  if (!VIEWS.includes(name)) name = 'tests';
  for (const v of VIEWS) {
    const el = document.getElementById('view-' + v);
    if (el) el.classList.toggle('active', v === name);
  }
  document.querySelectorAll('#appNav a').forEach(a => {
    a.classList.toggle('active', a.dataset.view === name);
  });
  // Highlight My Data dropdown toggle when data view is active
  const ddToggle = document.getElementById('myDataToggle');
  if (ddToggle) ddToggle.classList.toggle('active', name === 'data');
  // Lazy-load data for views that fetch something
  if (name === 'reports') loadReports();
  if (name === 'data') renderDataFiles();
  // Start/stop the chat polling so we don't hammer tmux when the
  // user is on a different tab.
  if (name === 'chat') chatViewActivated();
  else chatViewDeactivated();
}

// ── My Data dropdown ────────────────────────────────────────
function toggleMyDataDropdown(e) {
  e.stopPropagation();
  document.getElementById('myDataDropdown').classList.toggle('open');
}
function closeMyDataDropdown() {
  document.getElementById('myDataDropdown').classList.remove('open');
}
document.addEventListener('click', function(e) {
  const dd = document.getElementById('myDataDropdown');
  if (dd && !dd.contains(e.target)) dd.classList.remove('open');
});

function applyRoute() {
  showView(currentView());
  adjustTopPadding();
}

window.addEventListener('hashchange', applyRoute);

// Offset the main container by the measured top-stack height so the
// fixed header+active-file+status-bar don't cover the content.
function adjustTopPadding() {
  const stack = document.getElementById('topStack');
  if (!stack) return;
  document.body.style.paddingTop = stack.offsetHeight + 'px';
}
window.addEventListener('resize', adjustTopPadding);
// adjustTopPadding is already called inside setStatusLevel() after
// every level change, so no extra wiring needed here.

// ── My Data view: registered files table ───────────────────────
function renderDataFiles() {
  const list = document.getElementById('dataFilesList');
  if (!list) return;
  if (!files.length) {
    list.innerHTML = '<div class="reports-empty">No files registered yet. Use the form above to add one.</div>';
    return;
  }
  const rows = files.map(f => {
    const isActive = f.id === activeFileId;
    const when = f.added_at ? new Date(f.added_at).toLocaleString() : '';
    return `
      <div class="data-files-row ${isActive ? 'active-row' : ''}">
        <div class="df-name" title="${escapeHtml(f.path || '')}">${escapeHtml(f.name || '')}</div>
        <div class="df-size">${formatSize(f.size)}</div>
        <div class="df-src">${escapeHtml(f.source || '')}</div>
        <div class="df-added">${escapeHtml(when)}</div>
        <div class="df-actions">
          <a class="file-btn" href="${BASE}/api/files/${f.id}/download" download="${escapeHtml(f.name || '')}" title="Download file">Download</a>
          <button class="file-btn" onclick="renameFile('${f.id}')" title="Rename this file (display name only)">Rename</button>
          <button class="danger-btn" onclick="deleteFileById('${f.id}')" title="Remove this file">Delete</button>
        </div>
      </div>
    `;
  }).join('');
  list.innerHTML = `
    <div class="data-files-table">
      <div class="data-files-row header">
        <div>Name</div>
        <div>Size</div>
        <div>Source</div>
        <div>Added</div>
        <div style="text-align:right">Actions</div>
      </div>
      ${rows}
    </div>
  `;
}

async function renameFile(fileId) {
  const f = files.find(x => x.id === fileId);
  if (!f) return;
  const newName = prompt('New display name for this file:', f.name || '');
  if (newName == null) return;  // cancelled
  const trimmed = newName.trim();
  if (!trimmed || trimmed === f.name) return;
  const resp = await fetch(BASE + `/api/files/${fileId}/rename`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: trimmed }),
  });
  const data = await resp.json();
  if (!data.ok) {
    alert('Rename failed: ' + (data.error || 'unknown'));
    return;
  }
  await refreshFiles();
  renderDataFiles();
  // If this was the active file, the reports view caches the file_name;
  // refresh it so the rename shows up there too.
  if (fileId === activeFileId && currentView() === 'reports') loadReports();
}

async function deleteFileById(fileId) {
  if (!confirm('Remove this file from the list? Uploaded files are also deleted from disk.')) return;
  await fetch(BASE + `/api/files/${fileId}`, { method: 'DELETE' });
  await refreshFiles();
  renderDataFiles();
}

// ── Reports view ─────────────────────────────────────────────────
let _allReports = [];
// Default sort: most recent first.
let _reportsSort = { key: 'completed_at', dir: 'desc' };

const REPORTS_COLUMNS = [
  { key: 'completed_at', label: 'DATE',     sortable: true,  type: 'string' },
  { key: 'category',     label: 'CATEGORY', sortable: true,  type: 'string' },
  { key: 'test_name',    label: 'RUNS',     sortable: true,  type: 'string' },
  { key: 'headline',     label: 'RESULT',   sortable: true,  type: 'string' },
  { key: 'match_rate_value', label: 'MATCH',     sortable: true, type: 'number' },
  { key: 'percentile',       label: '%ILE',      sortable: true, type: 'number' },
  { key: 'file_name',    label: 'FILE',     sortable: true,  type: 'string' },
  { key: null,           label: '',         sortable: false },
];

function matchTooltip(matchVal, status) {
  // Map a row's status + match rate to the human-readable explanation
  // shown when the user hovers the MATCH cell.
  if (matchVal == null) return '';
  const s = (status || '').toLowerCase();
  if (s === 'failed')  return 'Failed: match rate too low — PGS not computed';
  if (s === 'warning') return 'Warning: borderline accuracy — match rate below the safe threshold';
  if (matchVal >= 95)  return 'Pass: high match rate (≥95%)';
  if (matchVal >= 85)  return 'Pass: borderline accuracy (85–95%)';
  return 'Warning: borderline accuracy (60–85%)';
}

async function loadReports() {
  const list = document.getElementById('reportsList');
  const count = document.getElementById('reportsCount');
  list.innerHTML = '<div class="reports-empty">Loading…</div>';
  try {
    const resp = await fetch(BASE + '/api/reports?limit=1000');
    const data = await resp.json();
    _allReports = data.reports || [];
    count.textContent = `${_allReports.length} total`;
    renderReportsView();
  } catch (e) {
    list.innerHTML = `<div class="reports-empty">Failed to load reports: ${escapeHtml(e.message)}</div>`;
  }
}

function sortReportsBy(key) {
  if (!key) return;
  if (_reportsSort.key === key) {
    _reportsSort.dir = _reportsSort.dir === 'asc' ? 'desc' : 'asc';
  } else {
    // Numeric columns default to desc (high → low) on first click; the
    // date column also defaults to desc; everything else defaults to asc.
    const col = REPORTS_COLUMNS.find(c => c.key === key);
    _reportsSort.key = key;
    _reportsSort.dir = (col && col.type === 'number') || key === 'completed_at' ? 'desc' : 'asc';
  }
  renderReportsView();
}

function applyReportSort(rows) {
  const { key, dir } = _reportsSort;
  if (!key) return rows;
  const col = REPORTS_COLUMNS.find(c => c.key === key);
  const numeric = col && col.type === 'number';
  const mult = dir === 'asc' ? 1 : -1;
  return rows.slice().sort((a, b) => {
    let av = a[key];
    let bv = b[key];
    if (numeric) {
      av = av == null ? -Infinity : Number(av);
      bv = bv == null ? -Infinity : Number(bv);
      // For desc on numbers we still want missing values to sink to the
      // bottom rather than fly to the top. -Infinity * -1 = Infinity, so
      // missing rows come last for both directions.
      if (av === -Infinity && bv === -Infinity) return 0;
      if (av === -Infinity) return 1;
      if (bv === -Infinity) return -1;
      return (av - bv) * mult;
    }
    av = (av == null ? '' : String(av)).toLowerCase();
    bv = (bv == null ? '' : String(bv)).toLowerCase();
    if (av < bv) return -1 * mult;
    if (av > bv) return  1 * mult;
    return 0;
  });
}

function renderReportsView() {
  const list = document.getElementById('reportsList');
  const scopeEl = document.getElementById('reportsScope');
  if (!list) return;

  // Scope: active file filter. "All files" mode and "no selection"
  // both show every report.
  const scoped = (activeFileId && !isAllMode())
    ? _allReports.filter(r => r.file_id === activeFileId)
    : _allReports;

  const activeFile = files.find(f => f.id === activeFileId);
  if (scopeEl) {
    if (isAllMode()) {
      scopeEl.textContent = `Showing reports for every file (All files mode, ${_allReports.length} total).`;
    } else if (activeFile) {
      scopeEl.textContent = `Showing reports for ${activeFile.name} (${scoped.length} of ${_allReports.length}). Pick "All files" in the dropdown to see every report.`;
    } else {
      scopeEl.textContent = `Showing reports for every file (${_allReports.length} total).`;
    }
  }

  const q = (document.getElementById('reportsSearch').value || '').toLowerCase().trim();
  const filtered = !q ? scoped : scoped.filter(r =>
    (r.test_name || '').toLowerCase().includes(q) ||
    (r.category  || '').toLowerCase().includes(q) ||
    (r.file_name || '').toLowerCase().includes(q) ||
    (r.headline  || '').toLowerCase().includes(q)
  );

  const sorted = applyReportSort(filtered);

  // Header HTML with click-to-sort and active indicator
  const headerHtml = REPORTS_COLUMNS.map(c => {
    if (!c.sortable) return '<div></div>';
    const isActive = _reportsSort.key === c.key;
    const arrow = isActive ? (_reportsSort.dir === 'asc' ? '&#9650;' : '&#9660;') : '&#9662;';
    let tip;
    if (isActive) {
      const nextDir = _reportsSort.dir === 'asc' ? 'descending' : 'ascending';
      tip = `Click to sort ${nextDir}`;
    } else if (c.key === 'completed_at') {
      tip = 'Click to sort by date (newest first); click again for oldest first';
    } else {
      tip = `Click to sort by ${c.label.toLowerCase()}`;
    }
    return `<div class="${isActive ? 'sort-active' : ''}" title="${escapeHtml(tip)}" onclick="sortReportsBy('${c.key}')">${c.label}<span class="sort-arrow">${arrow}</span></div>`;
  }).join('');

  if (!sorted.length) {
    list.innerHTML = `
      <div class="reports-table">
        <div class="reports-header">${headerHtml}</div>
        <div class="reports-empty">No reports${q ? ' match "' + escapeHtml(q) + '"' : ''}.</div>
      </div>
    `;
    return;
  }

  const rows = sorted.map(r => {
    const dateStr = r.completed_at ? new Date(r.completed_at) : null;
    const dateOnly = dateStr ? dateStr.toISOString().slice(0, 10) : '';
    const timeOnly = dateStr ? dateStr.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '';
    const status = (r.status || 'passed').toLowerCase();

    // PGS quality cells. Hover tooltip on the match cell explains
    // pass / borderline / failed in plain language so the user doesn't
    // need to open the report to interpret the colour.
    const mr = r.match_rate_value;
    let matchHtml, pctHtml;
    if (mr != null) {
      const cls = matchClass(mr);
      const tip = matchTooltip(mr, status);
      matchHtml = `<div class="rep-match ${cls}" title="${escapeHtml(tip)}">${mr.toFixed(0)}%</div>`;
      pctHtml = (r.percentile != null)
        ? `<div class="rep-pct">${(typeof r.percentile === 'number' ? r.percentile.toFixed(0) : escapeHtml(r.percentile))}%</div>`
        : '<div class="rep-pct dim">—</div>';
    } else {
      // Non-PGS row: hover the dash to surface the row's overall status.
      const tip = status === 'failed'  ? 'Failed'
                : status === 'warning' ? 'Warning'
                : 'Passed';
      matchHtml = `<div class="rep-match match-none" title="${escapeHtml(tip)}">—</div>`;
      pctHtml = '<div class="rep-pct dim">—</div>';
    }

    return `
      <div class="reports-row">
        <div class="rep-when">
          <span class="date">${escapeHtml(dateOnly)}</span>
          <span class="time">${escapeHtml(timeOnly)}</span>
        </div>
        <div class="rep-cat">${escapeHtml(r.category || '')}</div>
        <div class="rep-test" title="${escapeHtml(r.test_name || '')}">${escapeHtml(r.test_name || '')}</div>
        <div class="rep-headline" title="${escapeHtml(r.headline || '')}">${escapeHtml(r.headline || '')}</div>
        ${matchHtml}
        ${pctHtml}
        <div class="rep-file" title="${escapeHtml(r.file_name || '')}">${escapeHtml(r.file_name || '')}</div>
        <div class="rep-actions">
          <button class="view-btn" onclick="viewReportByTaskId('${r.task_id}')">View</button>
          <a class="file-btn" href="${BASE}/api/report/${r.task_id}/download" download="${r.task_id}.json" title="Download this report">Download</a>
          <button class="danger-btn" onclick="deleteReportByTaskId('${r.task_id}')" title="Delete this report">Delete</button>
        </div>
      </div>
    `;
  }).join('');

  list.innerHTML = `
    <div class="reports-table">
      <div class="reports-header">${headerHtml}</div>
      ${rows}
    </div>
  `;
}

async function deleteReportByTaskId(taskId) {
  if (!confirm('Delete this report?')) return;
  const resp = await fetch(BASE + `/api/report/${taskId}`, { method: 'DELETE' });
  if (!resp.ok) { alert('Delete failed'); return; }
  // Drop from the cached list + re-render, no full reload needed.
  _allReports = _allReports.filter(r => r.task_id !== taskId);
  // Also wipe the matching testStatus entry so the row snaps back to idle
  // if the user's on the Tests view right now.
  for (const [testId, tid] of Object.entries(taskMap)) {
    if (tid === taskId) {
      delete testStatus[testId];
      delete taskMap[testId];
      updateRow(testId);
      break;
    }
  }
  document.getElementById('reportsCount').textContent = `${_allReports.length} total`;
  renderReportsView();
}

function downloadAllReports() {
  // Scope download to the active file if one is selected; otherwise
  // bundle every report on disk.
  const url = BASE + '/api/reports/download' + (activeFileId ? `?file_id=${activeFileId}` : '');
  window.location.href = url;
}

async function viewReportByTaskId(taskId) {
  // viewReport() uses taskMap[testId]; we need to call it directly by task_id.
  const resp = await fetch(BASE + `/api/report/${taskId}`);
  const report = await resp.json();
  _openReportModal(report);
}

// Extracted from viewReport() so both the row button and the reports
// list can open the same modal without duplicating the rendering logic.
function _openReportModal(report) {
  document.getElementById('modalTitle').textContent = report.test_name || 'Report';

  const result = report.result || {};
  const meta = document.getElementById('reportMeta');
  const metaItems = [
    `<div class="meta-item"><label>Category</label><span>${escapeHtml(report.category || '')}</span></div>`,
    `<div class="meta-item"><label>Duration</label><span>${report.elapsed_seconds || 0}s</span></div>`,
    `<div class="meta-item"><label>Completed</label><span>${report.completed_at ? new Date(report.completed_at).toLocaleString() : ''}</span></div>`,
    `<div class="meta-item"><label>VCF</label><span>${escapeHtml((report.vcf_path || '').split('/').pop())}</span></div>`,
  ];

  const mr = result.match_rate_value;
  if (mr != null) {
    const cls = matchClass(mr);
    metaItems.push(
      `<div class="meta-item ${cls}"><label>Match rate</label><span>${escapeHtml(result.match_rate || (mr + '%'))}</span></div>`
    );
    if (result.percentile != null) {
      metaItems.push(
        `<div class="meta-item"><label>Percentile (EUR)</label><span>${result.percentile}%</span></div>`
      );
    }
  }
  meta.innerHTML = metaItems.join('');

  const content = document.getElementById('reportContent');
  let text = '';
  if (result.no_report) {
    text = `PGS failed — match rate too low (${result.match_rate || 'unknown'}).\n\n` +
           `${result.matched_variants || 0}/${result.total_variants || result.n_variants || 0} variants matched.\n` +
           `No percentile computed because the score would not be reliable.\n`;
    content.textContent = text;
    document.getElementById('reportModal').classList.add('open');
    return;
  }
  if (result.summary) text += result.summary + '\n\n';
  if (result.apoe_status) {
    text += `APOE Genotype: ${result.apoe_status.genotype}\nRisk: ${result.apoe_status.risk}\n\n`;
  }
  if (result.variants) {
    text += '--- Variant Details ---\n';
    for (const v of result.variants) {
      text += `${v.gene} ${v.name} (${v.variant}): ${v.found ? v.genotype : 'Not found (ref/ref)'}\n`;
    }
    text += '\n';
  }
  if (result.findings && result.findings.length > 0) {
    text += '--- Pathogenic Findings ---\n';
    for (const f of result.findings) {
      text += `${f.gene}: ${f.chrom}:${f.pos} ${f.ref}>${f.alt} [${f.clnsig}] GT=${f.genotype}\n`;
    }
    text += '\n';
  }
  if (result.error) text += `ERROR: ${result.error}\n`;
  text += '\n--- Raw JSON ---\n' + JSON.stringify(result, null, 2);
  content.textContent = text;
  document.getElementById('reportModal').classList.add('open');
}

// ── Auth: who am I, and what to do if 401 ──────────────────────
async function fetchCurrentUser() {
  try {
    const resp = await fetch(BASE + '/api/auth/me');
    if (resp.status === 401) {
      window.location.href = BASE + '/login';
      return null;
    }
    if (!resp.ok) return null;
    return await resp.json();
  } catch (e) {
    return null;
  }
}

function showUserChip(username) {
  // The chip itself is a tiny green dot + Logout button — no username
  // in the visible UI. The full email is exposed only as a hover title
  // for the user's own confirmation.
  const chip = document.getElementById('userChip');
  if (!chip) return;
  if (username) {
    chip.title = `Signed in as ${username}`;
    chip.style.display = 'inline-flex';
  } else {
    chip.style.display = 'none';
  }
}

async function doLogout() {
  try {
    await fetch(BASE + '/api/auth/logout', { method: 'POST' });
  } catch (e) {}
  window.location.href = BASE + '/login';
}

(async function bootstrap() {
  // Auth gate FIRST. If we have no session, the redirect to /login fires
  // and the rest of bootstrap never runs.
  const me = await fetchCurrentUser();
  if (!me || !me.authenticated) {
    return;
  }
  showUserChip(me.username);

  await init();       // loads tests + files registry
  applyRoute();       // safe to render My Data / Reports now
  pollSystemStats();  // kick off the 5s poll
  setTimeout(adjustTopPadding, 100);
})();

// ─── AI Assistant tab ───────────────────────────────────────────────
// Polls /api/chat/status periodically while the chat tab is active.
// All state is intentionally module-local so the rest of the dashboard
// can ignore it.
const chatState = {
  messages: [],
  status: 'idle',
  detail: '',
  sessionExists: false,
  active: false,
  sub: 'chat',           // 'chat' | 'terminal'
  pollHandle: null,
  rawPollHandle: null,
  rawLines: 0,
  rawScrollAtBottom: true,
  msgScrollAtBottom: true,
  sending: false,
};

function chatEscape(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function chatRenderInline(text) {
  return chatEscape(text)
    .replace(/`([^`]+)`/g, '<code class="chat-inline-code">$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/\n/g, '<br/>');
}

function chatRenderMarkdown(text) {
  if (!text) return '';
  // Split on fenced code blocks ```...```
  const parts = text.split(/(```[\s\S]*?```)/g);
  return parts.map(part => {
    if (part.startsWith('```')) {
      const code = part.replace(/^```\w*\n?/, '').replace(/\n?```$/, '');
      return '<pre class="chat-code-block"><code>' + chatEscape(code) + '</code></pre>';
    }
    return chatRenderInline(part);
  }).join('');
}

function chatFormatTime(ts) {
  if (!ts) return '';
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function chatRenderMessages() {
  const root = document.getElementById('chatMessages');
  if (!root) return;
  if (!chatState.messages.length) {
    // Welcome stays visible by default; nothing to render.
    return;
  }
  const html = chatState.messages.map(m => {
    const body = m.role === 'assistant'
      ? chatRenderMarkdown(m.text)
      : chatEscape(m.text).replace(/\n/g, '<br/>');
    return '<div class="chat-bubble ' + m.role + '">'
      + '<div class="chat-bubble-content">' + body + '</div>'
      + '<div class="chat-bubble-time">' + chatFormatTime(m.ts) + '</div>'
      + '</div>';
  }).join('');
  let extra = '';
  if (chatState.status === 'busy') {
    const detail = chatState.detail || 'Working';
    extra = '<div class="chat-typing"><div class="typing-dots"><span></span><span></span><span></span></div><span>' + chatEscape(detail) + '…</span></div>';
  }
  root.innerHTML = html + extra;
  if (chatState.msgScrollAtBottom) {
    root.scrollTop = root.scrollHeight;
  }
}

function chatUpdateBadge() {
  const badge = document.getElementById('chatStatusBadge');
  const text = document.getElementById('chatStatusText');
  if (!badge || !text) return;
  badge.classList.remove('busy', 'idle', 'stopped', 'unknown');
  badge.classList.add(chatState.status || 'unknown');
  if (chatState.status === 'busy') {
    text.textContent = chatState.detail || 'Working…';
  } else if (chatState.status === 'idle') {
    text.textContent = 'Ready';
  } else if (chatState.status === 'stopped') {
    text.textContent = 'Session stopped';
  } else {
    text.textContent = chatState.detail || 'Unknown';
  }
  document.getElementById('chatStopBtn').style.display =
    chatState.status === 'busy' ? '' : 'none';
}

function chatMergeMessages(incoming) {
  if (!Array.isArray(incoming)) return;
  // Use role|ts as identity — server emits monotonic timestamps.
  const seen = new Set(chatState.messages.map(m => m.role + '|' + m.ts));
  let added = false;
  for (const m of incoming) {
    const key = m.role + '|' + m.ts;
    if (!seen.has(key)) {
      chatState.messages.push(m);
      seen.add(key);
      added = true;
    }
  }
  if (added) {
    chatState.messages.sort((a, b) => a.ts - b.ts);
  }
}

async function chatPollStatus() {
  try {
    const r = await fetch(BASE + '/api/chat/status');
    if (!r.ok) return;
    const data = await r.json();
    chatState.status = data.status || 'idle';
    chatState.detail = data.detail || '';
    chatState.sessionExists = !!data.session_exists;
    if (data.messages) chatMergeMessages(data.messages);
    chatUpdateBadge();
    chatRenderMessages();
  } catch (e) {
    // Network blip — leave state alone.
  }
}

async function chatSend() {
  if (chatState.sending) return;
  const input = document.getElementById('chatInput');
  const text = (input.value || '').trim();
  if (!text) return;
  chatState.sending = true;
  document.getElementById('chatSendBtn').disabled = true;
  input.value = '';
  input.style.height = 'auto';

  // Optimistically append the user message so it appears immediately.
  chatState.messages.push({ role: 'user', text, ts: Date.now() / 1000 });
  chatState.msgScrollAtBottom = true;
  chatRenderMessages();

  try {
    const r = await fetch(BASE + '/api/chat/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text }),
    });
    const data = await r.json();
    if (data && data.ok) {
      chatState.status = 'busy';
      chatUpdateBadge();
    } else {
      chatState.messages.push({
        role: 'assistant',
        text: 'Error: ' + (data && data.error ? data.error : 'send failed'),
        ts: Date.now() / 1000,
      });
      chatRenderMessages();
    }
  } catch (e) {
    chatState.messages.push({
      role: 'assistant',
      text: 'Error: ' + e.message,
      ts: Date.now() / 1000,
    });
    chatRenderMessages();
  } finally {
    chatState.sending = false;
    document.getElementById('chatSendBtn').disabled = false;
    // Force a fast follow-up poll so the assistant reply lands ASAP
    setTimeout(chatPollStatus, 1500);
  }
}

function chatInputKey(e) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    chatSend();
  }
}

function chatInputAutosize() {
  const ta = document.getElementById('chatInput');
  if (!ta) return;
  ta.style.height = 'auto';
  ta.style.height = Math.min(ta.scrollHeight, 150) + 'px';
}

async function chatInterrupt() {
  try { await fetch(BASE + '/api/chat/interrupt', { method: 'POST' }); } catch (e) {}
  setTimeout(chatPollStatus, 500);
}

async function chatRestart() {
  if (!confirm('Kill the AI session and start a new one?')) return;
  try { await fetch(BASE + '/api/chat/restart', { method: 'POST' }); } catch (e) {}
  chatState.messages = [];
  chatRenderMessages();
  setTimeout(chatPollStatus, 500);
}

async function chatClear() {
  if (!confirm('Clear chat history? (The Claude Code session will keep running.)')) return;
  try { await fetch(BASE + '/api/chat/clear', { method: 'POST' }); } catch (e) {}
  chatState.messages = [];
  chatRenderMessages();
}

// ── Terminal sub-tab ──
async function chatRawLoadFull() {
  try {
    const r = await fetch(BASE + '/api/chat/raw');
    if (!r.ok) return;
    const data = await r.json();
    const out = document.getElementById('chatRawOutput');
    if (!out) return;
    if (data.raw) {
      out.innerHTML = '<pre class="chat-raw-pre"></pre>';
      out.querySelector('pre').textContent = data.raw;
      out.scrollTop = out.scrollHeight;
    } else {
      out.innerHTML = '<div class="chat-raw-empty">'
        + (chatState.sessionExists ? 'No output yet.' : 'Session not running.')
        + '</div>';
    }
    chatState.rawLines = data.lines || 0;
  } catch (e) {}
}

async function chatRawPollTail() {
  try {
    const r = await fetch(BASE + '/api/chat/raw_tail?from_lines=' + chatState.rawLines);
    if (!r.ok) return;
    const data = await r.json();
    const out = document.getElementById('chatRawOutput');
    if (!out) return;
    if (data.mode === 'full') {
      out.innerHTML = '<pre class="chat-raw-pre"></pre>';
      out.querySelector('pre').textContent = data.raw || '';
    } else if (data.mode === 'delta' && data.raw) {
      let pre = out.querySelector('pre');
      if (!pre) {
        out.innerHTML = '<pre class="chat-raw-pre"></pre>';
        pre = out.querySelector('pre');
      }
      pre.textContent += data.raw;
    }
    chatState.rawLines = data.total_lines || chatState.rawLines;
    if (chatState.rawScrollAtBottom) out.scrollTop = out.scrollHeight;
  } catch (e) {}
}

async function chatRawSend() {
  const input = document.getElementById('chatRawInput');
  const cmd = (input.value || '').trim();
  if (!cmd) return;
  input.value = '';
  try {
    await fetch(BASE + '/api/chat/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: cmd }),
    });
  } catch (e) {}
  setTimeout(chatRawPollTail, 500);
}

function chatRawKey(e) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    chatRawSend();
  }
}

function chatSwitchTab(name) {
  chatState.sub = name;
  document.getElementById('chatSubChat').style.display = name === 'chat' ? '' : 'none';
  document.getElementById('chatSubTerminal').style.display = name === 'terminal' ? '' : 'none';
  document.getElementById('chatTabChat').classList.toggle('active', name === 'chat');
  document.getElementById('chatTabTerminal').classList.toggle('active', name === 'terminal');

  // Manage terminal polling
  if (name === 'terminal') {
    chatRawLoadFull();
    if (chatState.rawPollHandle) clearInterval(chatState.rawPollHandle);
    chatState.rawPollHandle = setInterval(chatRawPollTail, 1500);
  } else if (chatState.rawPollHandle) {
    clearInterval(chatState.rawPollHandle);
    chatState.rawPollHandle = null;
  }
}

function chatViewActivated() {
  if (chatState.active) return;
  chatState.active = true;
  // Wire scroll-tracking once
  const msgEl = document.getElementById('chatMessages');
  if (msgEl && !msgEl.dataset.scrollWired) {
    msgEl.addEventListener('scroll', () => {
      chatState.msgScrollAtBottom = (msgEl.scrollHeight - msgEl.scrollTop - msgEl.clientHeight) < 50;
    });
    msgEl.dataset.scrollWired = '1';
  }
  const rawEl = document.getElementById('chatRawOutput');
  if (rawEl && !rawEl.dataset.scrollWired) {
    rawEl.addEventListener('scroll', () => {
      chatState.rawScrollAtBottom = (rawEl.scrollHeight - rawEl.scrollTop - rawEl.clientHeight) < 50;
    });
    rawEl.dataset.scrollWired = '1';
  }
  // Default to the chat sub-tab on first activation
  if (!document.getElementById('chatTabChat').classList.contains('active')) {
    chatSwitchTab('chat');
  }
  // Seed messages from server
  fetch(BASE + '/api/chat/history').then(r => r.json()).then(data => {
    if (data && data.messages) {
      chatState.messages = data.messages.slice().sort((a, b) => a.ts - b.ts);
      chatRenderMessages();
    }
  }).catch(() => {});
  // Start polling
  chatPollStatus();
  if (chatState.pollHandle) clearInterval(chatState.pollHandle);
  chatState.pollHandle = setInterval(() => {
    chatPollStatus();
  }, chatState.status === 'busy' ? 2000 : 4000);
}

function chatViewDeactivated() {
  if (!chatState.active) return;
  chatState.active = false;
  if (chatState.pollHandle) { clearInterval(chatState.pollHandle); chatState.pollHandle = null; }
  if (chatState.rawPollHandle) { clearInterval(chatState.rawPollHandle); chatState.rawPollHandle = null; }
}
</script>
</body>
</html>"""


# ── Main ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
