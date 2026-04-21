"""
23 & Claude — lightweight genomic analysis dashboard.
Upload a VCF, pick tests from the checklist, run them sequentially.
"""

# Load .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
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
from threading import Thread, Lock, Semaphore

from cryptography.fernet import Fernet

from fastapi import FastAPI, File, UploadFile, Form, Request, HTTPException, Depends, Cookie
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse, RedirectResponse
import uvicorn

from test_registry import TESTS, TESTS_BY_ID, CATEGORIES
from runners import run_test

from google import genai

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
NUM_WORKERS = int(os.getenv("SIMPLE_GENOMICS_WORKERS", "8"))

PGS_CATALOG_API = "https://www.pgscatalog.org/rest"

# ── PGS Enrichment data (persists across restarts) ──
PGS_ENRICHMENT_FILE = SG_DATA_ROOT / "pgs_enrichment.json"
_pgs_enrichment_lock = Lock()
_pgs_refresh_status = {}  # category -> {status, progress, total, errors}

def _load_pgs_enrichment() -> dict:
    """Load PGS enrichment data from JSON. Returns {pgs_id: {...metadata...}}."""
    if PGS_ENRICHMENT_FILE.exists():
        try:
            return json.loads(PGS_ENRICHMENT_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}

def _save_pgs_enrichment(data: dict) -> None:
    """Save PGS enrichment data to JSON file."""
    PGS_ENRICHMENT_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))

def _fetch_pgs_enrichment(pgs_id: str) -> dict:
    """Fetch enrichment metadata for a PGS ID from the PGS Catalog REST API."""
    data = _pgs_catalog_get(f"/score/{pgs_id}")

    pub = data.get("publication") or {}
    ancestry = data.get("ancestry_distribution") or {}
    gwas = ancestry.get("gwas") or {}
    gwas_dist = gwas.get("dist") or {}
    gwas_n = gwas.get("count")

    # Citation
    author = pub.get("firstauthor", "")
    journal = pub.get("journal", "")
    year = (pub.get("date_publication") or "")[:4]
    citation = f"{author} et al., {journal} ({year})" if author else ""

    # DOI
    doi = pub.get("doi")
    doi_url = f"https://doi.org/{doi}" if doi else None

    # Genome build
    ftp = data.get("ftp_harmonized_scoring_files") or {}
    builds = []
    if ftp.get("GRCh37"):
        builds.append("GRCh37")
    if ftp.get("GRCh38"):
        builds.append("GRCh38")

    # Trait description from EFO
    trait_efo_list = data.get("trait_efo") or []
    efo_description = ""
    if trait_efo_list:
        efo_description = trait_efo_list[0].get("description", "")

    # Weight type and method
    weight_type = data.get("weight_type") or "NR"
    method_name = data.get("method_name") or ""

    # Ancestry formatting
    ancestry_parts = []
    for pop, pct in sorted(gwas_dist.items(), key=lambda x: -x[1]):
        ancestry_parts.append(f"{pop}: {pct}%")
    gwas_ancestry_str = ", ".join(ancestry_parts) if ancestry_parts else "Not reported"

    return {
        "pgs_id": pgs_id,
        "trait": data.get("trait_reported", ""),
        "variants": data.get("variants_number"),
        "citation": citation,
        "doi": doi_url,
        "pub_title": pub.get("title", ""),
        "pub_pmid": pub.get("PMID"),
        "genome_build": data.get("original_genome_build") or (builds[0] if builds else "NR"),
        "builds_available": builds,
        "weight_type": weight_type,
        "method_name": method_name,
        "trait_description": efo_description,
        "gwas_ancestry": gwas_ancestry_str,
        "gwas_n": gwas_n,
        "last_refreshed": datetime.now(timezone.utc).isoformat(),
    }

def _run_pgs_refresh(category: str) -> None:
    """Background thread: refresh all PGS entries in a category."""
    try:
        pgs_tests = [t for t in TESTS if t["category"] == category and t.get("test_type") == "pgs_score"]
        total = len(pgs_tests)
        if total == 0:
            _pgs_refresh_status[category] = {"status": "error", "error": "No PGS tests in category"}
            return

        errors = []
        _pgs_refresh_status[category] = {"status": "running", "progress": 0, "total": total, "errors": []}

        for i, test in enumerate(pgs_tests):
            pgs_id = test["params"].get("pgs_id", "")
            if not pgs_id:
                errors.append(f"{test['id']}: no pgs_id in params")
                _pgs_refresh_status[category] = {"status": "running", "progress": i + 1, "total": total, "errors": errors}
                continue
            try:
                fetched = _fetch_pgs_enrichment(pgs_id)

                # QA: compare with existing enrichment
                with _pgs_enrichment_lock:
                    enrichment = _load_pgs_enrichment()
                    existing = enrichment.get(pgs_id, {})
                    qa_notes = []
                    if existing.get("variants") and fetched["variants"]:
                        if existing["variants"] != fetched["variants"]:
                            qa_notes.append(f"Variant count changed: {existing['variants']} -> {fetched['variants']}")
                    if existing.get("trait") and fetched["trait"]:
                        if existing["trait"] != fetched["trait"]:
                            qa_notes.append(f"Trait name changed: '{existing['trait']}' -> '{fetched['trait']}'")

                    if qa_notes:
                        fetched["qa_notes"] = qa_notes
                    enrichment[pgs_id] = fetched
                    _save_pgs_enrichment(enrichment)

                logger.info(f"PGS enrichment: refreshed {pgs_id} ({fetched.get('trait', '?')})")

            except Exception as exc:
                error_msg = f"{pgs_id}: {str(exc)}"
                errors.append(error_msg)
                logger.warning(f"PGS enrichment: failed {pgs_id}: {exc}")

            _pgs_refresh_status[category] = {"status": "running", "progress": i + 1, "total": total, "errors": errors}
            time.sleep(0.4)  # Polite crawl rate

        _pgs_refresh_status[category] = {
            "status": "completed",
            "progress": total,
            "total": total,
            "errors": errors,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        logger.exception(f"PGS enrichment refresh failed for {category}")
        _pgs_refresh_status[category] = {"status": "error", "error": str(exc)}



# ── Gemini (Vertex AI) for LLM interpretation of test results ──
try:
    _gemini_client = genai.Client(
        vertexai=True, project="nimo-gpt", location="us-central1"
    )
    logger.info("Gemini (Vertex AI) client initialised")
except Exception as _ge:
    _gemini_client = None
    logger.warning(f"Gemini client init failed — interpretations disabled: {_ge}")

# Rate-limit concurrent Gemini calls to avoid 429 RESOURCE_EXHAUSTED
_gemini_semaphore = Semaphore(2)  # max 2 concurrent LLM calls

# Fallback Anthropic API key for when Gemini hits rate limits
_FALLBACK_ANTHROPIC_KEY = os.getenv("FALLBACK_ANTHROPIC_KEY", "")


DEFAULT_USER_USERNAME = os.getenv("SG_DEFAULT_USER", "admin@example.com")
DEFAULT_USER_PASSWORD = os.getenv("SG_DEFAULT_PASSWORD", "changeme123")

SESSION_COOKIE = "sg_session"
SESSION_TTL_SECONDS = 60 * 60 * 24 * 30  # 30 days

# ── Fernet encryption for API keys at rest ───────────────────────
_FERNET_KEY_FILE = SG_DATA_ROOT / ".fernet_key"


def _get_fernet() -> Fernet:
    if _FERNET_KEY_FILE.exists():
        key = _FERNET_KEY_FILE.read_bytes().strip()
    else:
        key = Fernet.generate_key()
        fd = os.open(str(_FERNET_KEY_FILE), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(key)
    return Fernet(key)


_fernet = _get_fernet()


def _encrypt_api_key(plaintext: str) -> str:
    return _fernet.encrypt(plaintext.encode("utf-8")).decode("ascii")


def _decrypt_api_key(ciphertext: str) -> str:
    return _fernet.decrypt(ciphertext.encode("ascii")).decode("utf-8")


def _mask_api_key(key: str) -> str:
    if len(key) <= 12:
        return key[:4] + "..." + key[-4:]
    return key[:10] + "..." + key[-4:]


def _set_user_api_key(username: str, api_key: str):
    uname = _norm_username(username)
    with users_lock:
        if uname not in users_state:
            return
        users_state[uname]["api_key_enc"] = _encrypt_api_key(api_key)
        _save_users()


def _get_user_api_key(username: str) -> str | None:
    uname = _norm_username(username)
    with users_lock:
        rec = users_state.get(uname, {})
    enc = rec.get("api_key_enc")
    if not enc:
        return None
    try:
        return _decrypt_api_key(enc)
    except Exception:
        logger.warning(f"Failed to decrypt API key for {uname}")
        return None


def _has_user_api_key(username: str) -> bool:
    return _get_user_api_key(username) is not None


def _remove_user_api_key(username: str):
    uname = _norm_username(username)
    with users_lock:
        if uname in users_state:
            users_state[uname].pop("api_key_enc", None)
            _save_users()


# ── Per-user settings (interpretation model, provider keys) ───────
def _user_settings_path(username: str) -> Path:
    return user_dir(username) / "settings.json"

def _load_user_settings(username: str) -> dict:
    p = _user_settings_path(username)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return {}

def _save_user_settings(username: str, settings: dict):
    p = _user_settings_path(username)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(settings, indent=2))
    tmp.replace(p)

def _set_provider_key(username: str, provider: str, key: str):
    settings = _load_user_settings(username)
    settings.setdefault("provider_keys", {})
    settings["provider_keys"][provider] = _encrypt_api_key(key)
    _save_user_settings(username, settings)

def _get_provider_key(username: str, provider: str) -> str | None:
    settings = _load_user_settings(username)
    enc = settings.get("provider_keys", {}).get(provider)
    if not enc:
        return None
    try:
        return _decrypt_api_key(enc)
    except Exception:
        return None

def _remove_provider_key(username: str, provider: str):
    settings = _load_user_settings(username)
    settings.get("provider_keys", {}).pop(provider, None)
    _save_user_settings(username, settings)

def _get_interp_model(username: str) -> str:
    settings = _load_user_settings(username)
    return settings.get("interp_model", "gemini")

def _set_interp_model(username: str, model: str):
    if model not in ("gemini", "openai", "claude"):
        return
    settings = _load_user_settings(username)
    settings["interp_model"] = model
    _save_user_settings(username, settings)




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
            pgen_status = _check_pgen_ready(path_str)
            entry = {
                "id": fid,
                "name": name or Path(path_str).name,
                "path": path_str,
                "source": source,
                "added_at": datetime.now(timezone.utc).isoformat(),
                "size": size,
                "pgen_status": pgen_status,
            }
            ctx.files_state["files"][fid] = entry
        else:
            entry = ctx.files_state["files"][fid]
        if select or ctx.files_state["active_file_id"] is None:
            ctx.files_state["active_file_id"] = fid
        ctx.save_files()
    # Auto-trigger pgen build if needed (non-blocking background thread)
    if entry.get("pgen_status") == "pending":
        _trigger_pgen_build(username, fid, path_str)
    return entry



# ── Pgen readiness ────────────────────────────────────────────────
# A file is "pgen-ready" when its per-file pgen cache is built (needed
# for PGS scoring and PCA). Non-gVCF files build instantly; gVCFs take
# minutes on the first build but are cached permanently afterward.

# In-memory dict tracking active builds: file_id -> "building"
_pgen_build_status = {}  # file_id -> "building" | "ready" | "failed"


def _check_pgen_ready(path):
    """Check if the pgen cache already exists for this file.
    Returns 'ready', 'not_needed', or 'pending'."""
    from runners import _is_gvcf, _pgen_cache_key, PGEN_CACHE
    path_str = str(path)

    if not os.path.exists(path_str):
        return "not_needed"

    # Determine if this file type needs pgen at all
    ftype = path_str.lower()
    if ftype.endswith(('.bam', '.cram')):
        return "not_needed"  # BAM/CRAM files use variant calling, not pgen

    # Check if the PGS-style pgen cache exists
    key = _pgen_cache_key(path_str, "chr@:#", None)
    cache_dir = os.path.join(PGEN_CACHE, key)
    prefix = os.path.join(cache_dir, "sample")
    stamp = os.path.join(cache_dir, ".vcf_mtime")

    if (os.path.exists(prefix + ".pgen") and
        os.path.exists(prefix + ".pvar") and
        os.path.exists(prefix + ".psam") and
        os.path.exists(stamp)):
        try:
            vcf_mtime = os.path.getmtime(path_str)
            with open(stamp) as f:
                cached = float(f.read().strip())
            if abs(cached - vcf_mtime) < 1e-6:
                return "ready"
        except (OSError, ValueError):
            pass
    return "pending"


def _trigger_pgen_build(username, file_id, path):
    """Kick off a background pgen build for this file.
    Runs in a daemon thread so it doesn't block the request."""
    from runners import _get_or_build_pgen, _is_gvcf

    def _build():
        try:
            _pgen_build_status[file_id] = "building"
            logger.info(f"[pgen-prep] Starting pgen build for {file_id}: {path}")
            _get_or_build_pgen(path)
            _pgen_build_status[file_id] = "ready"
            # Update file entry
            ctx = get_user_state(username)
            with ctx.lock:
                entry = ctx.files_state["files"].get(file_id)
                if entry:
                    entry["pgen_status"] = "ready"
                    ctx.save_files()
            logger.info(f"[pgen-prep] Pgen cache built for {file_id}")
        except Exception as e:
            _pgen_build_status[file_id] = "failed"
            logger.error(f"[pgen-prep] Pgen build failed for {file_id}: {e}", exc_info=True)
            import sys; print(f"[pgen-prep] FAILED {file_id}: {e}", file=sys.stderr, flush=True)

    t = Thread(target=_build, daemon=True, name=f"pgen-build-{file_id[:8]}")
    t.start()


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


# ── LLM Interpretation ────────────────────────────────────────────
def _interpret_result(test_def: dict, result: dict, username: str = None) -> tuple[str | None, str | None]:
    """Ask an LLM to explain a test result in plain English.
    Returns (interpretation_text, error_message).
    Uses semaphore to limit concurrency and retries on transient errors."""

    # Determine which model to use
    model_choice = "gemini"
    if username:
        model_choice = _get_interp_model(username)

    # Build a concise snapshot for the LLM
    result_snapshot = {
        k: v for k, v in result.items()
        if k not in ("raw_json", "debug", "pipeline_info", "scoring_diagnostics") and v is not None
    }
    prompt = (
        "You are a clinical genomics specialist writing a brief, matter-of-fact "
        "interpretation of a genetic test result. Do NOT greet the reader or "
        "use filler phrases like 'Hello', 'Let's go over', 'I'm happy to'. "
        "Jump straight into the science.\n\n"
        f"Test: {test_def['name']}\n"
        f"Category: {test_def['category']}\n"
        f"Description: {test_def['description']}\n\n"
        f"Result:\n{json.dumps(result_snapshot, indent=2, default=str)}\n\n"
        "Write 3-5 concise sentences covering:\n"
        "1. What was tested and its clinical relevance\n"
        "2. What the specific results indicate for this individual\n"
        "3. Clinical significance (carrier status, risk level, drug "
        "response, actionable findings)\n\n"
        "Be direct and scientifically precise but accessible to a non-specialist. "
        "No greetings, no fluff."
    )

    max_retries = 3

    if model_choice == "gemini":
        if _gemini_client is None:
            return None, "Gemini client not available"
        for attempt in range(max_retries):
            _gemini_semaphore.acquire()
            try:
                resp = _gemini_client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                    config={"httpOptions": {"timeout": 30_000}},
                )
                text = resp.text.strip() if resp.text else None
                return text, None
            except Exception as exc:
                exc_str = str(exc)
                is_rate_limit = "429" in exc_str or "RESOURCE_EXHAUSTED" in exc_str
                if is_rate_limit and attempt < max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.info(f"Gemini 429 for {test_def['name']}, retry {attempt+1}/{max_retries} in {wait}s")
                    time.sleep(wait)
                    continue
                err_msg = f"Gemini failed after {attempt+1} attempts: {exc_str[:120]}"
                logger.warning(f"LLM interpretation failed for {test_def['name']}: {err_msg}")
                return None, err_msg
            finally:
                _gemini_semaphore.release()
        # Fallback to Claude if Gemini rate-limited
        if _FALLBACK_ANTHROPIC_KEY:
            logger.info(f"Gemini exhausted for {test_def['name']}, falling back to Claude")
            return _interpret_fallback_claude(prompt, test_def)
        return None, "Gemini: max retries exhausted"

    elif model_choice == "openai":
        api_key = _get_provider_key(username, "openai") if username else None
        if not api_key:
            return None, "OpenAI API key not configured. Set it in Settings."
        import urllib.request as urllib_req
        for attempt in range(max_retries):
            _gemini_semaphore.acquire()
            try:
                payload = json.dumps({
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 500,
                    "temperature": 0.3,
                }).encode()
                req = urllib_req.Request(
                    "https://api.openai.com/v1/chat/completions",
                    data=payload,
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                )
                with urllib_req.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read())
                    text = data["choices"][0]["message"]["content"].strip()
                    return text, None
            except Exception as exc:
                exc_str = str(exc)
                is_rate_limit = "429" in exc_str or "rate" in exc_str.lower()
                if is_rate_limit and attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                err_msg = f"OpenAI failed after {attempt+1} attempts: {exc_str[:120]}"
                logger.warning(f"LLM interpretation failed for {test_def['name']}: {err_msg}")
                return None, err_msg
            finally:
                _gemini_semaphore.release()
        return None, "OpenAI: max retries exhausted"

    elif model_choice == "claude":
        api_key = _get_provider_key(username, "claude") if username else None
        if not api_key and username:
            api_key = _get_user_api_key(username)
        if not api_key:
            return None, "Anthropic API key not configured. Set it in Settings."
        import urllib.request as urllib_req
        for attempt in range(max_retries):
            _gemini_semaphore.acquire()
            try:
                payload = json.dumps({
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 500,
                    "messages": [{"role": "user", "content": prompt}],
                }).encode()
                req = urllib_req.Request(
                    "https://api.anthropic.com/v1/messages",
                    data=payload,
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "Content-Type": "application/json",
                    },
                )
                with urllib_req.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read())
                    text = data["content"][0]["text"].strip()
                    return text, None
            except Exception as exc:
                exc_str = str(exc)
                is_rate_limit = "429" in exc_str or "rate" in exc_str.lower()
                if is_rate_limit and attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                err_msg = f"Claude failed after {attempt+1} attempts: {exc_str[:120]}"
                logger.warning(f"LLM interpretation failed for {test_def['name']}: {err_msg}")
                return None, err_msg
            finally:
                _gemini_semaphore.release()
        return None, "Claude: max retries exhausted"

    return None, f"Unknown model: {model_choice}"




def _interpret_fallback_claude(prompt: str, test_def: dict) -> tuple:
    """Fallback LLM interpretation via Anthropic Claude when primary model is rate-limited."""
    import urllib.request as urllib_req
    try:
        payload = json.dumps({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": prompt}],
        }).encode()
        req = urllib_req.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "x-api-key": _FALLBACK_ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
        )
        with urllib_req.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            text = data["content"][0]["text"].strip()
            return text, None
    except Exception as exc:
        err_msg = f"Claude fallback also failed: {str(exc)[:120]}"
        logger.warning(f"LLM interpretation fallback failed for {test_def['name']}: {err_msg}")
        return None, err_msg


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

            def progress_cb(step_msg):
                """Update the in-flight task headline so the frontend can show progress."""
                task_results[task_id]["headline"] = step_msg

            result = run_test(vcf_path, test_def, progress_cb=progress_cb)
            elapsed = time.time() - start

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

            # LLM interpretation — skip for failed tests
            interpretation = None
            interp_error = None
            if task_outcome != "failed":
                interpretation, interp_error = _interpret_result(test_def, result, username=username)

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
                "interpretation": interpretation,
                "interpretation_error": interp_error,
                "elapsed_seconds": round(elapsed, 1),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }

            # Persist genome build to file entry if detected
            detected_build = result.get("genome_build")
            if detected_build and task_outcome != "failed":
                try:
                    ctx = get_user_state(username)
                    with ctx.lock:
                        fentry = ctx.files_state["files"].get(file_id)
                        if fentry and not fentry.get("genome_build"):
                            fentry["genome_build"] = detected_build
                            ctx.save_files()
                except Exception:
                    pass

            report_path = _user_report_dir(username, file_id) / f"{task_id}.json"
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)

            task_results[task_id] = {
                "status": task_outcome,
                "test_id": test_id,
                "test_name": test_def["name"],
                "file_id": file_id,
                "username": username,
                "headline": headline,
                "error": error_msg,
                "interpretation": interpretation,
                "interpretation_error": interp_error,
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


_file_meta_cache: dict = {}  # path -> {mtime, meta}
_BCFTOOLS = os.getenv("BCFTOOLS", "/home/nimo/miniconda3/envs/genomics/bin/bcftools")


def _probe_file_metadata(path):
    """Extract comprehensive metadata from a genomic file header.

    Returns dict with: genome_build, chr_naming, sample_name, platform,
    aligner, variant_caller, indexed, variant_count, n_contigs.
    Results are cached by path+mtime.
    """
    try:
        mtime = os.path.getmtime(path)
        cached = _file_meta_cache.get(path)
        if cached and cached.get("mtime") == mtime:
            return cached["meta"]
    except Exception:
        mtime = 0

    meta = {}
    p_lower = path.lower()

    try:
        if p_lower.endswith(('.bam', '.cram')):
            # --- BAM/CRAM: parse full header ---
            r = subprocess.run(['samtools', 'view', '-H', str(path)],
                               capture_output=True, text=True, timeout=30)
            if r.returncode != 0:
                return meta
            header = r.stdout

            # Index check
            meta["indexed"] = any(
                os.path.exists(path + ext) for ext in ('.bai', '.crai', '.csi')
            )

            n_contigs = 0
            for line in header.split('\n'):
                if line.startswith('@SQ'):
                    n_contigs += 1
                    fields = dict(f.split(':', 1) for f in line.split('\t')[1:] if ':' in f)
                    sn = fields.get('SN', '')
                    # Chr naming from first contig
                    if 'chr_naming' not in meta:
                        meta['chr_naming'] = 'chr' if sn.startswith('chr') else 'numeric'
                    # Build from chr1 length
                    if sn in ('chr1', '1') and 'genome_build' not in meta:
                        ln = int(fields.get('LN', 0))
                        if ln == 248956422: meta['genome_build'] = 'GRCh38'
                        elif ln == 249250621: meta['genome_build'] = 'GRCh37'
                        elif ln == 247249719: meta['genome_build'] = 'hg18'

                elif line.startswith('@RG'):
                    fields = dict(f.split(':', 1) for f in line.split('\t')[1:] if ':' in f)
                    if 'SM' in fields and 'sample_name' not in meta:
                        meta['sample_name'] = fields['SM']
                    if 'PL' in fields and 'platform' not in meta:
                        meta['platform'] = fields['PL'].upper()
                    if 'CN' in fields and 'center' not in meta:
                        meta['center'] = fields['CN']
                    if 'LB' in fields and 'library' not in meta:
                        meta['library'] = fields['LB']

                elif line.startswith('@PG'):
                    fields = dict(f.split(':', 1) for f in line.split('\t')[1:] if ':' in f)
                    prog = (fields.get('PN', '') or fields.get('ID', '')).lower()
                    if any(x in prog for x in ('bwa', 'bowtie', 'minimap', 'dragen', 'star', 'hisat')):
                        aligner = fields.get('PN', fields.get('ID', ''))
                        if 'VN' in fields:
                            aligner += ' ' + fields['VN']
                        meta['aligner'] = aligner

            meta['n_contigs'] = n_contigs

            # Estimate coverage from idxstats (fast, uses index)
            if meta.get('indexed'):
                try:
                    r2 = subprocess.run(['samtools', 'idxstats', str(path)],
                                        capture_output=True, text=True, timeout=30)
                    if r2.returncode == 0:
                        total_mapped = 0
                        genome_len = 0
                        for line in r2.stdout.strip().split('\n'):
                            parts = line.split('\t')
                            if len(parts) >= 4 and parts[0] != '*':
                                genome_len += int(parts[1])
                                total_mapped += int(parts[2])
                        if genome_len > 0:
                            # Rough coverage estimate: (mapped_reads * avg_read_len) / genome_len
                            # Assume 150bp reads for Illumina
                            read_len = 150
                            if meta.get('platform') in ('ONT', 'PACBIO'):
                                read_len = 5000
                            est_cov = round(total_mapped * read_len / genome_len, 1)
                            meta['est_coverage'] = f'{est_cov}x'
                            meta['total_reads'] = total_mapped
                except Exception:
                    pass

            # Detect read length from first few reads
            try:
                r3 = subprocess.run(
                    f'samtools view "{path}" | head -3 | cut -f10 | awk \'{{print length($0)}}\'',
                    shell=True, capture_output=True, text=True, timeout=15)
                if r3.returncode == 0 and r3.stdout.strip():
                    lengths = [int(x) for x in r3.stdout.strip().split('\n') if x.strip().isdigit()]
                    if lengths:
                        avg_len = sum(lengths) // len(lengths)
                        meta['read_length'] = f'{avg_len}bp'
            except Exception:
                pass

        else:
            # --- VCF/gVCF: parse header ---
            r = subprocess.run([_BCFTOOLS, 'view', '-h', str(path)],
                               capture_output=True, text=True, timeout=30)
            if r.returncode != 0:
                return meta
            header = r.stdout

            meta["indexed"] = any(
                os.path.exists(path + ext) for ext in ('.tbi', '.csi')
            )

            n_contigs = 0
            for line in header.split('\n'):
                if line.startswith('##contig'):
                    n_contigs += 1
                    inner = line.split('<', 1)[-1].rstrip('>')
                    fields = dict(f.split('=', 1) for f in inner.split(',') if '=' in f)
                    cid = fields.get('ID', '')
                    if 'chr_naming' not in meta:
                        meta['chr_naming'] = 'chr' if cid.startswith('chr') else 'numeric'
                    if cid in ('chr1', '1') and 'genome_build' not in meta:
                        ln = int(fields.get('length', 0))
                        if ln == 248956422: meta['genome_build'] = 'GRCh38'
                        elif ln == 249250621: meta['genome_build'] = 'GRCh37'
                        elif ln == 247249719: meta['genome_build'] = 'hg18'

                elif line.startswith('##DeepVariant'):
                    ver = line.split('=', 1)[-1].strip() if '=' in line else ''
                    meta['variant_caller'] = f'DeepVariant {ver}'.strip()
                elif line.startswith('##source') and 'variant_caller' not in meta:
                    src = line.split('=', 1)[-1].strip() if '=' in line else ''
                    if any(x in src.lower() for x in ('deepvariant', 'gatk', 'dragen', 'freebayes', 'strelka', 'octopus')):
                        meta['variant_caller'] = src
                elif line.startswith('##GATKCommandLine') and 'variant_caller' not in meta:
                    meta['variant_caller'] = 'GATK HaplotypeCaller'

                elif line.startswith('#CHROM'):
                    samples = line.strip().split('\t')[9:]
                    if samples:
                        meta['sample_name'] = samples[0]
                        if len(samples) > 1:
                            meta['n_samples'] = len(samples)

            meta['n_contigs'] = n_contigs

            # Variant count (fast if indexed; skip for gVCFs as count includes ref blocks)
            is_gvcf = '.g.vcf' in p_lower or '.gvcf' in p_lower
            if meta.get('indexed') and not is_gvcf:
                try:
                    r2 = subprocess.run([_BCFTOOLS, 'index', '-n', str(path)],
                                        capture_output=True, text=True, timeout=10)
                    if r2.returncode == 0 and r2.stdout.strip():
                        meta['variant_count'] = int(r2.stdout.strip())
                except Exception:
                    pass

    except Exception:
        pass

    _file_meta_cache[path] = {"mtime": mtime, "meta": meta}
    return meta


def _detect_build_from_header(path):
    """Detect genome build from file header (wrapper around _probe_file_metadata)."""
    meta = _probe_file_metadata(path)
    return meta.get('genome_build')


def _detect_chr_naming_from_header(path):
    """Detect chr naming convention (wrapper around _probe_file_metadata)."""
    meta = _probe_file_metadata(path)
    return meta.get('chr_naming')


# ── API Routes ────────────────────────────────────────────────────

@app.get("/api/files")
async def list_files(username: str = Depends(current_user)):
    """List the calling user's registered files + their active file id."""
    ctx = get_user_state(username)
    with ctx.lock:
        files = list(ctx.files_state["files"].values())
        active_id = ctx.files_state.get("active_file_id")
    # Enrich with live pgen status (in-memory builds may have completed)
    for f in files:
        fid = f["id"]
        mem_status = _pgen_build_status.get(fid)
        if mem_status == "building":
            # Verify disk — build may have completed or server restarted mid-build
            disk_status = _check_pgen_ready(f["path"])
            if disk_status == "ready":
                _pgen_build_status[fid] = "ready"
                f["pgen_status"] = "ready"
            else:
                f["pgen_status"] = "building"
        elif mem_status:
            f["pgen_status"] = mem_status
        elif f.get("pgen_status") not in ("ready", "not_needed"):
            # Re-check on disk in case it was built by a test run
            f["pgen_status"] = _check_pgen_ready(f["path"])
    # Enrich with file type and full metadata from headers
    needs_save = False
    for f in files:
        path = f.get("path", "")
        p_lower = path.lower()
        if p_lower.endswith(('.g.vcf.gz', '.gvcf.gz', '.gvcf')):
            f["file_type"] = "gVCF"
        elif p_lower.endswith(('.vcf.gz', '.vcf', '.bcf')):
            f["file_type"] = "VCF"
        elif p_lower.endswith('.bam'):
            f["file_type"] = "BAM"
        elif p_lower.endswith('.cram'):
            f["file_type"] = "CRAM"
        else:
            f["file_type"] = ""
        # Probe file header for all metadata (cached by mtime)
        if os.path.exists(path):
            meta = _probe_file_metadata(path)
            # Fields to auto-fill from header probe
            meta_fields = [
                "genome_build", "chr_naming", "sample_name", "platform",
                "aligner", "variant_caller", "indexed", "variant_count",
                "est_coverage", "total_reads", "read_length", "n_contigs",
                "center", "library", "n_samples",
            ]
            for key in meta_fields:
                if meta.get(key) and not f.get(key):
                    f[key] = meta[key]
                    # Persist key fields back to files_state
                    if key in ("genome_build", "chr_naming", "sample_name", "platform",
                               "aligner", "variant_caller", "est_coverage", "read_length"):
                        with ctx.lock:
                            fentry = ctx.files_state["files"].get(f["id"])
                            if fentry and not fentry.get(key):
                                fentry[key] = meta[key]
                                needs_save = True
        # Ensure required fields exist
        for key in ("genome_build", "chr_naming"):
            if key not in f:
                f[key] = ""
    if needs_save:
        with ctx.lock:
            ctx.save_files()
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


@app.post("/api/files/{file_id}/prepare")
async def prepare_file(file_id: str, username: str = Depends(current_user)):
    """Manually trigger pgen cache build for a file."""
    ctx = get_user_state(username)
    with ctx.lock:
        entry = ctx.files_state["files"].get(file_id)
    if not entry:
        return JSONResponse({"ok": False, "error": "Unknown file_id"}, status_code=404)
    status = _pgen_build_status.get(file_id)
    if status == "building":
        return {"ok": True, "pgen_status": "building", "message": "Already building"}
    if _check_pgen_ready(entry["path"]) == "ready":
        _pgen_build_status[file_id] = "ready"
        with ctx.lock:
            entry_live = ctx.files_state["files"].get(file_id)
            if entry_live:
                entry_live["pgen_status"] = "ready"
                ctx.save_files()
        return {"ok": True, "pgen_status": "ready"}
    _trigger_pgen_build(username, file_id, entry["path"])
    return {"ok": True, "pgen_status": "building", "message": "Build started"}


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

    # Merge PGS enrichment data into test entries
    enrichment = _load_pgs_enrichment()
    enriched_tests = []
    for t in TESTS:
        if t.get("test_type") == "pgs_score" and t.get("params", {}).get("pgs_id"):
            pgs_id = t["params"]["pgs_id"]
            e = enrichment.get(pgs_id)
            if e:
                t_copy = dict(t)
                t_copy["enrichment"] = e
                enriched_tests.append(t_copy)
                continue
        enriched_tests.append(t)

    return {
        "categories": CATEGORIES,
        "tests": enriched_tests,
        "active_file": active,
        "active_vcf": active["path"] if active else None,
    }



# ── Tab definitions ───────────────────────────────────────────────
TAB_DEFS = {
    "general": {
        "label": "General",
        "categories": ["Sex Check", "Sample QC", "Ancestry"],
    },
    "polygenic": {
        "label": "Polygenic Scores",
        "categories": [
            "PGS - Cancer", "PGS - Cardiovascular", "PGS - Metabolic",
            "PGS - Autoimmune", "PGS - Neurological", "PGS - Traits",
            "PGS - Lifestyle", "PGS - Custom", "PGS - rsID Lists",
        ],
    },
    "monogenic": {
        "label": "Monogenic & Variants",
        "categories": [
            "Monogenic", "Carrier Status", "Single Variants",
            "Fun Traits", "Nutrigenomics", "Sports & Fitness",
            "Sleep & Circadian",
        ],
    },
    "pharmacogenomics": {
        "label": "Pharmacogenomics",
        "categories": ["Pharmacogenomics"],
    },
}
TAB_ORDER = ["general", "polygenic", "monogenic", "pharmacogenomics"]

def _tab_for_category(cat):
    """Return tab key for a category name."""
    for tab_key, tab_def in TAB_DEFS.items():
        if cat in tab_def["categories"]:
            return tab_key
    return "polygenic" if cat.startswith("PGS") else "monogenic"


# ── Test registry markdown export / import ────────────────────────
def _tests_to_markdown(tab=None):
    """Serialize TESTS to Markdown. If tab given, only that tab's categories.
    rsid_pgs_score tests excluded (they come from rsid_list_pgs.py)."""
    tab_label = TAB_DEFS[tab]["label"] if tab and tab in TAB_DEFS else "Test Registry"
    lines = [f"# {tab_label}\n"]
    current_cat = None
    for t in TESTS:
        if t["test_type"] == "rsid_pgs_score":
            continue
        if tab and _tab_for_category(t["category"]) != tab:
            continue
        if t["category"] != current_cat:
            current_cat = t["category"]
            lines.append(f"\n## {current_cat}\n")
        params_str = json.dumps(t["params"], ensure_ascii=False) if t.get("params") else "{}"
        lines.append(f"- **{t['name']}** (`{t['id']}`)")
        lines.append(f"  {t['description']}")
        lines.append(f"  `type: {t['test_type']}` `params: {params_str}`")
        # Add PGS Catalog link for PGS tests
        pgs_id = t.get("params", {}).get("pgs_id", "")
        if pgs_id:
            lines.append(f"  [PGS Catalog](https://www.pgscatalog.org/score/{pgs_id}/)")
        lines.append("")
    return "\n".join(lines)


def _markdown_to_tests(md_text):
    """Parse markdown back into a list of test dicts.
    Returns (tests_list, categories_list) or raises ValueError."""
    import re as _re
    tests_out = []
    cats_out = []
    current_cat = None
    lines = md_text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        m = _re.match(r'^##\s+(.+)$', line)
        if m:
            current_cat = m.group(1).strip()
            if current_cat not in cats_out:
                cats_out.append(current_cat)
            i += 1
            continue
        m = _re.match(r'^-\s+\*\*(.+?)\*\*\s+\(`([^`]+)`\)\s*$', line)
        if m and current_cat:
            name = m.group(1)
            test_id = m.group(2)
            description = ""
            test_type = "specialized"
            params = {}
            i += 1
            while i < len(lines) and lines[i].startswith("  "):
                sub = lines[i].strip()
                tm = _re.match(r'^`type:\s*([^`]+)`\s*`params:\s*(.+)`$', sub)
                if tm:
                    test_type = tm.group(1).strip()
                    try:
                        params = json.loads(tm.group(2).strip())
                    except json.JSONDecodeError:
                        params = {}
                elif sub.startswith("[PGS Catalog]"):
                    pass  # Skip auto-generated PGS Catalog links
                else:
                    if description:
                        description += " " + sub
                    else:
                        description = sub
                i += 1
            tests_out.append({
                "id": test_id, "category": current_cat, "name": name,
                "description": description, "test_type": test_type, "params": params,
            })
            continue
        i += 1
    if not tests_out:
        raise ValueError("No tests found in markdown")
    return tests_out, cats_out


def _rewrite_test_registry_file(tests_list):
    """Rewrite test_registry.py from the given tests list."""
    import os as _os
    registry_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "test_registry.py")
    out = []
    out.append('"""')
    out.append("Test registry \u2014 all genomic tests.")
    out.append("Each test has: id, category, name, description, test_type, params.")
    out.append("")
    out.append("test_type determines which runner handles it:")
    out.append("  - variant_lookup / vcf_stats / pgs_score / clinvar_screen / specialized / rsid_pgs_score")
    out.append('"""')
    out.append("")
    out.append("TESTS = []")
    out.append("")
    out.append("def _t(id, category, name, description, test_type, params):")
    out.append("    TESTS.append({")
    out.append('        "id": id, "category": category, "name": name,')
    out.append('        "description": description, "test_type": test_type, "params": params,')
    out.append("    })")
    out.append("")

    current_cat = None
    for t in tests_list:
        if t["category"] != current_cat:
            current_cat = t["category"]
            border = "\u2500" * max(1, 60 - len(current_cat))
            out.append(f"# \u2500\u2500 {current_cat} {border}")
        out.append(f"_t({repr(t['id'])}, {repr(t['category'])}, {repr(t['name'])},")
        out.append(f"   {repr(t['description'])},")
        out.append(f"   {repr(t['test_type'])}, {repr(t['params'])})")
        out.append("")

    # rsID PGS import block (always preserved)
    out.append("")
    out.append("# \u2500\u2500 PGS - rsID Lists (from rsid-list.md) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
    out.append("try:")
    out.append("    from rsid_list_pgs import RSID_PGS as _RSID_PGS_LIST")
    out.append("except ImportError:")
    out.append("    _RSID_PGS_LIST = []")
    out.append("")
    out.append("def _slug(s):")
    out.append("    import re as _re")
    out.append('    return _re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")')
    out.append("")
    out.append("_seen_ids = set()")
    out.append("for _idx, _pgs in enumerate(_RSID_PGS_LIST):")
    out.append('    _title = _pgs["title"]')
    out.append('    _citation = _pgs["citation"]')
    out.append('    _author = _citation.split(",")[0].strip().lower() if _citation else f"v{_idx}"')
    out.append('    _id = f"rsid_{_slug(_title)}_{_slug(_author)}"')
    out.append("    _base = _id")
    out.append("    _suffix = 2")
    out.append("    while _id in _seen_ids:")
    out.append('        _id = f"{_base}_{_suffix}"')
    out.append("        _suffix += 1")
    out.append("    _seen_ids.add(_id)")
    out.append('    _name = f"{_title} ({_citation})" if _citation else _title')
    out.append("    _desc = f\"{len(_pgs['variants'])} variants from {_citation or 'rsid-list.md'}. \" \\")
    out.append('            f"Score = sum(dosage \u00d7 effect) across rsIDs."')
    out.append('    _t(_id, "PGS - rsID Lists", _name, _desc,')
    out.append('       "rsid_pgs_score", {"title": _title, "citation": _citation, "variants": _pgs["variants"]})')
    out.append("")
    out.append('TESTS_BY_ID = {t["id"]: t for t in TESTS}')
    out.append("CATEGORIES = []")
    out.append("_seen = set()")
    out.append("for t in TESTS:")
    out.append('    if t["category"] not in _seen:')
    out.append('        CATEGORIES.append(t["category"])')
    out.append('        _seen.add(t["category"])')
    out.append("")

    with open(registry_path, "w") as f:
        f.write("\n".join(out))


def _reload_tests_from_parsed(new_tests, username=None):
    """Replace in-memory TESTS with new_tests, re-add rsid PGS, rebuild indexes."""
    TESTS.clear()
    TESTS_BY_ID.clear()
    CATEGORIES.clear()
    for t in new_tests:
        TESTS.append(t)
        TESTS_BY_ID[t["id"]] = t
    # Re-add rsid_pgs_score tests
    try:
        from rsid_list_pgs import RSID_PGS as _rpgs
        import re as _re2
        _seen2 = {t["id"] for t in TESTS}
        for _idx2, _pgs2 in enumerate(_rpgs):
            _title2 = _pgs2["title"]
            _cit2 = _pgs2["citation"]
            _auth2 = _cit2.split(",")[0].strip().lower() if _cit2 else f"v{_idx2}"
            _id2 = f"rsid_{_re2.sub(r'[^a-z0-9]+', '_', _title2.lower()).strip('_')}_{_re2.sub(r'[^a-z0-9]+', '_', _auth2.lower()).strip('_')}"
            _base2 = _id2
            _suf2 = 2
            while _id2 in _seen2:
                _id2 = f"{_base2}_{_suf2}"
                _suf2 += 1
            _seen2.add(_id2)
            _nm2 = f"{_title2} ({_cit2})" if _cit2 else _title2
            _desc2 = f"{len(_pgs2['variants'])} variants from {_cit2 or 'rsid-list.md'}. Score = sum(dosage \u00d7 effect) across rsIDs."
            t2 = {"id": _id2, "category": "PGS - rsID Lists", "name": _nm2,
                   "description": _desc2, "test_type": "rsid_pgs_score",
                   "params": {"title": _title2, "citation": _cit2, "variants": _pgs2["variants"]}}
            TESTS.append(t2)
            TESTS_BY_ID[_id2] = t2
    except ImportError:
        pass
    # Rebuild CATEGORIES
    seen = set()
    for t in TESTS:
        if t["category"] not in seen:
            CATEGORIES.append(t["category"])
            seen.add(t["category"])
    if username:
        _eager_inject_custom_pgs_for_user(username)


@app.get("/api/tests/tabs")
async def get_tests_tabs(username: str = Depends(current_user)):
    """Return tab definitions with test counts."""
    _eager_inject_custom_pgs_for_user(username)
    result = []
    for tk in TAB_ORDER:
        td = TAB_DEFS[tk]
        count = sum(1 for t in TESTS if _tab_for_category(t["category"]) == tk)
        result.append({"key": tk, "label": td["label"], "count": count})
    return {"ok": True, "tabs": result}


@app.get("/api/tests/markdown")
async def get_tests_markdown(tab: str = "", username: str = Depends(current_user)):
    """Return test registry as markdown, optionally filtered by tab."""
    _eager_inject_custom_pgs_for_user(username)
    t = tab if tab in TAB_DEFS else None
    md = _tests_to_markdown(tab=t)
    count = sum(1 for tt in TESTS if (not t or _tab_for_category(tt["category"]) == t)
                and tt["test_type"] != "rsid_pgs_score")
    return {"ok": True, "markdown": md, "test_count": count, "tab": t or "all"}


@app.put("/api/tests/markdown")
async def put_tests_markdown(request: Request, tab: str = "", username: str = Depends(current_user)):
    """Update test registry from edited markdown. Tab-scoped: only replaces
    categories belonging to that tab, keeping others intact."""
    body = await request.json()
    md_text = body.get("markdown", "")
    if not md_text.strip():
        return JSONResponse({"ok": False, "error": "Empty markdown"}, status_code=400)
    try:
        edited_tests, _ = _markdown_to_tests(md_text)
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

    t = tab if tab in TAB_DEFS else None
    if t:
        kept = [tt for tt in TESTS if _tab_for_category(tt["category"]) != t
                and tt["test_type"] != "rsid_pgs_score"]
        new_tests = kept + edited_tests
    else:
        new_tests = edited_tests

    try:
        _rewrite_test_registry_file(new_tests)
    except Exception:
        logger.exception("Failed to rewrite test_registry.py")
        return JSONResponse({"ok": False, "error": "Failed to save file"}, status_code=500)

    _reload_tests_from_parsed(new_tests, username=username)
    return {"ok": True, "test_count": len(TESTS), "categories": len(CATEGORIES)}


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
def get_status(username: str = Depends(current_user)):
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
def system_stats():
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


# ── PGS Enrichment Refresh endpoints ──────────────────────────────

@app.post("/api/pgs/refresh/{category}")
async def refresh_pgs_category(category: str, username: str = Depends(current_user)):
    """Trigger a background refresh of all PGS entries in a category from PGS Catalog."""
    # URL-decode category name
    category = urllib.parse.unquote(category)

    # Validate the category exists and has PGS tests
    if category not in CATEGORIES:
        raise HTTPException(status_code=404, detail=f"Category not found: {category}")
    pgs_tests = [t for t in TESTS if t["category"] == category and t.get("test_type") == "pgs_score"]
    if not pgs_tests:
        raise HTTPException(status_code=400, detail=f"No PGS tests in category: {category}")

    # Check if already running
    status = _pgs_refresh_status.get(category)
    if status and status.get("status") == "running":
        return {"status": "already_running", "detail": status}

    # Start background refresh
    thread = Thread(target=_run_pgs_refresh, args=(category,), daemon=True)
    thread.start()
    return {"status": "started", "category": category, "total": len(pgs_tests)}


@app.get("/api/pgs/refresh/{category}/status")
async def get_pgs_refresh_status(category: str, username: str = Depends(current_user)):
    """Check status of a PGS category refresh."""
    category = urllib.parse.unquote(category)
    status = _pgs_refresh_status.get(category)
    if not status:
        return {"status": "idle"}
    return status


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
    api_key = _get_user_api_key(username)
    return {
        "authenticated": True,
        "username": username,
        "has_api_key": api_key is not None,
        "masked_api_key": _mask_api_key(api_key) if api_key else None,
    }


# ── API key management ──────────────────────────────────────────
@app.post("/api/auth/api-key")
async def set_api_key(request: Request):
    sid = request.cookies.get(SESSION_COOKIE)
    username = _resolve_session(sid)
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)
    key = (data.get("api_key") or "").strip()
    if not key.startswith("sk-ant-"):
        return JSONResponse(
            {"ok": False, "error": "Invalid key format — must start with sk-ant-"},
            status_code=400,
        )
    if len(key) < 20:
        return JSONResponse(
            {"ok": False, "error": "Key too short"},
            status_code=400,
        )
    _set_user_api_key(username, key)
    # Kill existing tmux session so it restarts with the new key
    from chat import _session_name, _session_exists, _run_tmux
    session = _session_name(username)
    if _session_exists(session):
        try:
            _run_tmux(["kill-session", "-t", session], timeout=10)
        except Exception:
            pass
    return {"ok": True, "masked_key": _mask_api_key(key)}


@app.get("/api/auth/api-key")
async def get_api_key(request: Request):
    sid = request.cookies.get(SESSION_COOKIE)
    username = _resolve_session(sid)
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    api_key = _get_user_api_key(username)
    return {
        "has_key": api_key is not None,
        "masked_key": _mask_api_key(api_key) if api_key else None,
    }


@app.delete("/api/auth/api-key")
async def delete_api_key(request: Request):
    sid = request.cookies.get(SESSION_COOKIE)
    username = _resolve_session(sid)
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    _remove_user_api_key(username)
    from chat import _session_name, _session_exists, _run_tmux
    session = _session_name(username)
    if _session_exists(session):
        try:
            _run_tmux(["kill-session", "-t", session], timeout=10)
        except Exception:
            pass
    return {"ok": True}


# ── Settings API ──────────────────────────────────────────────────
@app.get("/api/settings")
async def get_settings(request: Request):
    sid = request.cookies.get(SESSION_COOKIE)
    username = _resolve_session(sid)
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    settings = _load_user_settings(username)
    provider_keys = settings.get("provider_keys", {})
    openai_key = _get_provider_key(username, "openai")
    claude_key = _get_provider_key(username, "claude") or _get_user_api_key(username)
    return {
        "interp_model": settings.get("interp_model", "gemini"),
        "keys": {
            "openai": {
                "has_key": openai_key is not None,
                "masked": _mask_api_key(openai_key) if openai_key else None,
            },
            "claude": {
                "has_key": claude_key is not None,
                "masked": _mask_api_key(claude_key) if claude_key else None,
            },
            "gemini": {"has_key": True, "masked": "server-credential"},
        },
    }

@app.post("/api/settings/interp-model")
async def set_interp_model_endpoint(request: Request):
    sid = request.cookies.get(SESSION_COOKIE)
    username = _resolve_session(sid)
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    data = await request.json()
    model = (data.get("model") or "").strip()
    if model not in ("gemini", "openai", "claude"):
        return JSONResponse({"ok": False, "error": "Invalid model"}, status_code=400)
    _set_interp_model(username, model)
    return {"ok": True, "model": model}

@app.post("/api/settings/provider-key")
async def set_provider_key_endpoint(request: Request):
    sid = request.cookies.get(SESSION_COOKIE)
    username = _resolve_session(sid)
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    data = await request.json()
    provider = (data.get("provider") or "").strip()
    key = (data.get("key") or "").strip()
    if provider not in ("openai", "claude"):
        return JSONResponse({"ok": False, "error": "Invalid provider"}, status_code=400)
    if not key:
        return JSONResponse({"ok": False, "error": "Key is required"}, status_code=400)
    if provider == "openai" and not key.startswith("sk-"):
        return JSONResponse({"ok": False, "error": "OpenAI key must start with sk-"}, status_code=400)
    if provider == "claude" and not key.startswith("sk-ant-"):
        return JSONResponse({"ok": False, "error": "Anthropic key must start with sk-ant-"}, status_code=400)
    _set_provider_key(username, provider, key)
    if provider == "claude":
        _set_user_api_key(username, key)
    return {"ok": True, "masked": _mask_api_key(key)}

@app.delete("/api/settings/provider-key/{provider}")
async def delete_provider_key_endpoint(request: Request, provider: str):
    sid = request.cookies.get(SESSION_COOKIE)
    username = _resolve_session(sid)
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if provider not in ("openai", "claude"):
        raise HTTPException(status_code=400, detail="Invalid provider")
    _remove_provider_key(username, provider)
    if provider == "claude":
        _remove_user_api_key(username)
    return {"ok": True}




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


// ─── Settings view ──────────────────────────────────────────────
async function loadSettingsView() {
  try {
    const r = await fetch(BASE + '/api/settings');
    if (!r.ok) return;
    const data = await r.json();

    // Model selector
    const sel = document.getElementById('settingsInterpModel');
    if (sel) sel.value = data.interp_model || 'gemini';

    // Key status for each provider
    for (const provider of ['openai', 'claude']) {
      const info = (data.keys || {})[provider] || {};
      const dot = document.getElementById(provider + 'KeyDot');
      const label = document.getElementById(provider + 'KeyLabel');
      const masked = document.getElementById(provider + 'KeyMasked');
      const removeBtn = document.getElementById(provider + 'RemoveBtn');
      if (info.has_key) {
        if (dot) dot.classList.add('set');
        if (label) label.textContent = 'Active:';
        if (masked) masked.textContent = info.masked || 'set';
        if (removeBtn) removeBtn.style.display = '';
      } else {
        if (dot) dot.classList.remove('set');
        if (label) label.textContent = 'No key set';
        if (masked) masked.textContent = '';
        if (removeBtn) removeBtn.style.display = 'none';
      }
    }

    updateModelNote();
  } catch (e) {
    console.error('Failed to load settings:', e);
  }
}

function updateModelNote() {
  const sel = document.getElementById('settingsInterpModel');
  const note = document.getElementById('settingsModelNote');
  if (!sel || !note) return;
  const v = sel.value;
  if (v === 'gemini') {
    note.textContent = 'Uses the server Vertex AI credential. No API key needed from you.';
  } else if (v === 'openai') {
    note.textContent = 'Requires your OpenAI API key below. Uses GPT-4o-mini (~$0.001/interpretation).';
  } else if (v === 'claude') {
    note.textContent = 'Requires your Anthropic API key below. Uses Claude Sonnet (~$0.002/interpretation).';
  }
}

async function saveInterpModel(model) {
  updateModelNote();
  try {
    await fetch(BASE + '/api/settings/interp-model', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model }),
    });
  } catch (e) {
    console.error('Failed to save model:', e);
  }
}

async function saveProviderKey(provider) {
  const input = document.getElementById(provider + 'KeyInput');
  const errEl = document.getElementById(provider + 'KeyError');
  const successEl = document.getElementById(provider + 'KeySuccess');
  const key = (input ? input.value : '').trim();
  if (errEl) errEl.style.display = 'none';
  if (successEl) successEl.style.display = 'none';
  if (!key) {
    if (errEl) { errEl.textContent = 'Please enter your API key'; errEl.style.display = 'block'; }
    return;
  }
  try {
    const r = await fetch(BASE + '/api/settings/provider-key', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ provider, key }),
    });
    const data = await r.json();
    if (data.ok) {
      if (input) input.value = '';
      if (successEl) { successEl.textContent = 'Key saved successfully.'; successEl.style.display = 'block'; }
      loadSettingsView();
      // Also refresh the chat settings if it was the Claude key
      if (provider === 'claude') {
        window.sgHasApiKey = true;
        window.sgMaskedApiKey = data.masked || null;
        if (typeof settingsRefreshKeyUI === 'function') settingsRefreshKeyUI();
        if (typeof updateApiKeyOverlay === 'function') updateApiKeyOverlay();
      }
    } else {
      if (errEl) { errEl.textContent = data.error || 'Failed'; errEl.style.display = 'block'; }
    }
  } catch (e) {
    if (errEl) { errEl.textContent = 'Network error'; errEl.style.display = 'block'; }
  }
}

async function removeProviderKey(provider) {
  if (!confirm('Remove this API key?')) return;
  try {
    const r = await fetch(BASE + '/api/settings/provider-key/' + provider, { method: 'DELETE' });
    const data = await r.json();
    if (data.ok) {
      loadSettingsView();
      if (provider === 'claude') {
        window.sgHasApiKey = false;
        window.sgMaskedApiKey = null;
        if (typeof settingsRefreshKeyUI === 'function') settingsRefreshKeyUI();
        if (typeof updateApiKeyOverlay === 'function') updateApiKeyOverlay();
      }
    }
  } catch (e) {
    console.error('Failed to remove key:', e);
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
.pgs-link { font-size: 0.8rem; text-decoration: none; color: var(--accent, #6c9fff); opacity: 0.7; vertical-align: super; margin-left: 4px; }
.pgs-link:hover { opacity: 1; }
.pgen-badge { font-size: 0.65rem; padding: 1px 5px; border-radius: 3px; font-weight: 500; vertical-align: middle; margin-left: 6px; }
.pgen-badge.ready { background: #1a3d1a; color: #4ade80; }
.pgen-badge.building { background: #3d3a1a; color: #facc15; animation: pulse 1.5s infinite; }
.pgen-badge.pending { background: #3d1a1a; color: #f87171; }
.prep-help { display: inline-flex; align-items: center; justify-content: center; width: 16px; height: 16px; border-radius: 50%; background: var(--bg3, #333); color: var(--text2, #aaa); font-size: 0.6rem; font-weight: 700; cursor: help; margin-left: 4px; position: relative; vertical-align: middle; }
.prep-help:hover .prep-tooltip, .prep-help:focus .prep-tooltip { display: block; }
.prep-tooltip { display: none; position: absolute; bottom: 120%; left: 50%; transform: translateX(-50%); width: 280px; padding: 8px 10px; background: var(--bg2, #1e1e2e); border: 1px solid var(--border, #333); border-radius: 6px; font-size: 0.7rem; font-weight: 400; line-height: 1.4; color: var(--text, #eee); box-shadow: 0 4px 12px rgba(0,0,0,0.4); z-index: 1000; white-space: normal; text-align: left; }
.file-select-hint { font-size: 0.7rem; color: var(--text2, #aaa); margin-top: 3px; padding: 2px 0; }
.file-select-hint a { color: var(--accent, #6c9fff); text-decoration: underline; cursor: pointer; }
.test-info p { font-size: 0.75rem; color: var(--text2); margin-top: 2px; }
.pgs-enrichment { display: flex; flex-wrap: wrap; gap: 4px 8px; margin-top: 4px; align-items: center; }
.pgs-enrichment .enr-item { font-size: 0.7rem; color: var(--text2); }
.pgs-enrichment .enr-link { color: var(--accent, #6c9fff); text-decoration: none; font-size: 0.7rem; }
.pgs-enrichment .enr-link:hover { text-decoration: underline; }
.pgs-enrichment .enr-chip { font-size: 0.6rem; padding: 1px 5px; border-radius: 3px; background: var(--surface2, #2a2a3e); color: var(--text2); border: 1px solid var(--border); font-weight: 500; }
.pgs-enrichment .enr-ancestry { font-size: 0.65rem; }
.pgs-enrichment .enr-desc { display: block; width: 100%; font-size: 0.68rem; color: var(--text3, #888); font-style: italic; margin-top: 2px; line-height: 1.3; }
.enrich-btn { background: var(--surface2, #2a2a3e) !important; border: 1px solid var(--accent, #6c9fff) !important; color: var(--accent, #6c9fff) !important; font-size: 0.7rem !important; }
.enrich-btn:hover { background: var(--accent, #6c9fff) !important; color: #fff !important; }
.enrich-btn:disabled { opacity: 0.6; cursor: wait; }
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
  font-size: 0.85rem;
  line-height: 1.6;
  word-break: break-word;
}
.report-section { margin-bottom: 16px; padding-bottom: 16px; border-bottom: 1px solid var(--border); }
.report-section:last-child { border-bottom: none; margin-bottom: 0; }
.report-section h4 { font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.5px; color: var(--accent); margin: 0 0 10px 0; }
.score-grid, .pipeline-grid, .diag-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px; }
.score-item, .pipe-item, .diag-item { background: var(--surface2); padding: 6px 10px; border-radius: 6px; }
.score-item label, .pipe-item label, .diag-item label { font-size: 0.65rem; color: var(--text2); display: block; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px; }
.score-item span, .pipe-item span, .diag-item span { font-size: 0.85rem; font-weight: 500; }
.pctl-value { color: var(--accent); font-weight: 700; font-size: 1.1rem !important; }
.pctl-details { margin-top: 10px; font-size: 0.75rem; color: var(--text2); line-height: 1.5; }
.pctl-details small { opacity: 0.7; }
.diag-badge { font-size: 0.6rem; padding: 2px 6px; border-radius: 3px; vertical-align: middle; margin-left: 6px; font-weight: 600; }
.diag-badge.ok { background: #1a3d1a; color: #4ade80; }
.diag-badge.warn { background: #3d3a1a; color: #facc15; }
.diag-badge.fail { background: #3d1a1a; color: #f87171; }
.sanity-gates { margin-top: 8px; }
.gate-trip { font-size: 0.75rem; color: #facc15; padding: 3px 0; }
.variant-table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
.variant-table th { text-align: left; font-size: 0.65rem; text-transform: uppercase; color: var(--text2); padding: 4px 8px; border-bottom: 1px solid var(--border); }
.variant-table td { padding: 4px 8px; border-bottom: 1px solid var(--border); }
.findings-section { background: rgba(248, 113, 113, 0.05); border-radius: 8px; padding: 12px; }
.error-section { background: rgba(248, 113, 113, 0.1); border-radius: 8px; padding: 12px; }
.error-section h4 { color: #f87171; }
.raw-json-section { margin-top: 16px; }
.raw-json-section summary { font-size: 0.75rem; color: var(--text2); cursor: pointer; padding: 6px 0; }
.raw-json-section pre { background: var(--surface2); padding: 12px; border-radius: 6px; font-size: 0.75rem; overflow-x: auto; white-space: pre-wrap; max-height: 400px; overflow-y: auto; }
.pipe-item a { color: var(--accent); text-decoration: none; }
.pipe-item a:hover { text-decoration: underline; }
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
.report-interpretation {
  background: var(--surface2);
  border-left: 4px solid var(--accent);
  border-radius: 8px;
  padding: 16px 20px;
  margin-bottom: 16px;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  font-size: 0.95rem;
  line-height: 1.7;
  color: var(--text);
}
.report-interpretation h4 {
  margin: 0 0 8px 0;
  font-size: 0.8rem;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--accent);
}

/* Tests tabs */
.tests-tabs {
  display: flex;
  gap: 0;
  margin-bottom: 12px;
  border-bottom: 2px solid var(--border);
}
.tests-tab {
  padding: 10px 20px;
  cursor: pointer;
  font-size: 0.9rem;
  font-weight: 500;
  color: var(--text2);
  border: none;
  background: none;
  border-bottom: 2px solid transparent;
  margin-bottom: -2px;
  transition: color 0.15s, border-color 0.15s;
  white-space: nowrap;
}
.tests-tab:hover { color: var(--text); }
.tests-tab.active {
  color: var(--accent);
  border-bottom-color: var(--accent);
  font-weight: 600;
}
.tests-tab .tab-count {
  font-size: 0.75rem;
  color: var(--text2);
  margin-left: 6px;
  opacity: 0.7;
}
.tests-tab.active .tab-count { color: var(--accent); opacity: 0.85; }

/* Filter bar */
.filter-bar {
  display: inline-block;
  position: relative;
  margin-bottom: 10px;
}
.filter-toggle-btn {
  padding: 7px 16px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--surface);
  color: var(--text2);
  cursor: pointer;
  font-size: 0.82rem;
  transition: all 0.15s;
  user-select: none;
  display: inline-flex;
  align-items: center;
  gap: 6px;
}
.filter-toggle-btn:hover { border-color: var(--accent); color: var(--text); }
.filter-toggle-btn.has-filter {
  border-color: var(--accent);
  color: var(--accent);
  font-weight: 600;
}
.filter-toggle-btn .filter-icon { font-size: 0.9rem; }
.filter-popup {
  display: none;
  position: absolute;
  top: calc(100% + 6px);
  left: 0;
  z-index: 200;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 10px;
  box-shadow: 0 6px 20px rgba(0,0,0,0.25);
  min-width: 220px;
}
.filter-popup.open { display: flex; flex-direction: column; gap: 6px; }
.filter-chip {
  padding: 7px 14px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--bg);
  color: var(--text2);
  cursor: pointer;
  font-size: 0.82rem;
  transition: all 0.15s;
  user-select: none;
  text-align: left;
  width: 100%;
}
.filter-chip:hover { border-color: var(--accent); color: var(--text); background: var(--surface); }
.filter-chip.active {
  background: var(--accent);
  border-color: var(--accent);
  color: #fff;
  font-weight: 600;
}
.filter-chip .chip-count {
  font-size: 0.7rem;
  opacity: 0.7;
  margin-left: 4px;
}

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

/* API Key first-run overlay */
.api-key-overlay {
  position: absolute;
  inset: 0;
  z-index: 100;
  background: rgba(10, 14, 23, 0.85);
  backdrop-filter: blur(6px);
  display: flex;
  align-items: center;
  justify-content: center;
}
.api-key-overlay-card {
  background: #111827;
  border: 1px solid #2d3748;
  border-radius: 16px;
  padding: 32px;
  width: 100%;
  max-width: 420px;
  box-shadow: 0 12px 50px rgba(0, 0, 0, 0.5);
}
.api-key-overlay-card h3 {
  margin: 0 0 8px 0;
  font-size: 1.1rem;
  font-weight: 600;
  color: var(--text, #e2e8f0);
}
.api-key-overlay-card p {
  color: var(--text2, #94a3b8);
  font-size: 0.85rem;
  line-height: 1.5;
  margin: 0 0 20px 0;
}
.api-key-overlay-card input {
  width: 100%;
  padding: 10px 14px;
  border-radius: 8px;
  border: 1px solid #374151;
  background: #1e293b;
  color: #e2e8f0;
  font-family: monospace;
  font-size: 0.85rem;
  outline: none;
  margin-bottom: 14px;
}
.api-key-overlay-card input:focus {
  border-color: #3b82f6;
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
}
.api-key-overlay-card .api-key-error {
  color: #f87171;
  font-size: 0.8rem;
  margin-bottom: 10px;
  display: none;
}
.api-key-overlay-card button {
  padding: 8px 20px;
  border-radius: 8px;
  font-size: 0.85rem;
  cursor: pointer;
  border: none;
  font-weight: 500;
  background: linear-gradient(135deg, #3b82f6, #8b5cf6);
  color: #fff;
  width: 100%;
}
.api-key-overlay-card button:hover { opacity: 0.9; }
.api-key-overlay-card button:disabled { opacity: 0.5; cursor: not-allowed; }

/* Settings sub-tab */
.chat-settings {
  flex: 1;
  overflow-y: auto;
  padding: 24px;
  background: var(--bg, #0d1117);
}
.settings-section {
  background: var(--surface, #161b22);
  border: 1px solid var(--border, #30363d);
  border-radius: 12px;
  padding: 20px 24px;
  margin-bottom: 16px;
}
.settings-section h3 {
  margin: 0 0 4px 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--text, #e2e8f0);
}
.settings-section .settings-desc {
  color: var(--text2, #8b949e);
  font-size: 0.82rem;
  line-height: 1.5;
  margin: 0 0 16px 0;
}
.settings-key-status {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 14px;
  font-size: 0.85rem;
}
.settings-key-status .key-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}
.settings-key-status .key-dot.set { background: #3fb950; }
.settings-key-status .key-dot.unset { background: #f85149; }
.settings-key-status .key-label {
  color: var(--text2, #8b949e);
}
.settings-key-status .key-masked {
  font-family: monospace;
  color: var(--text, #e2e8f0);
  font-size: 0.82rem;
}
.settings-key-input-row {
  display: flex;
  gap: 10px;
  align-items: stretch;
}
.settings-key-input-row input {
  flex: 1;
  padding: 9px 12px;
  border-radius: 8px;
  border: 1px solid #374151;
  background: #1e293b;
  color: #e2e8f0;
  font-family: monospace;
  font-size: 0.82rem;
  outline: none;
}
.settings-key-input-row input:focus {
  border-color: #3b82f6;
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
}
.settings-key-input-row .btn-settings-save {
  padding: 9px 18px;
  border-radius: 8px;
  border: none;
  font-size: 0.82rem;
  font-weight: 500;
  cursor: pointer;
  background: linear-gradient(135deg, #3b82f6, #8b5cf6);
  color: #fff;
  white-space: nowrap;
}
.settings-key-input-row .btn-settings-save:hover { opacity: 0.9; }
.settings-key-input-row .btn-settings-save:disabled { opacity: 0.5; cursor: not-allowed; }
.settings-key-error {
  color: #f87171;
  font-size: 0.8rem;
  margin-top: 8px;
  display: none;
}
.settings-key-success {
  color: #3fb950;
  font-size: 0.8rem;
  margin-top: 8px;
  display: none;
}
.settings-key-actions {
  margin-top: 12px;
  display: flex;
  gap: 10px;
}
.settings-key-actions button {
  padding: 6px 14px;
  border-radius: 6px;
  font-size: 0.78rem;
  cursor: pointer;
  border: 1px solid #374151;
  background: transparent;
  color: var(--text2, #8b949e);
}
/* Settings page (main view) */
.settings-page { max-width: 640px; margin: 0 auto; padding: 24px 16px; }
.settings-page-title { font-size: 1.2rem; font-weight: 600; margin: 0 0 20px 0; color: var(--text, #e2e8f0); }
.settings-model-select select { width: 100%; padding: 10px 12px; border-radius: 8px; border: 1px solid #374151; background: #1e293b; color: #e2e8f0; font-size: 0.85rem; cursor: pointer; }
.settings-model-select select:focus { border-color: #3b82f6; outline: none; }
.settings-model-note { font-size: 0.75rem; color: var(--text2, #8b949e); margin-top: 8px; }
.settings-key-row { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; font-size: 0.85rem; }
.settings-key-row .key-dot { width: 8px; height: 8px; border-radius: 50%; background: #f85149; flex-shrink: 0; }
.settings-key-row .key-dot.set { background: #3fb950; }
.btn-danger-sm { padding: 8px 12px; border-radius: 8px; border: 1px solid #f8717166; background: transparent; color: #f87171; font-size: 0.8rem; cursor: pointer; }
.btn-danger-sm:hover { background: #f8717122; }
.nav-settings { font-size: 1.1em !important; opacity: 0.7; }
.nav-settings:hover, .nav-settings.active { opacity: 1; }

/* File tags */
.file-tag { display: inline-block; font-size: 0.6rem; padding: 1px 5px; border-radius: 3px; margin-left: 4px; font-weight: 600; vertical-align: middle; letter-spacing: 0.3px; cursor: help; }
.ftip { position: fixed; background: #1c2028; color: #e6edf3; border: 1px solid #30363d; border-radius: 8px; padding: 10px 14px; font-size: 0.75rem; font-weight: 400; max-width: 320px; z-index: 10000; box-shadow: 0 8px 24px rgba(0,0,0,0.5); line-height: 1.5; letter-spacing: 0; pointer-events: none; }
.ftip b { color: #58a6ff; font-weight: 600; }
.type-tag { background: #1a2744; color: #60a5fa; border: 1px solid #1e3a5f; }
.build-tag { background: #1a3d1a; color: #4ade80; border: 1px solid #1a5c1a; }

/* Interpretation error banner */
.interp-error { background: rgba(251, 191, 36, 0.1); border: 1px solid rgba(251, 191, 36, 0.3); border-radius: 8px; padding: 8px 12px; margin-top: 8px; font-size: 0.78rem; color: #fbbf24; }
.interp-error a { color: #60a5fa; text-decoration: underline; }

.settings-key-actions button:hover {
  color: var(--text, #e2e8f0);
  border-color: #4b5563;
}
.settings-key-actions .btn-danger {
  border-color: rgba(248, 81, 73, 0.4);
  color: #f85149;
}
.settings-key-actions .btn-danger:hover {
  background: rgba(248, 81, 73, 0.1);
  border-color: #f85149;
}
.settings-info-link {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  color: #58a6ff;
  font-size: 0.8rem;
  text-decoration: none;
  margin-top: 4px;
}
.settings-info-link:hover { text-decoration: underline; }

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
        <a href="#/settings" data-view="settings" class="nav-settings">⚙</a>
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
    <div class="tests-tabs" id="testsTabs"></div>
    <div class="filter-bar" id="filterBar"></div>
    <div class="top-controls">
      <input type="text" class="search-box" id="searchBox" placeholder="Search tests..." oninput="filterTests()">
      <button onclick="expandAll()">Expand All</button>
      <button onclick="collapseAll()">Collapse All</button>
      <button class="run-all-btn" onclick="runAll()">Run All Tests</button>
      <button class="add-pgs-top-btn" onclick="openPgsModal()">+ Add PGS</button>
      <button class="clear-btn" onclick="clearQueue()">Clear Queue</button>
      <button onclick="openErrors()">Error Log</button>
      <button onclick="openTestEditor()">Edit</button>
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
  <div id="view-chat" class="view chat-view-wrap" style="position:relative">
    <!-- API Key first-run overlay (blocks chat until key is set) -->
    <div id="chatApiKeyOverlay" class="api-key-overlay" style="display:none">
      <div class="api-key-overlay-card">
        <h3>Anthropic API Key Required</h3>
        <p>To use the AI Assistant, enter your Anthropic API key below. Your key is encrypted at rest and never shared with anyone.</p>
        <div class="api-key-error" id="overlayKeyError"></div>
        <input type="password" id="overlayKeyInput" placeholder="sk-ant-api03-..." autocomplete="off"
               onkeydown="if(event.key==='Enter')overlayKeySave()">
        <button id="overlayKeySaveBtn" onclick="overlayKeySave()">Save &amp; Continue</button>
      </div>
    </div>
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
        <button class="chat-tab" id="chatTabTerminal" onclick="chatSwitchTab('terminal')">Terminal</button>
        <button class="chat-tab" id="chatTabChat" onclick="chatSwitchTab('chat')">Chat</button>
        <button class="chat-tab" id="chatTabSettings" onclick="chatSwitchTab('settings')">Settings</button>
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

      <!-- Settings sub-tab -->
      <div id="chatSubSettings" class="chat-sub" style="display:none">
        <div class="chat-settings">
          <div class="settings-section">
            <h3>Anthropic API Key</h3>
            <p class="settings-desc">
              Required for the AI Assistant. Your key is encrypted at rest and stored securely on the server.
              Each user provides their own key and is billed directly by Anthropic.
            </p>
            <div class="settings-key-status" id="settingsKeyStatus">
              <span class="key-dot unset" id="settingsKeyDot"></span>
              <span class="key-label" id="settingsKeyLabel">No key set</span>
              <span class="key-masked" id="settingsKeyMasked"></span>
            </div>
            <div class="settings-key-input-row">
              <input type="password" id="settingsKeyInput" placeholder="sk-ant-api03-..." autocomplete="off"
                     onkeydown="if(event.key==='Enter')settingsSaveKey()">
              <button class="btn-settings-save" id="settingsKeySaveBtn" onclick="settingsSaveKey()">Save Key</button>
            </div>
            <div class="settings-key-error" id="settingsKeyError"></div>
            <div class="settings-key-success" id="settingsKeySuccess"></div>
            <div class="settings-key-actions" id="settingsKeyActions" style="display:none">
              <button class="btn-danger" onclick="settingsRemoveKey()">Remove Key</button>
            </div>
            <a class="settings-info-link" href="https://console.anthropic.com/settings/keys" target="_blank" rel="noopener">
              Get an API key from Anthropic &rarr;
            </a>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>


  <!-- Settings view -->
  <div id="view-settings" class="view">
    <div class="settings-page">
      <h2 class="settings-page-title">⚙ Settings</h2>

      <div class="settings-section">
        <h3>AI Interpretation Model</h3>
        <p class="settings-desc">Choose which AI model generates the plain-English interpretation of test results.</p>
        <div class="settings-model-select">
          <select id="settingsInterpModel" onchange="saveInterpModel(this.value)">
            <option value="gemini">Gemini Flash (Vertex AI — free, default)</option>
            <option value="openai">OpenAI GPT-4o-mini</option>
            <option value="claude">Claude Sonnet (Anthropic)</option>
          </select>
        </div>
        <div class="settings-model-note" id="settingsModelNote"></div>
      </div>

      <div class="settings-section">
        <h3>OpenAI API Key</h3>
        <p class="settings-desc">Required if you select OpenAI for interpretations. Key is encrypted at rest.</p>
        <div class="settings-key-row" id="openaiKeyRow">
          <span class="key-dot" id="openaiKeyDot"></span>
          <span class="key-label" id="openaiKeyLabel">No key set</span>
          <span class="key-masked" id="openaiKeyMasked"></span>
        </div>
        <div class="settings-key-input-row">
          <input type="password" id="openaiKeyInput" placeholder="sk-..." autocomplete="off"
                 onkeydown="if(event.key==='Enter')saveProviderKey('openai')">
          <button class="btn-settings-save" onclick="saveProviderKey('openai')">Save</button>
          <button class="btn-danger-sm" onclick="removeProviderKey('openai')" id="openaiRemoveBtn" style="display:none">Remove</button>
        </div>
        <div class="settings-key-error" id="openaiKeyError"></div>
        <div class="settings-key-success" id="openaiKeySuccess"></div>
      </div>

      <div class="settings-section">
        <h3>Anthropic API Key</h3>
        <p class="settings-desc">Required if you select Claude for interpretations. Also powers the AI Assistant chat.</p>
        <div class="settings-key-row" id="claudeKeyRow">
          <span class="key-dot" id="claudeKeyDot"></span>
          <span class="key-label" id="claudeKeyLabel">No key set</span>
          <span class="key-masked" id="claudeKeyMasked"></span>
        </div>
        <div class="settings-key-input-row">
          <input type="password" id="claudeKeyInput" placeholder="sk-ant-api03-..." autocomplete="off"
                 onkeydown="if(event.key==='Enter')saveProviderKey('claude')">
          <button class="btn-settings-save" onclick="saveProviderKey('claude')">Save</button>
          <button class="btn-danger-sm" onclick="removeProviderKey('claude')" id="claudeRemoveBtn" style="display:none">Remove</button>
        </div>
        <div class="settings-key-error" id="claudeKeyError"></div>
        <div class="settings-key-success" id="claudeKeySuccess"></div>
      </div>

      <div class="settings-section">
        <h3>Gemini (Vertex AI)</h3>
        <p class="settings-desc">Uses the server's built-in Vertex AI credentials. No user key needed.</p>
        <div class="settings-key-row">
          <span class="key-dot set"></span>
          <span class="key-label" style="color: var(--text, #e2e8f0)">Active (server credential)</span>
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
    <div class="report-interpretation" id="reportInterpretation" style="display:none"></div>
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

<!-- Test Registry Editor Modal -->
<div class="modal-overlay" id="testEditorModal">
  <div class="modal" style="max-width: 1100px; max-height: 90vh; display: flex; flex-direction: column;">
    <div class="modal-header">
      <h2 id="editorTitle">Edit Test Registry</h2>
      <div style="display:flex;gap:8px;align-items:center;">
        <span id="editorStatus" style="font-size:0.8rem;color:var(--text2)"></span>
        <button onclick="saveTestEditor()" style="background:var(--accent);color:#fff;border:none;padding:6px 16px;border-radius:6px;cursor:pointer;font-size:0.85rem;">Save</button>
        <button class="modal-close" onclick="closeTestEditor()">&times;</button>
      </div>
    </div>
    <textarea id="testEditorArea" style="flex:1;width:100%;min-height:400px;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:monospace;font-size:0.82rem;resize:vertical;tab-size:2;" spellcheck="false"></textarea>
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

// ── Tab definitions ──────────────────────────────────────────────
const TAB_DEFS = {
  general: { label: 'General', categories: ['Sex Check', 'Sample QC', 'Ancestry'] },
  polygenic: { label: 'Polygenic Scores', categories: [
    'PGS - Cancer', 'PGS - Cardiovascular', 'PGS - Metabolic',
    'PGS - Autoimmune', 'PGS - Neurological', 'PGS - Traits',
    'PGS - Lifestyle', 'PGS - Custom', 'PGS - rsID Lists'] },
  monogenic: { label: 'Monogenic & Variants', categories: [
    'Monogenic', 'Carrier Status', 'Single Variants',
    'Fun Traits', 'Nutrigenomics', 'Sports & Fitness', 'Sleep & Circadian'] },
  pharmacogenomics: { label: 'Pharmacogenomics', categories: ['Pharmacogenomics'] },
};
const TAB_ORDER = ['general', 'polygenic', 'monogenic', 'pharmacogenomics'];
let activeTab = 'general';

function tabForCategory(cat) {
  for (const [key, def] of Object.entries(TAB_DEFS)) {
    if (def.categories.includes(cat)) return key;
  }
  return cat.startsWith('PGS') ? 'polygenic' : 'monogenic';
}

function renderTabs() {
  const el = document.getElementById('testsTabs');
  const vis = applyFilter(tests);
  el.innerHTML = TAB_ORDER.map(tk => {
    const def = TAB_DEFS[tk];
    const count = vis.filter(t => tabForCategory(t.category) === tk).length;
    const cls = tk === activeTab ? 'tests-tab active' : 'tests-tab';
    return `<button class="${cls}" onclick="switchTab('${tk}')">${def.label}<span class="tab-count">${count}</span></button>`;
  }).join('');
}

function switchTab(tk) {
  activeTab = tk;
  renderTabs();
  renderTests();
  renderFilters();
}

// ── Filter presets ──────────────────────────────────────────────
let activeFilter = null;

// Each filter defines which test IDs or patterns to EXCLUDE.
// 'match' is a function(test) => true if the test should be HIDDEN.
const FILTERS = {
  female: {
    label: 'Female',
    desc: 'Hide male-only tests (prostate, testicular, Y-chr, SRY)',
    match: t => {
      const n = (t.name + ' ' + t.description + ' ' + (t.params?.trait || '') + ' ' + t.id).toLowerCase();
      if (/prostat/.test(n)) return true;
      if (/testicul/.test(n)) return true;
      if (t.id === 'sex_y_reads' || t.id === 'sex_sry' || t.id === 'sex_xy_ratio' || t.id === 'sex_var_chry') return true;
      if (t.id === 'ancestry_y_haplo') return true;
      if (/testosterone.*(male|men)/.test(n)) return true;
      if (t.id === 'var_male_baldness' || /male.pattern.bald/.test(n)) return true;
      return false;
    }
  },
  male: {
    label: 'Male',
    desc: 'Hide female-only tests (breast, ovarian, endometrial, cervical)',
    match: t => {
      const n = (t.name + ' ' + t.description + ' ' + (t.params?.trait || '') + ' ' + t.id).toLowerCase();
      if (/breast/.test(n)) return true;
      if (/ovarian/.test(n)) return true;
      if (/endometri/.test(n)) return true;
      if (/cervical/.test(n)) return true;
      if (/uterine/.test(n)) return true;
      if (/menarche/.test(n)) return true;
      if (t.id === 'sex_het_chrx') return true;
      return false;
    }
  },
  pediatric: {
    label: 'Pediatric',
    desc: 'Focus on ages 0–15: carrier status, monogenic, QC, ancestry, ADHD, autism, asthma, T1D, epilepsy, celiac',
    match: t => {
      // KEEP: carrier, monogenic, QC, sex check, ancestry, pharmacogenomics
      const keepCats = ['Carrier Status', 'Monogenic', 'Sample QC', 'Sex Check', 'Ancestry', 'Pharmacogenomics'];
      if (keepCats.includes(t.category)) return false;
      // KEEP specific pediatric-relevant PGS/variants
      const keepIds = new Set([
        'fun_lactose', 'fun_norovirus', 'fun_blood_type', 'fun_eye_color', 'fun_earwax',
        'nutri_folate', 'nutri_vitd',
        'sleep_delayed', 'sleep_deep', 'sleep_caffeine',
        'var_mthfr', 'var_fto', 'var_a1at',
      ]);
      if (keepIds.has(t.id)) return false;
      const n = (t.name + ' ' + t.description + ' ' + (t.params?.trait || '')).toLowerCase();
      // KEEP pediatric-relevant polygenic scores
      const keepTraits = [
        'adhd', 'autism', 'asthma', 'type 1 diabetes', 'epilepsy', 'celiac',
        'atopic dermatitis', 'eczema', 'height', 'bmi', 'obesity',
        'myopia', 'intelligence', 'educational', 'neuroticism',
        'eye color', 'hair color', 'skin pigment',
      ];
      for (const kw of keepTraits) {
        if (n.includes(kw)) return false;
      }
      // HIDE everything else (adult-onset cancers, cardiovascular, etc.)
      return true;
    }
  },
  carrier: {
    label: 'Carrier Screen',
    desc: 'Show only carrier status, monogenic disease genes, and key single variants',
    match: t => {
      const keepCats = ['Carrier Status', 'Monogenic', 'Sample QC', 'Sex Check'];
      if (keepCats.includes(t.category)) return false;
      // Keep BRCA, Factor V, other clinically actionable variants
      if (t.category === 'Single Variants') return false;
      return true;
    }
  },
  actionable: {
    label: 'Actionable',
    desc: 'Focus on clinically actionable: pharmacogenomics, monogenic, carrier, BRCA, APOE, FVL, and top PGS',
    match: t => {
      const keepCats = ['Pharmacogenomics', 'Monogenic', 'Carrier Status', 'Single Variants', 'Sample QC', 'Sex Check', 'Ancestry'];
      if (keepCats.includes(t.category)) return false;
      // Keep the top/best PGS per major disease (first entry = usually best)
      const keepIds = new Set([
        'pgs_breast_335', 'pgs_prostate_662', 'pgs_colorectal_3850',
        'pgs_cad_3725', 'pgs_afib_016', 'pgs_hf_5097',
        'pgs_t2d_2780', 'pgs_t1d_2693',
        'pgs_alzheimer_2', 'pgs_pd_4245',
        'pgs_melanoma_743', 'pgs_lung_078',
        'pgs_stroke_2724', 'pgs_vte_043',
        'pgs_htn_4192',
      ]);
      if (keepIds.has(t.id)) return false;
      return true;
    }
  },
};
const FILTER_ORDER = ['female', 'male', 'pediatric', 'carrier', 'actionable'];

function applyFilter(testList) {
  if (!activeFilter || !FILTERS[activeFilter]) return testList;
  return testList.filter(t => !FILTERS[activeFilter].match(t));
}

function renderFilters() {
  const bar = document.getElementById('filterBar');
  const tabTests = tests.filter(t => tabForCategory(t.category) === activeTab);
  const activeLabel = activeFilter ? FILTERS[activeFilter].label : 'All';
  const btnCls = activeFilter ? 'filter-toggle-btn has-filter' : 'filter-toggle-btn';
  const popupOpen = bar.querySelector('.filter-popup.open') ? ' open' : '';

  const chips = FILTER_ORDER.map(fk => {
    const f = FILTERS[fk];
    const filtered = tabTests.filter(t => !f.match(t));
    const cls = fk === activeFilter ? 'filter-chip active' : 'filter-chip';
    return `<button class="${cls}" onclick="toggleFilter('${fk}')" title="${f.desc}">${f.label}<span class="chip-count">${filtered.length}</span></button>`;
  }).join('');

  const allCount = tabTests.length;
  const allCls = !activeFilter ? 'filter-chip active' : 'filter-chip';
  const allChip = `<button class="${allCls}" onclick="toggleFilter(null)">All<span class="chip-count">${allCount}</span></button>`;

  bar.innerHTML = `
    <button class="${btnCls}" onclick="toggleFilterPopup(event)">
      <span class="filter-icon">⚙</span> Filters: ${activeLabel}
    </button>
    <div class="filter-popup${popupOpen}">
      ${allChip}
      ${chips}
    </div>
  `;
}

function toggleFilterPopup(e) {
  e.stopPropagation();
  const popup = document.querySelector('.filter-popup');
  if (popup) popup.classList.toggle('open');
}

function toggleFilter(fk) {
  activeFilter = (activeFilter === fk) ? null : fk;
  // Close popup after selection
  const popup = document.querySelector('.filter-popup');
  if (popup) popup.classList.remove('open');
  renderFilters();
  renderTests();
  renderTabs();
}

// Close filter popup when clicking outside
document.addEventListener('click', function(e) {
  if (!e.target.closest('.filter-bar')) {
    const popup = document.querySelector('.filter-popup');
    if (popup) popup.classList.remove('open');
  }
});
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
  renderTabs();
  renderFilters();
  renderTests();
  await refreshFiles();
  pollStatus();
}

async function refreshFiles() {
  const oldStatuses = Object.fromEntries(files.map(f => [f.id, f.pgen_status]));
  const resp = await fetch(BASE + '/api/files');
  const data = await resp.json();
  files = data.files || [];
  activeFileId = data.active_file_id;
  renderFileSelect();
  updateVcfBadge();
  // If any pgen_status changed, re-render the data view
  const changed = files.some(f => oldStatuses[f.id] !== f.pgen_status);
  if (changed && currentView() === 'data') renderDataFiles();
}

// Special pseudo-file id for "All files" mode. Frontend-only — never
// sent to /api/files/<id>/select. Triggers cross-file behavior in the
// reports view (no filter) and run buttons (iterate over every file).
const ALL_FILES = '__all__';

function isAllMode() { return activeFileId === ALL_FILES; }

function _showFileSelectHint(html) {
  let hint = document.getElementById('fileSelectHint');
  if (!hint) {
    const container = document.querySelector('.header-active-file');
    if (!container) return;
    hint = document.createElement('div');
    hint.id = 'fileSelectHint';
    hint.className = 'file-select-hint';
    container.appendChild(hint);
  }
  hint.innerHTML = html;
  hint.style.display = html ? 'block' : 'none';
}

function renderFileSelect() {
  const sel = document.getElementById('fileSelect');
  // Only show files that are pgen-ready (or don't need pgen)
  const readyFiles = files.filter(f =>
    f.pgen_status === 'ready' || f.pgen_status === 'not_needed' || !f.pgen_status
  );
  if (readyFiles.length === 0) {
    const pending = files.filter(f => f.pgen_status === 'building' || f.pgen_status === 'pending');
    if (pending.length > 0) {
      sel.innerHTML = '<option value="">\u2014 preparing files\u2026 \u2014</option>';
      _showFileSelectHint('Files are being prepared. <a href="#" onclick="showView(\'data\'); return false;">Check progress in My Data</a>');
    } else if (files.length > 0) {
      sel.innerHTML = '<option value="">\u2014 files need preparation \u2014</option>';
      _showFileSelectHint('Your files need preparation before scoring. <a href="#" onclick="showView(\'data\'); return false;">Go to My Data</a> to prepare them.');
    } else {
      sel.innerHTML = '<option value="">\u2014 no file loaded \u2014</option>';
      _showFileSelectHint('');
    }
    return;
  }
  _showFileSelectHint('');
  const allOpt = `<option value="${ALL_FILES}" ${isAllMode() ? 'selected' : ''}>★ All files (${readyFiles.length})</option>`;
  const fileOpts = readyFiles.map(f => {
    const tags = [];
    if (f.file_type) tags.push(f.file_type.toUpperCase());
    if (f.genome_build) tags.push(f.genome_build);
    if (f.size) tags.push(formatSize(f.size));
    const tagStr = tags.length ? ' [' + tags.join(' \u00b7 ') + ']' : '';
    const label = f.name + tagStr;
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

function showToast(msg) {
  let t = document.getElementById('toast-msg');
  if (!t) {
    t = document.createElement('div');
    t.id = 'toast-msg';
    t.style.cssText = 'position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:#333;color:#fff;padding:10px 20px;border-radius:8px;font-size:0.8rem;z-index:9999;opacity:0;transition:opacity 0.3s;pointer-events:none;';
    document.body.appendChild(t);
  }
  t.textContent = msg;
  t.style.opacity = '1';
  clearTimeout(t._tid);
  t._tid = setTimeout(() => { t.style.opacity = '0'; }, 4000);
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
  const wasOpen = new Set();
  document.querySelectorAll('.tests-body.open').forEach(b => wasOpen.add(b.dataset.cat));
  container.innerHTML = '';

  const tabCats = categories.filter(c => tabForCategory(c) === activeTab);
  const filteredTests = applyFilter(tests);

  for (const cat of tabCats) {
    const catTests = filteredTests.filter(t => t.category === cat &&
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
          ${cat.startsWith('PGS') ? `<button class="cat-btn enrich-btn" onclick="event.stopPropagation(); refreshPgsCategory('${cat.replace(/'/g, "\\'")}')" id="enrich-btn-${slug}" title="Fetch metadata from PGS Catalog">&#x21bb; Enrich</button>` : ''}
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
        <h3>${t.name}${t.params&&t.params.pgs_id ? ' <a href="https://www.pgscatalog.org/score/'+t.params.pgs_id+'/" target="_blank" class="pgs-link" title="View on PGS Catalog">↗</a>' : ''}</h3>
        <p>${t.description.substring(0, 120)}${t.description.length > 120 ? '...' : ''}</p>
        ${t.enrichment ? `<div class="pgs-enrichment">
          ${t.enrichment.doi ? `<span class="enr-item"><a href="${t.enrichment.doi}" target="_blank" class="enr-link">${t.enrichment.citation || 'Publication'}</a></span>` : (t.enrichment.citation ? `<span class="enr-item">${t.enrichment.citation}</span>` : '')}
          ${t.enrichment.genome_build && t.enrichment.genome_build !== 'NR' ? `<span class="enr-chip">${t.enrichment.genome_build}</span>` : ''}
          ${t.enrichment.weight_type && t.enrichment.weight_type !== 'NR' ? `<span class="enr-chip">${t.enrichment.weight_type}</span>` : ''}
          ${t.enrichment.gwas_ancestry && t.enrichment.gwas_ancestry !== 'Not reported' ? `<span class="enr-item enr-ancestry">${t.enrichment.gwas_n ? t.enrichment.gwas_n.toLocaleString() + ' individuals' : ''} (${t.enrichment.gwas_ancestry})</span>` : ''}
          ${t.enrichment.trait_description ? `<span class="enr-desc">${t.enrichment.trait_description.substring(0, 150)}${t.enrichment.trait_description.length > 150 ? '...' : ''}</span>` : ''}
        </div>` : ''}
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

async function refreshPgsCategory(cat) {
  const slug = catSlug(cat);
  const btn = document.getElementById('enrich-btn-' + slug);
  if (btn) { btn.disabled = true; btn.textContent = '⟳ Refreshing...'; }

  try {
    const resp = await fetch(BASE + '/api/pgs/refresh/' + encodeURIComponent(cat), { method: 'POST' });
    const data = await resp.json();
    if (data.status === 'already_running') {
      showToast('Refresh already in progress for ' + cat);
    } else if (data.status === 'started') {
      showToast('Enrichment started: ' + data.total + ' tests in ' + cat);
      pollRefreshStatus(cat, slug);
    }
  } catch (e) {
    showToast('Error starting refresh: ' + e.message);
    if (btn) { btn.disabled = false; btn.textContent = '↻ Enrich'; }
  }
}

async function pollRefreshStatus(cat, slug) {
  const btn = document.getElementById('enrich-btn-' + slug);
  const poll = async () => {
    try {
      const resp = await fetch(BASE + '/api/pgs/refresh/' + encodeURIComponent(cat) + '/status');
      const data = await resp.json();
      if (data.status === 'running') {
        if (btn) btn.textContent = `⟳ ${data.progress}/${data.total}`;
        setTimeout(poll, 2000);
      } else if (data.status === 'completed') {
        if (btn) { btn.disabled = false; btn.textContent = '↻ Enrich'; }
        const errCount = (data.errors || []).length;
        showToast(`Enrichment done: ${data.total - errCount}/${data.total} updated` + (errCount ? ` (${errCount} errors)` : ''));
        // Reload tests to show new enrichment data
        const r = await fetch(BASE + '/api/tests');
        const d = await r.json();
        tests = d.tests;
        categories = d.categories;
        renderTests();
      } else {
        if (btn) { btn.disabled = false; btn.textContent = '↻ Enrich'; }
        showToast('Refresh ended: ' + (data.error || data.status));
      }
    } catch (e) {
      if (btn) { btn.disabled = false; btn.textContent = '↻ Enrich'; }
    }
  };
  setTimeout(poll, 2000);
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
    const tabLabel = TAB_DEFS[activeTab]?.label || 'all';
    const tabTests = tests.filter(t => tabForCategory(t.category) === activeTab);
    const total = tabTests.length * files.length;
    if (!confirm(
          `Queue every ${tabLabel} test against every file?\n\n` +
          `${tabTests.length} tests × ${files.length} files = ${total} tasks. ` +
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

// ── Test Registry Editor ─────────────────────────────────────────
async function openTestEditor() {
  const modal = document.getElementById('testEditorModal');
  const area = document.getElementById('testEditorArea');
  const status = document.getElementById('editorStatus');
  document.getElementById('editorTitle').textContent = 'Edit: ' + (TAB_DEFS[activeTab]?.label || 'Tests');
  status.textContent = 'Loading…';
  modal.classList.add('open');
  try {
    const resp = await fetch(BASE + '/api/tests/markdown?tab=' + activeTab);
    const data = await resp.json();
    if (data.ok) {
      area.value = data.markdown;
      status.textContent = data.test_count + ' tests loaded';
    } else {
      status.textContent = 'Error: ' + (data.error || 'unknown');
    }
  } catch (e) {
    status.textContent = 'Network error';
  }
}

function closeTestEditor() {
  document.getElementById('testEditorModal').classList.remove('open');
}

async function saveTestEditor() {
  const area = document.getElementById('testEditorArea');
  const status = document.getElementById('editorStatus');
  const md = area.value;
  status.textContent = 'Saving…';
  try {
    const resp = await fetch(BASE + '/api/tests/markdown?tab=' + activeTab, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ markdown: md }),
    });
    const data = await resp.json();
    if (data.ok) {
      status.textContent = 'Saved! ' + data.test_count + ' tests, ' + data.categories + ' categories';
      const resp2 = await fetch(BASE + '/api/tests');
      const d2 = await resp2.json();
      tests = d2.tests;
      categories = d2.categories;
      renderTabs();
      renderTests();
    } else {
      status.textContent = 'Error: ' + (data.error || 'save failed');
    }
  } catch (e) {
    status.textContent = 'Network error';
  }
}

document.getElementById('testEditorModal').addEventListener('click', function(e) {
  if (e.target === this) closeTestEditor();
});

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
const VIEWS = ['tests', 'data', 'reports', 'chat', 'settings'];

function currentView() {
  const h = (window.location.hash || '').replace(/^#\/?/, '');
  return VIEWS.includes(h) ? h : 'chat';
}

function showView(name) {
  if (!VIEWS.includes(name)) name = 'chat';
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
  if (name === 'settings') loadSettingsView();
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
  // Mobile: tap file-tag to show tooltip
  if (e.target.classList.contains('file-tag') && e.target.dataset.tip) {
    e.stopPropagation();
    _showFileTip(e.target);
  } else {
    _hideFileTip();
  }
});

// File tag tooltip system
const _tagTips = {
  'BAM': '<b>BAM</b> — Binary Alignment Map. Aligned sequencing reads in binary format. Used as input for variant calling.',
  'CRAM': '<b>CRAM</b> — Compressed Alignment Map. Like BAM but smaller. Requires a reference FASTA to decode.',
  'VCF': '<b>VCF</b> — Variant Call Format. Contains only variant positions (SNPs, indels). Ready for annotation.',
  'gVCF': '<b>gVCF</b> — Genomic VCF. Contains variants AND reference-confidence blocks. Used for joint calling.',
  'GVCF': '<b>gVCF</b> — Genomic VCF. Contains variants AND reference-confidence blocks. Used for joint calling.',
  'GRCh38': '<b>GRCh38</b> (hg38) — Current standard human reference genome (2013). Used by most modern pipelines.',
  'GRCh37': '<b>GRCh37</b> (hg19) — Previous standard (2009). Coordinates differ from GRCh38. Needs liftover.',
  'hg18': '<b>hg18</b> (NCBI36) — Legacy build (2006). Rarely used. Requires liftover to GRCh38.',
  'chr1-style': '<b>chr-prefixed</b> chromosomes (chr1, chr2, chr3&hellip;). Files being merged must use the same convention — mismatched naming causes silent data loss.',
  '1-style': '<b>Numeric</b> chromosomes (1, 2, 3&hellip;). Files being merged must use the same convention — mismatched naming causes silent data loss.',
  'ILLUMINA': '<b>Illumina</b> — Short-read sequencing (typically 150bp paired-end). High per-base accuracy (~Q30+). Best for SNP/indel calling.',
  'ONT': '<b>Oxford Nanopore</b> — Long-read sequencing. Lower per-base accuracy but resolves structural variants and methylation.',
  'PACBIO': '<b>PacBio HiFi</b> — Long reads with high accuracy. Best for phasing and structural variant detection.',
};
function _tipFor(tag) {
  const txt = (tag.textContent || '').trim();
  if (_tagTips[txt]) return _tagTips[txt];
  const cl = tag.dataset.tipkey;
  if (cl && _tagTips[cl]) return _tagTips[cl];
  // Dynamic tips based on content patterns
  if (/^\d+(\.\d+)?x$/.test(txt)) return '<b>Coverage: ' + txt + '</b><br>Mean genome-wide sequencing depth (mapped reads &times; read length &divide; genome size).<br><b>30x</b> = clinical WGS standard &nbsp; <b>50x+</b> = high coverage &nbsp; <b>&lt;10x</b> = low-pass.';
  if (/^\d+bp$/.test(txt)) return '<b>Read length: ' + txt + '</b><br>Average sequencing read length from the first reads.<br><b>150bp</b> = standard Illumina &nbsp; <b>250bp</b> = MiSeq &nbsp; <b>50-77bp</b> = older/exome.';
  if (/vars$/.test(txt)) return '<b>Variants: ' + txt.replace(' vars','') + '</b><br>Total variant records in the file.<br><b>4-5M</b> = typical WGS &nbsp; <b>30-80K</b> = exome.';
  if (/bwa|minimap|dragen/i.test(txt)) return '<b>Aligner: ' + txt + '</b><br>Software that mapped reads to the reference. Detected from BAM @PG header.<br><b>bwa</b> = standard short-read &nbsp; <b>minimap2</b> = long-read &nbsp; <b>DRAGEN</b> = Illumina hardware.';
  if (/deepvariant|gatk|haplotype/i.test(txt)) return '<b>Variant caller: ' + txt + '</b><br>Software that identified variants. Detected from VCF header.<br><b>DeepVariant</b> = Google AI caller &nbsp; <b>GATK</b> = Broad standard &nbsp; <b>DRAGEN</b> = Illumina hardware.';
  return null;
}
let _ftipEl = null;
function _showFileTip(tag) {
  _hideFileTip();
  const html = _tipFor(tag);
  if (!html) return;
  _ftipEl = document.createElement('div');
  _ftipEl.className = 'ftip';
  _ftipEl.innerHTML = html;
  document.body.appendChild(_ftipEl);
  const r = tag.getBoundingClientRect();
  let top = r.top - _ftipEl.offsetHeight - 8;
  let left = r.left + r.width / 2 - _ftipEl.offsetWidth / 2;
  if (top < 4) top = r.bottom + 8;
  if (left < 4) left = 4;
  if (left + _ftipEl.offsetWidth > window.innerWidth - 4) left = window.innerWidth - _ftipEl.offsetWidth - 4;
  _ftipEl.style.top = top + 'px';
  _ftipEl.style.left = left + 'px';
}
function _hideFileTip() {
  if (_ftipEl) { _ftipEl.remove(); _ftipEl = null; }
}
document.addEventListener('scroll', _hideFileTip, true);
document.addEventListener('mouseover', function(e) {
  if (e.target.classList.contains('file-tag')) _showFileTip(e.target);
});
document.addEventListener('mouseout', function(e) {
  if (e.target.classList.contains('file-tag')) _hideFileTip();
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
    const ps = f.pgen_status || 'pending';
    const statusBadge = ps === 'ready' || ps === 'not_needed'
      ? '<span class="pgen-badge ready">Ready</span>'
      : ps === 'building'
        ? '<span class="pgen-badge building">Preparing…</span>'
        : '<span class="pgen-badge pending">Needs prep</span>';
    const prepBtn = (ps === 'pending' || ps === 'failed')
      ? `<button class="file-btn" onclick="prepareFile('${f.id}')">Prepare</button><span class="prep-help" tabindex="0">?<span class="prep-tooltip">Builds a variant index (pgen cache) for this file. Required before running PGS tests. Plain VCF files take a few seconds; gVCF files (with reference blocks) take 5\u201315 minutes on first run as each chromosome is normalized. Once built, the cache is permanent and reused for all future tests.</span></span>`
      : '';
    return `
      <div class="data-files-row ${isActive ? 'active-row' : ''}">
        <div class="df-name" title="${escapeHtml(f.path || '')}">
          ${escapeHtml(f.name || '')} ${statusBadge}
          ${f.file_type ? '<span class="file-tag type-tag">' + f.file_type.toUpperCase() + '</span>' : ''}
          ${f.genome_build ? '<span class="file-tag build-tag">' + escapeHtml(f.genome_build) + '</span>' : ''}
          ${f.chr_naming ? '<span class="file-tag" style="background:rgba(88,166,255,0.15);color:#58a6ff">' + (f.chr_naming === 'chr' ? 'chr1-style' : f.chr_naming === 'numeric' ? '1-style' : escapeHtml(f.chr_naming)) + '</span>' : ''}
          ${f.platform ? '<span class="file-tag" style="background:rgba(188,140,255,0.15);color:#bc8cff">' + escapeHtml(f.platform) + '</span>' : ''}
          ${f.est_coverage ? '<span class="file-tag" style="background:rgba(255,123,114,0.15);color:#ff7b72">' + escapeHtml(f.est_coverage) + '</span>' : ''}
          ${f.read_length ? '<span class="file-tag" style="background:rgba(255,159,67,0.12);color:#ff9f43">' + escapeHtml(f.read_length) + '</span>' : ''}
          ${f.variant_count ? '<span class="file-tag" style="background:rgba(86,211,100,0.12);color:#56d364">' + Number(f.variant_count).toLocaleString() + ' vars</span>' : ''}
          ${f.aligner ? '<span class="file-tag" style="background:rgba(139,148,158,0.12);color:#8b949e;font-size:0.55rem">' + escapeHtml(f.aligner) + '</span>' : ''}
          ${f.variant_caller ? '<span class="file-tag" style="background:rgba(139,148,158,0.12);color:#8b949e;font-size:0.55rem">' + escapeHtml(f.variant_caller) + '</span>' : ''}
        </div>
        <div class="df-size">${formatSize(f.size)}</div>
        <div class="df-src">${escapeHtml(f.source || '')}</div>
        <div class="df-added">${escapeHtml(when)}</div>
        <div class="df-actions">
          ${prepBtn}
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

async function prepareFile(fileId) {
  const resp = await fetch(BASE + `/api/files/${fileId}/prepare`, { method: 'POST' });
  const data = await resp.json();
  if (data.pgen_status === 'building') {
    fmStatus('Building variant cache… This may take a few minutes.', 'info');
  } else if (data.pgen_status === 'ready') {
    fmStatus('File is ready for PGS scoring.', 'ok');
  }
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

  // LLM interpretation (if available)
  const interpEl = document.getElementById('reportInterpretation');
  if (report.interpretation) {
    interpEl.innerHTML = '<h4>AI Interpretation</h4><p>' + escapeHtml(report.interpretation) + '</p>';
    interpEl.style.display = 'block';
  } else if (report.interpretation_error) {
    interpEl.innerHTML = '<div class="interp-error">\u26a0 AI interpretation unavailable: ' + escapeHtml(report.interpretation_error) +
      ' <a href="#/settings" onclick="showView(\'settings\'); closeModal(); return false;">Check Settings</a></div>';
    interpEl.style.display = 'block';
  } else {
    interpEl.style.display = 'none';
  }

  const content = document.getElementById('reportContent');

  if (result.no_report) {
    content.innerHTML = `<div class="report-section error-section">
      <h4>Score Failed</h4>
      <p>Match rate too low (${escapeHtml(result.match_rate || 'unknown')}) — ${result.matched_variants || 0}/${result.total_variants || result.n_variants || 0} variants matched.</p>
      <p>No percentile computed because the score would not be reliable.</p>
    </div>`;
    document.getElementById('reportModal').classList.add('open');
    return;
  }

  let html = '';

  // ── PGS Score Summary ──
  if (result.test_type === 'pgs_score' && result.pgs_id) {
    html += '<div class="report-section">';
    html += `<h4>Score Result</h4>`;
    html += '<div class="score-grid">';
    html += `<div class="score-item"><label>Raw Score</label><span>${result.raw_score != null ? result.raw_score.toExponential(4) : 'N/A'}</span></div>`;
    html += `<div class="score-item"><label>Matched Variants</label><span>${(result.matched_variants || 0).toLocaleString()} / ${(result.total_variants || 0).toLocaleString()}</span></div>`;
    html += `<div class="score-item"><label>Match Rate</label><span>${escapeHtml(result.match_rate || 'N/A')}</span></div>`;
    if (result.percentile != null) {
      html += `<div class="score-item"><label>Percentile (EUR)</label><span class="pctl-value">${result.percentile}%</span></div>`;
    }
    if (result.genome_build) {
      html += `<div class="score-item"><label>Genome Build</label><span>${escapeHtml(result.genome_build)}</span></div>`;
    }
    if (result.scoring_file_source) {
      html += `<div class="score-item"><label>Scoring File</label><span>${escapeHtml(result.scoring_file_source)}</span></div>`;
    }
    if (result.build_notes) {
      html += `<div class="score-item build-note"><label>Build Notes</label><span>${escapeHtml(result.build_notes)}</span></div>`;
    }
    if (result.sample_id) {
      html += `<div class="score-item"><label>Sample</label><span>${escapeHtml(result.sample_id)}</span></div>`;
    }
    html += '</div></div>';
  }

  // ── Pipeline Info ──
  const pi = result.pipeline_info;
  if (pi) {
    html += '<div class="report-section">';
    html += '<h4>Pipeline Details</h4>';
    html += '<div class="pipeline-grid">';
    html += `<div class="pipe-item"><label>Scoring Tool</label><span>${escapeHtml(pi.scoring_tool || '')}</span></div>`;
    html += `<div class="pipe-item"><label>Method</label><span>${escapeHtml(pi.scoring_method || '')}</span></div>`;
    html += `<div class="pipe-item"><label>Input File</label><span>${escapeHtml(pi.input_file || '')}</span></div>`;
    html += `<div class="pipe-item"><label>Input Type</label><span>${escapeHtml(pi.input_type || '')}</span></div>`;
    html += `<div class="pipe-item"><label>Genome Build</label><span>${escapeHtml(pi.genome_build || '')}</span></div>`;
    if (pi.scoring_file_build) {
      html += `<div class="pipe-item"><label>Scoring File Build</label><span>${escapeHtml(pi.scoring_file_build)}</span></div>`;
    }
    if (pi.scoring_file_source) {
      html += `<div class="pipe-item"><label>Scoring File</label><span>${escapeHtml(pi.scoring_file_source)}</span></div>`;
    }
    if (pi.liftover_applied) {
      html += `<div class="pipe-item"><label>Liftover</label><span>${escapeHtml(pi.liftover_applied)}</span></div>`;
    }
    if (pi.build_notes) {
      html += `<div class="pipe-item"><label>Build Notes</label><span>${escapeHtml(pi.build_notes)}</span></div>`;
    }
    html += `<div class="pipe-item"><label>Normalization</label><span>${escapeHtml(pi.normalization || 'none')}</span></div>`;
    html += `<div class="pipe-item"><label>Reference Population</label><span>${escapeHtml(pi.reference_population || '')}</span></div>`;
    html += `<div class="pipe-item"><label>Reference Panel</label><span>${escapeHtml(pi.reference_panel || '')}</span></div>`;
    if (pi.pgs_catalog_url) {
      html += `<div class="pipe-item"><label>PGS Catalog</label><span><a href="${escapeHtml(pi.pgs_catalog_url)}" target="_blank" rel="noopener">${escapeHtml(pi.pgs_catalog_id || pi.pgs_catalog_url)}</a></span></div>`;
    }
    html += '</div>';
    // Percentile computation details
    const pd = pi.percentile_details;
    if (pd && pd.method) {
      html += '<div class="pctl-details">';
      html += `<strong>Percentile Method:</strong> ${escapeHtml(pd.method)}`;
      if (pd.ref_mean != null) html += ` — ref μ=${pd.ref_mean.toFixed(6)}, σ=${pd.ref_std.toFixed(6)}`;
      if (pd.z_score != null) html += `, z=${pd.z_score.toFixed(3)}`;
      if (pd.description) html += `<br><small>${escapeHtml(pd.description)}</small>`;
      html += '</div>';
    }
    html += '</div>';
  }

  // ── Scoring Diagnostics ──
  const sd = result.scoring_diagnostics;
  if (sd) {
    const sanityClass = sd.z_sanity === 'ok' ? 'ok' : sd.z_sanity === 'warn_extreme' ? 'warn' : 'fail';
    html += '<div class="report-section">';
    html += `<h4>Scoring Diagnostics <span class="diag-badge ${sanityClass}">${sd.z_sanity || 'ok'}</span></h4>`;
    html += '<div class="diag-grid">';
    if (sd.z_score != null) html += `<div class="diag-item"><label>Z-Score</label><span>${sd.z_score.toFixed(3)}</span></div>`;
    if (sd.ref_mean != null) html += `<div class="diag-item"><label>Ref Mean</label><span>${sd.ref_mean.toFixed(6)}</span></div>`;
    if (sd.ref_std != null) html += `<div class="diag-item"><label>Ref Std</label><span>${sd.ref_std.toFixed(6)}</span></div>`;
    if (sd.method_used) html += `<div class="diag-item"><label>Method</label><span>${escapeHtml(sd.method_used)}</span></div>`;
    if (sd.ref_variants_matched != null) html += `<div class="diag-item"><label>Ref Panel Matched</label><span>${sd.ref_variants_matched.toLocaleString()}</span></div>`;
    html += `<div class="diag-item"><label>Cross-validated</label><span>${sd.method_cross_validated ? 'Yes' : 'No'}</span></div>`;
    html += '</div>';
    if (sd.sanity_gates_tripped && sd.sanity_gates_tripped.length) {
      html += '<div class="sanity-gates">';
      for (const g of sd.sanity_gates_tripped) html += `<div class="gate-trip">⚠ ${escapeHtml(g)}</div>`;
      html += '</div>';
    }
    html += '</div>';
  }

  // ── APOE Status ──
  if (result.apoe_status) {
    html += '<div class="report-section">';
    html += `<h4>APOE Genotype</h4>`;
    html += `<p><strong>${escapeHtml(result.apoe_status.genotype)}</strong> — Risk: ${escapeHtml(result.apoe_status.risk)}</p>`;
    html += '</div>';
  }

  // ── Variant Details (non-PGS tests) ──
  if (result.variants && result.variants.length) {
    html += '<div class="report-section">';
    html += '<h4>Variant Details</h4>';
    html += '<table class="variant-table"><tr><th>Gene</th><th>Variant</th><th>Genotype</th></tr>';
    for (const v of result.variants) {
      html += `<tr><td>${escapeHtml(v.gene || '')}</td><td>${escapeHtml(v.name || '')} (${escapeHtml(v.variant || '')})</td><td>${v.found ? escapeHtml(v.genotype || '') : 'Not found (ref/ref)'}</td></tr>`;
    }
    html += '</table></div>';
  }

  // ── Pathogenic Findings ──
  if (result.findings && result.findings.length > 0) {
    html += '<div class="report-section findings-section">';
    html += '<h4>Pathogenic Findings</h4>';
    html += '<table class="variant-table"><tr><th>Gene</th><th>Position</th><th>Change</th><th>Significance</th><th>Genotype</th></tr>';
    for (const f of result.findings) {
      html += `<tr><td>${escapeHtml(f.gene || '')}</td><td>${f.chrom}:${f.pos}</td><td>${escapeHtml(f.ref || '')}>${escapeHtml(f.alt || '')}</td><td>${escapeHtml(f.clnsig || '')}</td><td>${escapeHtml(f.genotype || '')}</td></tr>`;
    }
    html += '</table></div>';
  }

  // ── Errors ──
  if (result.error) {
    html += `<div class="report-section error-section"><h4>Error</h4><p>${escapeHtml(result.error)}</p></div>`;
  }

  // ── Raw JSON (collapsible) ──
  html += '<details class="raw-json-section"><summary>Raw JSON</summary>';
  html += `<pre>${escapeHtml(JSON.stringify(result, null, 2))}</pre>`;
  html += '</details>';

  content.innerHTML = html;
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
  window.sgHasApiKey = !!me.has_api_key;
  window.sgMaskedApiKey = me.masked_api_key || null;

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
  sub: 'terminal',       // 'terminal' | 'chat'
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
  if (!window.sgHasApiKey) {
    chatSwitchTab('settings');
    return;
  }
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
    } else if (r.status === 403) {
      // API key missing or invalid on the server — reset and show settings
      window.sgHasApiKey = false;
      window.sgMaskedApiKey = null;
      updateApiKeyOverlay();
      settingsRefreshKeyUI();
      chatState.messages.push({
        role: 'assistant',
        text: 'Please set your Anthropic API key in the Settings tab to use the AI Assistant.',
        ts: Date.now() / 1000,
      });
      chatRenderMessages();
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
  document.getElementById('chatSubSettings').style.display = name === 'settings' ? '' : 'none';
  document.getElementById('chatTabChat').classList.toggle('active', name === 'chat');
  document.getElementById('chatTabTerminal').classList.toggle('active', name === 'terminal');
  document.getElementById('chatTabSettings').classList.toggle('active', name === 'settings');

  // Manage terminal polling
  if (name === 'terminal') {
    chatRawLoadFull();
    if (chatState.rawPollHandle) clearInterval(chatState.rawPollHandle);
    chatState.rawPollHandle = setInterval(chatRawPollTail, 1500);
  } else if (chatState.rawPollHandle) {
    clearInterval(chatState.rawPollHandle);
    chatState.rawPollHandle = null;
  }
  // Refresh settings display when switching to settings
  if (name === 'settings') {
    settingsRefreshKeyUI();
  }
}

// ─── API Key management ─────────────────────────────────────────
window.sgHasApiKey = false;
window.sgMaskedApiKey = null;

// Show/hide the first-run blocking overlay
function updateApiKeyOverlay() {
  const overlay = document.getElementById('chatApiKeyOverlay');
  if (!overlay) return;
  overlay.style.display = window.sgHasApiKey ? 'none' : 'flex';
}

// ─── First-run overlay save ─────────────────────────────────────
async function overlayKeySave() {
  const input = document.getElementById('overlayKeyInput');
  const errEl = document.getElementById('overlayKeyError');
  const saveBtn = document.getElementById('overlayKeySaveBtn');
  const key = (input ? input.value : '').trim();

  if (!key) {
    if (errEl) { errEl.textContent = 'Please enter your API key'; errEl.style.display = 'block'; }
    return;
  }
  if (!key.startsWith('sk-ant-')) {
    if (errEl) { errEl.textContent = 'Key must start with sk-ant-'; errEl.style.display = 'block'; }
    return;
  }

  if (saveBtn) saveBtn.disabled = true;
  if (errEl) errEl.style.display = 'none';

  try {
    const r = await fetch(BASE + '/api/auth/api-key', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: key }),
    });
    const data = await r.json();
    if (data && data.ok) {
      window.sgHasApiKey = true;
      window.sgMaskedApiKey = data.masked_key || null;
      if (input) input.value = '';
      updateApiKeyOverlay();
      settingsRefreshKeyUI();
    } else {
      if (errEl) {
        errEl.textContent = (data && data.error) || 'Failed to save key';
        errEl.style.display = 'block';
      }
    }
  } catch (e) {
    if (errEl) {
      errEl.textContent = 'Network error: ' + e.message;
      errEl.style.display = 'block';
    }
  } finally {
    if (saveBtn) saveBtn.disabled = false;
  }
}

// ─── Settings tab: key display ──────────────────────────────────
function settingsRefreshKeyUI() {
  const dot = document.getElementById('settingsKeyDot');
  const label = document.getElementById('settingsKeyLabel');
  const masked = document.getElementById('settingsKeyMasked');
  const actions = document.getElementById('settingsKeyActions');
  const input = document.getElementById('settingsKeyInput');
  const errEl = document.getElementById('settingsKeyError');
  const successEl = document.getElementById('settingsKeySuccess');

  if (errEl) errEl.style.display = 'none';
  if (successEl) successEl.style.display = 'none';
  if (input) input.value = '';

  if (window.sgHasApiKey) {
    if (dot) { dot.classList.remove('unset'); dot.classList.add('set'); }
    if (label) label.textContent = 'Active:';
    if (masked) masked.textContent = window.sgMaskedApiKey || 'set';
    if (actions) actions.style.display = '';
    if (input) input.placeholder = 'Enter new key to replace...';
  } else {
    if (dot) { dot.classList.remove('set'); dot.classList.add('unset'); }
    if (label) label.textContent = 'No key set';
    if (masked) masked.textContent = '';
    if (actions) actions.style.display = 'none';
    if (input) input.placeholder = 'sk-ant-api03-...';
  }
}

async function settingsSaveKey() {
  const input = document.getElementById('settingsKeyInput');
  const errEl = document.getElementById('settingsKeyError');
  const successEl = document.getElementById('settingsKeySuccess');
  const saveBtn = document.getElementById('settingsKeySaveBtn');
  const key = (input ? input.value : '').trim();

  if (successEl) successEl.style.display = 'none';
  if (!key) {
    if (errEl) { errEl.textContent = 'Please enter your API key'; errEl.style.display = 'block'; }
    return;
  }
  if (!key.startsWith('sk-ant-')) {
    if (errEl) { errEl.textContent = 'Key must start with sk-ant-'; errEl.style.display = 'block'; }
    return;
  }

  if (saveBtn) saveBtn.disabled = true;
  if (errEl) errEl.style.display = 'none';

  try {
    const r = await fetch(BASE + '/api/auth/api-key', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: key }),
    });
    const data = await r.json();
    if (data && data.ok) {
      window.sgHasApiKey = true;
      window.sgMaskedApiKey = data.masked_key || null;
      settingsRefreshKeyUI();
      updateApiKeyOverlay();
      if (successEl) {
        successEl.textContent = 'API key saved. The AI session will restart with the new key.';
        successEl.style.display = 'block';
        setTimeout(() => { successEl.style.display = 'none'; }, 5000);
      }
    } else {
      if (errEl) {
        errEl.textContent = (data && data.error) || 'Failed to save key';
        errEl.style.display = 'block';
      }
    }
  } catch (e) {
    if (errEl) {
      errEl.textContent = 'Network error: ' + e.message;
      errEl.style.display = 'block';
    }
  } finally {
    if (saveBtn) saveBtn.disabled = false;
  }
}

async function settingsRemoveKey() {
  if (!confirm('Remove your API key? The AI Assistant will stop working until you set a new one.')) return;
  const errEl = document.getElementById('settingsKeyError');
  const successEl = document.getElementById('settingsKeySuccess');

  try {
    const r = await fetch(BASE + '/api/auth/api-key', { method: 'DELETE' });
    const data = await r.json();
    if (data && data.ok) {
      window.sgHasApiKey = false;
      window.sgMaskedApiKey = null;
      settingsRefreshKeyUI();
      updateApiKeyOverlay();
      if (successEl) {
        successEl.textContent = 'API key removed.';
        successEl.style.display = 'block';
        setTimeout(() => { successEl.style.display = 'none'; }, 4000);
      }
    }
  } catch (e) {
    if (errEl) {
      errEl.textContent = 'Failed to remove key: ' + e.message;
      errEl.style.display = 'block';
    }
  }
}

function chatViewActivated() {
  if (chatState.active) return;
  chatState.active = true;
  updateApiKeyOverlay();
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
  // Default to the terminal sub-tab on first activation
  if (!document.getElementById('chatTabTerminal').classList.contains('active')) {
    chatSwitchTab('terminal');
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


// ─── Settings view ──────────────────────────────────────────────
async function loadSettingsView() {
  try {
    const r = await fetch(BASE + '/api/settings');
    if (!r.ok) return;
    const data = await r.json();

    // Model selector
    const sel = document.getElementById('settingsInterpModel');
    if (sel) sel.value = data.interp_model || 'gemini';

    // Key status for each provider
    for (const provider of ['openai', 'claude']) {
      const info = (data.keys || {})[provider] || {};
      const dot = document.getElementById(provider + 'KeyDot');
      const label = document.getElementById(provider + 'KeyLabel');
      const masked = document.getElementById(provider + 'KeyMasked');
      const removeBtn = document.getElementById(provider + 'RemoveBtn');
      if (info.has_key) {
        if (dot) dot.classList.add('set');
        if (label) label.textContent = 'Active:';
        if (masked) masked.textContent = info.masked || 'set';
        if (removeBtn) removeBtn.style.display = '';
      } else {
        if (dot) dot.classList.remove('set');
        if (label) label.textContent = 'No key set';
        if (masked) masked.textContent = '';
        if (removeBtn) removeBtn.style.display = 'none';
      }
    }

    updateModelNote();
  } catch (e) {
    console.error('Failed to load settings:', e);
  }
}

function updateModelNote() {
  const sel = document.getElementById('settingsInterpModel');
  const note = document.getElementById('settingsModelNote');
  if (!sel || !note) return;
  const v = sel.value;
  if (v === 'gemini') {
    note.textContent = 'Uses the server Vertex AI credential. No API key needed from you.';
  } else if (v === 'openai') {
    note.textContent = 'Requires your OpenAI API key below. Uses GPT-4o-mini (~$0.001/interpretation).';
  } else if (v === 'claude') {
    note.textContent = 'Requires your Anthropic API key below. Uses Claude Sonnet (~$0.002/interpretation).';
  }
}

async function saveInterpModel(model) {
  updateModelNote();
  try {
    await fetch(BASE + '/api/settings/interp-model', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model }),
    });
  } catch (e) {
    console.error('Failed to save model:', e);
  }
}

async function saveProviderKey(provider) {
  const input = document.getElementById(provider + 'KeyInput');
  const errEl = document.getElementById(provider + 'KeyError');
  const successEl = document.getElementById(provider + 'KeySuccess');
  const key = (input ? input.value : '').trim();
  if (errEl) errEl.style.display = 'none';
  if (successEl) successEl.style.display = 'none';
  if (!key) {
    if (errEl) { errEl.textContent = 'Please enter your API key'; errEl.style.display = 'block'; }
    return;
  }
  try {
    const r = await fetch(BASE + '/api/settings/provider-key', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ provider, key }),
    });
    const data = await r.json();
    if (data.ok) {
      if (input) input.value = '';
      if (successEl) { successEl.textContent = 'Key saved successfully.'; successEl.style.display = 'block'; }
      loadSettingsView();
      // Also refresh the chat settings if it was the Claude key
      if (provider === 'claude') {
        window.sgHasApiKey = true;
        window.sgMaskedApiKey = data.masked || null;
        if (typeof settingsRefreshKeyUI === 'function') settingsRefreshKeyUI();
        if (typeof updateApiKeyOverlay === 'function') updateApiKeyOverlay();
      }
    } else {
      if (errEl) { errEl.textContent = data.error || 'Failed'; errEl.style.display = 'block'; }
    }
  } catch (e) {
    if (errEl) { errEl.textContent = 'Network error'; errEl.style.display = 'block'; }
  }
}

async function removeProviderKey(provider) {
  if (!confirm('Remove this API key?')) return;
  try {
    const r = await fetch(BASE + '/api/settings/provider-key/' + provider, { method: 'DELETE' });
    const data = await r.json();
    if (data.ok) {
      loadSettingsView();
      if (provider === 'claude') {
        window.sgHasApiKey = false;
        window.sgMaskedApiKey = null;
        if (typeof settingsRefreshKeyUI === 'function') settingsRefreshKeyUI();
        if (typeof updateApiKeyOverlay === 'function') updateApiKeyOverlay();
      }
    }
  } catch (e) {
    console.error('Failed to remove key:', e);
  }
}
</script>
</body>
</html>"""


# ── Main ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
