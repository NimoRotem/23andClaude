"""Chat backend for the 23 & Claude AI assistant tab.

Each user gets their own tmux session running `claude --dangerously-skip-permissions`
and their own per-user chat_messages.json. The session name is derived from
a hash of the username so two users never collide.

The flow:
  1. /api/chat/send         POST { message }   → write to user's tmux
  2. /api/chat/status       GET                 → poll status + new msgs
  3. /api/chat/interrupt    POST                → send Esc to user's session
  4. /api/chat/restart      POST ?clear=...     → kill+respawn user's session
  5. /api/chat/clear        POST ?kill=...      → reset user's history
  6. /api/chat/raw          GET                 → full pane scrollback
  7. /api/chat/raw_tail     GET ?from_lines=N   → incremental tail

Every endpoint depends on `current_user`, so unauthenticated callers get a
401 from FastAPI before any tmux work happens.
"""

import hashlib
import json
import logging
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from threading import Lock
from typing import Optional

from fastapi import APIRouter, Query, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logger = logging.getLogger("simple-genomics.chat")

# ─── Constants ───────────────────────────────────────────────────────
CLAUDE_CMD = os.environ.get("CLAUDE_CMD", "/usr/bin/claude")
WORK_DIR = os.environ.get("CHAT_WORK_DIR", "/home/nimrod_rotem/simple-genomics")
EXTRA_PATH = os.environ.get(
    "CHAT_TMUX_EXTRA_PATH",
    "/home/nimo/miniconda3/envs/genomics/bin",
)

router = APIRouter()


# ─── Per-user routing ───────────────────────────────────────────────
def _user_hash(username: str) -> str:
    return hashlib.sha1((username or "").lower().encode("utf-8")).hexdigest()[:16]


def _session_name(username: str) -> str:
    return f"sg-ai-{_user_hash(username)}"


def _messages_file(username: str) -> Path:
    """Per-user chat history. Imported from app.py at call time to avoid a
    circular import at module load."""
    from app import user_dir
    return user_dir(username) / "chat_messages.json"


def _current_user_dep(request: Request) -> str:
    """Per-route auth dependency. We re-import at call time so chat.py can
    be imported before app.py finishes wiring up the auth helpers."""
    from app import current_user
    return current_user(request)


# ─── Per-user state ─────────────────────────────────────────────────
# Each user gets their own _send_state slot under one global lock.
_state_lock = Lock()
_user_send_state: dict = {}      # username_lc -> {hash, ts, lines, user_msg}
_user_auto_approve_ts: dict = {}  # username_lc -> float


def _get_send_state(username):
    user_lc = (username or "").lower()
    with _state_lock:
        if user_lc not in _user_send_state:
            _user_send_state[user_lc] = {"hash": "", "ts": 0.0, "lines": 0, "user_msg": ""}
        return _user_send_state[user_lc]


def _set_send_state(username, **kwargs):
    user_lc = (username or "").lower()
    with _state_lock:
        st = _user_send_state.setdefault(
            user_lc, {"hash": "", "ts": 0.0, "lines": 0, "user_msg": ""}
        )
        st.update(kwargs)


# ─── ANSI helpers ───────────────────────────────────────────────────
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


# ─── Tmux helpers ───────────────────────────────────────────────────
def _run_tmux(args: list, timeout: int = 5) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["tmux"] + args, capture_output=True, text=True, timeout=timeout
    )


def _session_exists(session: str) -> bool:
    try:
        return _run_tmux(["has-session", "-t", session]).returncode == 0
    except Exception:
        return False


def _capture_pane_full(session: str) -> str:
    try:
        r = _run_tmux(["capture-pane", "-t", session, "-p", "-S", "-"], timeout=10)
        return r.stdout if r.returncode == 0 else ""
    except Exception:
        return ""


def _capture_pane_recent(session: str, lines: int = 80) -> str:
    try:
        r = _run_tmux(["capture-pane", "-t", session, "-p", "-S", f"-{lines}"])
        return r.stdout if r.returncode == 0 else ""
    except Exception:
        return ""


def _check_auto_approve(username, session, visible: str):
    """Detect Claude Code permission prompts and auto-pick option 2."""
    user_lc = (username or "").lower()
    last_ts = _user_auto_approve_ts.get(user_lc, 0.0)
    if time.time() - last_ts < 10:
        return

    lines = visible.split("\n")
    option2_line = -1
    selected_line = -1
    for i, line in enumerate(lines):
        s = line.strip()
        if re.search(r"2\.\s+Yes.*bypass", s):
            option2_line = i
        if s.startswith("\u276f") or s.startswith(">"):
            selected_line = i

    if option2_line < 0 or selected_line < 0:
        return
    downs = option2_line - selected_line
    if downs < 0:
        return

    try:
        for _ in range(downs):
            _run_tmux(["send-keys", "-t", session, "Down"], timeout=3)
        _run_tmux(["send-keys", "-t", session, "Enter"], timeout=3)
        _user_auto_approve_ts[user_lc] = time.time()
    except Exception:
        pass


def _detect_activity(username, session) -> dict:
    """Return whether the tmux session is busy / idle / stopped."""
    info = {"status": "unknown", "command": "", "detail": ""}

    if not _session_exists(session):
        info["status"] = "stopped"
        info["detail"] = "Session not running"
        return info

    try:
        r = _run_tmux(
            ["display-message", "-t", session, "-p",
             "#{pane_current_command}:#{pane_pid}"]
        )
        if r.returncode != 0:
            info["status"] = "stopped"
            info["detail"] = "Cannot read session"
            return info
        cmd = r.stdout.strip().split(":")[0]
        info["command"] = cmd

        try:
            vis = _run_tmux(["capture-pane", "-t", session, "-p"])
            visible = vis.stdout if vis.returncode == 0 else ""
        except Exception:
            visible = ""

        _check_auto_approve(username, session, visible)

        all_lines = visible.split("\n")
        while all_lines and not all_lines[-1].strip():
            all_lines.pop()
        bottom = all_lines[-6:] if len(all_lines) >= 6 else all_lines
        bottom_text = "\n".join(bottom)
        has_esc_to_interrupt = "esc to interrupt" in bottom_text

        idle_prompt_patterns = [
            r"^[❯➜]\s*$",
            r"Tip:.*claude",
            r"[A-Z][a-zé]+ for \d+[ms]",
        ]
        has_idle_prompt = any(
            re.search(p, line.strip())
            for line in bottom for p in idle_prompt_patterns
        )

        window = all_lines[-25:] if len(all_lines) >= 25 else all_lines
        SPINNER_ICONS = (
            r"[✶✽✻·\*☆◆●⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏✢✦✧✹✵✴✸❋❊❉✺◇◈"
            r"⟡⊛⊕⊗▸▹►▻◉◎★♦♢⬡⬢]"
        )
        COMPLETION_RE = re.compile(r"^●\s+(Done|Completed)\b")
        for line in window:
            stripped = line.strip()
            if COMPLETION_RE.match(stripped):
                continue
            if re.match(r"^[⎿\s]*◼", stripped):
                info["status"] = "busy"
                info["detail"] = "Running task"
                return info
            if re.match(SPINNER_ICONS + r"\s+\w+(?:…|\.{2,3})", stripped) \
               or re.search(SPINNER_ICONS + r"\s+\w+(?:…|\.{2,3})(?:\s*\(.*?\))?\s*$", stripped):
                info["status"] = "busy"
                info["detail"] = (
                    "Thinking" if "(thinking)" in stripped or "thought for" in stripped
                    else "Working"
                )
                return info
            if re.search(r"\(thought for \d+", stripped) or stripped.endswith("(thinking)"):
                info["status"] = "busy"
                info["detail"] = "Thinking"
                return info

        if has_idle_prompt and not has_esc_to_interrupt:
            info["status"] = "idle"
            info["detail"] = "Waiting for input"
            return info
        if has_esc_to_interrupt:
            info["status"] = "busy"
            info["detail"] = "Background tasks"
            return info

        last_line = bottom[-1].strip() if bottom else ""
        if cmd.lower() in {"bash", "zsh", "sh", "fish", "tmux"}:
            if re.search(r"[\$#%>]\s*$", last_line) or not last_line:
                info["status"] = "idle"
                info["detail"] = "Shell prompt"
            else:
                info["status"] = "busy"
                info["detail"] = cmd
        elif cmd.lower() in ("claude", "node"):
            info["status"] = "idle"
            info["detail"] = "Waiting for input"
        else:
            info["status"] = "busy"
            info["detail"] = cmd
    except Exception:
        pass
    return info


# ─── Session lifecycle ──────────────────────────────────────────────
def _ensure_session(username, session):
    """Create + launch claude in the tmux session if it isn't running."""
    if _session_exists(session):
        activity = _detect_activity(username, session)
        if activity["command"] in ("claude", "node"):
            return
        _run_tmux([
            "send-keys", "-t", session, "-l",
            f"{CLAUDE_CMD} --dangerously-skip-permissions --name {session}",
        ])
        _run_tmux(["send-keys", "-t", session, "Enter"])
    else:
        try:
            os.makedirs(WORK_DIR, exist_ok=True)
            _run_tmux(["new-session", "-d", "-s", session, "-c", WORK_DIR], timeout=10)
            time.sleep(0.5)
            env_setup = f"export PATH={EXTRA_PATH}:$PATH" if EXTRA_PATH else "export PATH=$PATH"
            _run_tmux(["send-keys", "-t", session, "-l", env_setup])
            _run_tmux(["send-keys", "-t", session, "Enter"])
            time.sleep(0.5)
            _run_tmux([
                "send-keys", "-t", session, "-l",
                f"{CLAUDE_CMD} --dangerously-skip-permissions --name {session}",
            ])
            _run_tmux(["send-keys", "-t", session, "Enter"])
        except Exception as e:
            logger.warning(f"Failed to start tmux session {session}: {e}")
            return

    for _ in range(30):
        time.sleep(1)
        activity = _detect_activity(username, session)
        if activity["status"] == "idle" and activity["command"] in ("claude", "node"):
            break
        if "bypass permissions" in _capture_pane_recent(session, 10).lower():
            break

    last = _capture_pane_full(session)
    for _ in range(20):
        time.sleep(0.3)
        cur = _capture_pane_full(session)
        if cur == last and cur:
            return
        last = cur


# ─── Message persistence (per-user) ───────────────────────────────
def _load_messages(username) -> list:
    try:
        path = _messages_file(username)
        if path.exists():
            data = json.loads(path.read_text())
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def _save_messages(username, messages: list):
    try:
        path = _messages_file(username)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(messages, indent=2))
    except Exception:
        pass


def _msg_similarity(a: str, b: str) -> float:
    wa = set(a.lower().split())
    wb = set(b.lower().split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def _append_assistant_msg(username, messages: list, text: str, ts: float):
    for m in reversed(messages):
        if m["role"] == "assistant":
            if m["text"] == text or _msg_similarity(m["text"], text) > 0.7:
                return
            break
    messages.append({"role": "assistant", "text": text, "ts": ts})
    _save_messages(username, messages)


# ─── Send / extract ────────────────────────────────────────────────
def _send_to_tmux(session, text: str):
    if len(text) > 200:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
            tmp.write(text)
            tmp_path = tmp.name
        try:
            _run_tmux(["load-buffer", tmp_path])
            _run_tmux(["paste-buffer", "-t", session])
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    else:
        _run_tmux(["send-keys", "-t", session, "-l", text])
    _run_tmux(["send-keys", "-t", session, "Enter"])


def _pane_hash(content: str) -> str:
    return hashlib.md5(content.encode()).hexdigest()


def _extract_response(username, session, old_lines: int) -> Optional[str]:
    full = _capture_pane_full(session)
    if not full:
        return None
    clean = _strip_ansi(full)
    all_lines = clean.split("\n")

    state = _get_send_state(username)
    user_msg = state.get("user_msg", "")
    new_lines = None
    if user_msg:
        for i in range(len(all_lines) - 1, -1, -1):
            ln = all_lines[i].strip()
            if user_msg in ln and ln.endswith(user_msg):
                new_lines = all_lines[i + 1:]
                break
    if new_lines is None:
        return None

    filtered = []
    for line in new_lines:
        s = line.strip()
        if not s:
            continue
        if s.startswith("?") or s.startswith("⏵") or s.startswith("⏷"):
            continue
        if re.match(r"^[\d\s]+$", s):
            continue
        if "esc to interrupt" in s.lower():
            continue
        if re.match(r"^\s*[╭╰│─╮╯]", line):
            continue
        if re.match(r"^[⎿\s]*[\$>%#]", s):
            continue
        if re.search(r"^Bypassing Permissions", s):
            continue
        filtered.append(line.rstrip())
    if not filtered:
        return None

    response = "\n".join(filtered)
    if user_msg and response.strip() == user_msg.strip():
        return None
    if len(response.strip()) < 3:
        return None
    return response


# ─── Request models ────────────────────────────────────────────────
class SendRequest(BaseModel):
    message: str


# ─── Endpoints ─────────────────────────────────────────────────────
@router.post("/send")
async def send_message(body: SendRequest, username: str = Depends(_current_user_dep)):
    try:
        session = _session_name(username)
        _ensure_session(username, session)
        full = _capture_pane_full(session)
        clean = _strip_ansi(full)
        line_count = len(clean.split("\n"))
        h = _pane_hash(clean)
        _set_send_state(username,
                        hash=h, ts=time.time(),
                        lines=line_count, user_msg=body.message)
        _send_to_tmux(session, body.message)

        messages = _load_messages(username)
        messages.append({
            "role": "user",
            "text": body.message,
            "ts": time.time(),
        })
        _save_messages(username, messages)
        return {"ok": True, "session_status": "busy"}
    except Exception as e:
        logger.exception("send failed")
        return JSONResponse({"error": f"Send failed: {e}"}, status_code=500)


@router.get("/status")
async def get_status(username: str = Depends(_current_user_dep)):
    try:
        session = _session_name(username)
        if not _session_exists(session):
            return {
                "status": "stopped",
                "detail": "Session not running",
                "messages": _load_messages(username),
                "session_exists": False,
            }
        activity = _detect_activity(username, session)
        state = _get_send_state(username)
        state_ts = state["ts"]
        state_lines = state["lines"]

        if activity["status"] == "idle" and state_ts > 0 and time.time() - state_ts > 3:
            extracted = False
            response = _extract_response(username, session, state_lines)
            if response:
                messages = _load_messages(username)
                _append_assistant_msg(username, messages, response, time.time())
                extracted = True
            if extracted or time.time() - state_ts > 60:
                full = _capture_pane_full(session)
                _set_send_state(username,
                                hash=_pane_hash(_strip_ansi(full)),
                                ts=0)

        return {
            "status": activity["status"],
            "detail": activity["detail"],
            "messages": _load_messages(username),
            "session_exists": True,
        }
    except Exception as e:
        logger.exception("status failed")
        return JSONResponse({"error": f"Status failed: {e}"}, status_code=500)


@router.post("/interrupt")
async def interrupt_session(username: str = Depends(_current_user_dep)):
    session = _session_name(username)
    if not _session_exists(session):
        return JSONResponse({"error": "Session not found"}, status_code=404)
    try:
        _run_tmux(["send-keys", "-t", session, "Escape"])
        return {"ok": True, "action": "interrupt"}
    except Exception as e:
        return JSONResponse({"error": f"Interrupt failed: {e}"}, status_code=500)


@router.post("/restart")
async def restart_session(
    clear_history: bool = Query(default=False),
    username: str = Depends(_current_user_dep),
):
    try:
        session = _session_name(username)
        if _session_exists(session):
            _run_tmux(["kill-session", "-t", session], timeout=10)
            time.sleep(0.5)
        if clear_history:
            _save_messages(username, [])
        _ensure_session(username, session)
        return {"ok": True, "action": "restart", "history_cleared": clear_history}
    except Exception as e:
        return JSONResponse({"error": f"Restart failed: {e}"}, status_code=500)


@router.get("/history")
async def get_history(username: str = Depends(_current_user_dep)):
    return {"messages": _load_messages(username)}


@router.post("/clear")
async def clear_history(
    kill_session: bool = Query(default=False),
    username: str = Depends(_current_user_dep),
):
    try:
        session = _session_name(username)
        _save_messages(username, [])
        if kill_session and _session_exists(session):
            _run_tmux(["kill-session", "-t", session], timeout=10)
        return {"ok": True, "action": "clear", "session_killed": kill_session}
    except Exception as e:
        return JSONResponse({"error": f"Clear failed: {e}"}, status_code=500)


@router.get("/raw")
async def get_raw(username: str = Depends(_current_user_dep)):
    session = _session_name(username)
    if not _session_exists(session):
        return {"raw": "", "lines": 0, "session_exists": False}
    full = _capture_pane_full(session)
    clean = _strip_ansi(full)
    return {"raw": clean, "lines": len(clean.split("\n")), "session_exists": True}


@router.get("/raw_tail")
async def get_raw_tail(
    from_lines: int = Query(default=0),
    username: str = Depends(_current_user_dep),
):
    session = _session_name(username)
    if not _session_exists(session):
        return {"mode": "full", "raw": "", "total_lines": 0, "session_exists": False}
    full = _capture_pane_full(session)
    clean = _strip_ansi(full)
    all_lines = clean.split("\n")
    total = len(all_lines)
    if from_lines <= 0 or from_lines > total:
        return {"mode": "full", "raw": clean, "total_lines": total, "session_exists": True}
    delta = "\n".join(all_lines[from_lines:])
    if delta:
        delta = "\n" + delta if delta else delta
    return {"mode": "delta", "raw": delta, "total_lines": total, "session_exists": True}
