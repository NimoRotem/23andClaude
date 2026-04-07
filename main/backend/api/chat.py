"""Chat API — bridges a web chat interface to per-user Claude Code tmux sessions.

Each authenticated user gets their own tmux session, message history file, send-state,
and skills directory so that no data or context leaks between accounts.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.config import user_data_dir
from backend.models.schemas import User
from backend.utils.auth import get_current_user

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SESSION_PREFIX = os.environ.get("CHAT_TMUX_SESSION_PREFIX", "genomics-claude")
CLAUDE_CMD = os.environ.get("CLAUDE_CMD", "claude")
WORK_DIR_DEFAULT = os.environ.get("CHAT_WORK_DIR", str(Path(__file__).resolve().parents[2]))

# Legacy single-tenant defaults — used only as a one-shot migration source for
# the first admin who connects after the upgrade.
_LEGACY_SESSION_NAME = os.environ.get("CHAT_TMUX_SESSION", "genomics-claude")
_LEGACY_MESSAGES_FILE = Path(os.environ.get("CHAT_MESSAGES_FILE", "/data/app/chat_messages.json"))

router = APIRouter()

# ---------------------------------------------------------------------------
# Module-level state — keyed per user so requests never collide
# ---------------------------------------------------------------------------
# user_id -> last auto-approve timestamp
_auto_approve_sent: dict[str, float] = {}

# user_id -> last send-state ({"hash", "ts", "lines", "user_msg"})
_last_send_states: dict[str, dict] = {}


def _send_state(user_id: str) -> dict:
    state = _last_send_states.get(user_id)
    if state is None:
        state = {"hash": "", "ts": 0, "lines": 0, "user_msg": ""}
        _last_send_states[user_id] = state
    return state


# ---------------------------------------------------------------------------
# Per-user paths / identifiers
# ---------------------------------------------------------------------------

def _user_session_name(user: User) -> str:
    """Tmux session name unique to a user (max 12 chars of ID)."""
    return f"{SESSION_PREFIX}-{user.id[:12]}"


def _user_work_dir(user: User) -> str:
    """Working directory for the user's Claude Code tmux session."""
    base = user_data_dir(user.id) / "claude-workspace"
    base.mkdir(parents=True, exist_ok=True)
    return str(base)


def _user_messages_file(user: User) -> Path:
    return user_data_dir(user.id) / "chat_messages.json"


def _user_skills_dirs(user: User) -> list[Path]:
    """Allowed skill .md directories for this user."""
    workspace = Path(_user_work_dir(user))
    claude_skills = workspace / ".claude" / "skills"
    return [workspace, claude_skills]


def _maybe_migrate_legacy_admin(user: User):
    """One-shot migration: copy legacy global chat_messages.json into the
    admin user's per-user file the first time they connect after upgrade.

    Non-admins never inherit legacy data.
    """
    if user.role != "admin":
        return
    target = _user_messages_file(user)
    if target.exists():
        return
    if not _LEGACY_MESSAGES_FILE.exists():
        return
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(_LEGACY_MESSAGES_FILE, target)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------------------
_ANSI_RE = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub('', text)


# ---------------------------------------------------------------------------
# Tmux helpers
# ---------------------------------------------------------------------------

def capture_pane_full(session_name: str) -> str:
    """Capture the entire scrollback buffer of a tmux pane."""
    try:
        result = subprocess.run(
            ["tmux", "capture-pane", "-t", session_name, "-p", "-S", "-"],
            capture_output=True, text=True, timeout=10,
        )
        return result.stdout if result.returncode == 0 else ""
    except Exception:
        return ""


def capture_pane_recent(session_name: str, lines: int = 80) -> str:
    """Capture the most recent N lines of a tmux pane."""
    try:
        result = subprocess.run(
            ["tmux", "capture-pane", "-t", session_name, "-p", "-S", f"-{lines}"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout if result.returncode == 0 else ""
    except Exception:
        return ""


def _check_auto_approve(user_id: str, session_name: str, visible: str):
    """Detect Claude Code permission prompts and auto-select option 2 (bypass)."""
    last = _auto_approve_sent.get(user_id, 0.0)
    if time.time() - last < 10:
        return

    lines = visible.split("\n")
    option2_line = -1
    selected_line = -1
    for i, line in enumerate(lines):
        stripped = line.strip()
        if re.search(r'2\.\s+Yes.*bypass', stripped):
            option2_line = i
        if stripped.startswith('\u276f') or stripped.startswith('>'):
            selected_line = i

    if option2_line < 0 or selected_line < 0:
        return

    downs = option2_line - selected_line
    if downs < 0:
        return

    try:
        for _ in range(downs):
            subprocess.run(
                ["tmux", "send-keys", "-t", session_name, "Down"],
                capture_output=True, text=True, timeout=3,
            )
        subprocess.run(
            ["tmux", "send-keys", "-t", session_name, "Enter"],
            capture_output=True, text=True, timeout=3,
        )
        _auto_approve_sent[user_id] = time.time()
    except Exception:
        pass


def _session_exists(session_name: str) -> bool:
    """Check if a named tmux session exists."""
    try:
        result = subprocess.run(
            ["tmux", "has-session", "-t", session_name],
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def detect_activity(user_id: str, session_name: str) -> dict:
    """Detect whether the tmux session is busy, idle, or stopped."""
    info = {"status": "unknown", "command": "", "detail": ""}

    if not _session_exists(session_name):
        info["status"] = "stopped"
        info["detail"] = "Session not running"
        return info

    try:
        result = subprocess.run(
            ["tmux", "display-message", "-t", session_name, "-p",
             "#{pane_current_command}:#{pane_pid}"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode != 0:
            info["status"] = "stopped"
            info["detail"] = "Cannot read session"
            return info

        parts = result.stdout.strip().split(":")
        cmd = parts[0] if parts else ""
        info["command"] = cmd

        try:
            vis = subprocess.run(
                ["tmux", "capture-pane", "-t", session_name, "-p"],
                capture_output=True, text=True, timeout=5,
            )
            visible = vis.stdout if vis.returncode == 0 else ""
        except Exception:
            visible = ""

        _check_auto_approve(user_id, session_name, visible)

        all_lines = visible.split("\n")
        while all_lines and not all_lines[-1].strip():
            all_lines.pop()

        bottom = all_lines[-6:] if len(all_lines) >= 6 else all_lines
        bottom_text = "\n".join(bottom)

        has_esc_to_interrupt = "esc to interrupt" in bottom_text

        idle_prompt_patterns = [
            r'^[❯➜]\s*$',
            r'Tip:.*claude',
            r'[A-Z][a-zé]+ for \d+[ms]',
        ]
        has_idle_prompt = False
        for pattern in idle_prompt_patterns:
            for line in bottom:
                if re.search(pattern, line.strip()):
                    has_idle_prompt = True
                    break
            if has_idle_prompt:
                break

        window = all_lines[-25:] if len(all_lines) >= 25 else all_lines
        SPINNER_ICONS = r'[✶✽✻·\*☆◆●⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏✢✦✧✹✵✴✸❋❊❉✺◇◈⟡⊛⊕⊗▸▹►▻◉◎★♦♢⬡⬢]'
        COMPLETION_RE = re.compile(r'^●\s+(Done|Completed)\b')
        for line in window:
            stripped = line.strip()
            if COMPLETION_RE.match(stripped):
                continue
            if re.match(r'^[⎿\s]*◼', stripped):
                info["status"] = "busy"
                info["detail"] = "Running task"
                return info
            if re.match(SPINNER_ICONS + r'\s+\w+(?:…|\.{2,3})', stripped):
                info["status"] = "busy"
                if '(thinking)' in stripped or 'thought for' in stripped:
                    info["detail"] = "Thinking"
                else:
                    info["detail"] = "Working"
                return info
            if re.search(SPINNER_ICONS + r'\s+\w+(?:…|\.{2,3})(?:\s*\(.*?\))?\s*$', stripped):
                info["status"] = "busy"
                if '(thinking)' in stripped or 'thought for' in stripped:
                    info["detail"] = "Thinking"
                else:
                    info["detail"] = "Working"
                return info
            if re.search(r'\(thought for \d+', stripped) or stripped.endswith('(thinking)'):
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
        shell_cmds = {"bash", "zsh", "sh", "fish", "tmux"}
        if cmd.lower() in shell_cmds:
            if re.search(r'[\$#%>]\s*$', last_line) or not last_line:
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


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

def _ensure_session(user: User):
    """Create the user's Claude Code tmux session if it doesn't exist."""
    session_name = _user_session_name(user)
    work_dir = _user_work_dir(user)

    if _session_exists(session_name):
        activity = detect_activity(user.id, session_name)
        if activity["command"] in ("claude", "node"):
            return
        # Session exists but Claude isn't running — launch it
        subprocess.run(
            ["tmux", "send-keys", "-t", session_name, "-l",
             f"{CLAUDE_CMD} --dangerously-skip-permissions --name genomics-ai-{user.id[:8]}"],
            capture_output=True, text=True, timeout=5,
        )
        subprocess.run(
            ["tmux", "send-keys", "-t", session_name, "Enter"],
            capture_output=True, text=True, timeout=5,
        )
    else:
        try:
            extra_path = os.environ.get("CHAT_TMUX_EXTRA_PATH", "")
            env_setup = f"export PATH={extra_path}:$PATH" if extra_path else "export PATH=$PATH"
            subprocess.run(
                ["tmux", "new-session", "-d", "-s", session_name, "-c", work_dir],
                capture_output=True, text=True, timeout=10,
            )
            time.sleep(0.5)
            subprocess.run(
                ["tmux", "send-keys", "-t", session_name, "-l", env_setup],
                capture_output=True, text=True, timeout=5,
            )
            subprocess.run(
                ["tmux", "send-keys", "-t", session_name, "Enter"],
                capture_output=True, text=True, timeout=5,
            )
            time.sleep(0.5)
            subprocess.run(
                ["tmux", "send-keys", "-t", session_name, "-l",
                 f"{CLAUDE_CMD} --dangerously-skip-permissions --name genomics-ai-{user.id[:8]}"],
                capture_output=True, text=True, timeout=5,
            )
            subprocess.run(
                ["tmux", "send-keys", "-t", session_name, "Enter"],
                capture_output=True, text=True, timeout=5,
            )
        except Exception:
            pass

    # Wait for Claude Code to be ready (up to 30 seconds)
    for _ in range(30):
        time.sleep(1)
        activity = detect_activity(user.id, session_name)
        if activity["status"] == "idle" and activity["command"] in ("claude", "node"):
            return
        visible = capture_pane_recent(session_name, 10)
        if "bypass permissions" in visible.lower():
            return


# ---------------------------------------------------------------------------
# Message persistence (per user)
# ---------------------------------------------------------------------------

def _load_messages(user: User) -> list:
    """Load chat messages from the user's history file."""
    _maybe_migrate_legacy_admin(user)
    path = _user_messages_file(user)
    try:
        if path.exists():
            data = json.loads(path.read_text())
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def _save_messages(user: User, messages: list):
    """Persist chat messages to the user's history file."""
    path = _user_messages_file(user)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(messages, indent=2))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Similarity / dedup
# ---------------------------------------------------------------------------

def _msg_similarity(a: str, b: str) -> float:
    wa = set(a.lower().split())
    wb = set(b.lower().split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def _append_assistant_msg(user: User, messages: list, text: str, ts: float):
    """Append an assistant message, skipping if too similar to the last one."""
    for m in reversed(messages):
        if m["role"] == "assistant":
            if m["text"] == text or _msg_similarity(m["text"], text) > 0.7:
                return
            break
    messages.append({"role": "assistant", "text": text, "ts": ts})
    _save_messages(user, messages)


# ---------------------------------------------------------------------------
# Send text to tmux
# ---------------------------------------------------------------------------

def _send_to_tmux(session_name: str, text: str):
    """Send text to a tmux session. Uses load-buffer for long text."""
    if len(text) > 200:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
            tmp.write(text)
            tmp_path = tmp.name
        try:
            subprocess.run(
                ["tmux", "load-buffer", tmp_path],
                capture_output=True, text=True, timeout=5,
            )
            subprocess.run(
                ["tmux", "paste-buffer", "-t", session_name],
                capture_output=True, text=True, timeout=5,
            )
        finally:
            os.unlink(tmp_path)
    else:
        subprocess.run(
            ["tmux", "send-keys", "-t", session_name, "-l", text],
            capture_output=True, text=True, timeout=5,
        )
    subprocess.run(
        ["tmux", "send-keys", "-t", session_name, "Enter"],
        capture_output=True, text=True, timeout=5,
    )


# ---------------------------------------------------------------------------
# Response extraction
# ---------------------------------------------------------------------------

def _pane_hash(content: str) -> str:
    return hashlib.md5(content.encode()).hexdigest()


def _extract_response(user: User, session_name: str, old_lines: int) -> Optional[str]:
    """Extract Claude's response from new pane content since the user message."""
    full = capture_pane_full(session_name)
    if not full:
        return None

    clean = _strip_ansi(full)
    all_lines = clean.split("\n")

    if old_lines > 0 and old_lines < len(all_lines):
        new_lines = all_lines[old_lines:]
    else:
        new_lines = all_lines

    ui_patterns = [
        r'^[❯➜>]\s*$',
        r'^─+$',
        r'^\s*$',
        r'^Tip:',
        r'esc to interrupt',
        r'bypass permissions',
        r'^[A-Z][a-zé]+ for \d+[ms]',
        r'^\s*╭',
        r'^\s*╰',
        r'^\s*│\s*$',
        r'^⏵⏵\s',
        r'^\$ .*$',
    ]

    filtered = []
    for line in new_lines:
        stripped = line.strip()
        if not stripped:
            if filtered:
                filtered.append("")
            continue
        skip = False
        for pat in ui_patterns:
            if re.match(pat, stripped):
                skip = True
                break
        if not skip:
            filtered.append(stripped)

    while filtered and not filtered[-1]:
        filtered.pop()
    while filtered and not filtered[0]:
        filtered.pop(0)

    if not filtered:
        return None

    response = "\n".join(filtered)

    user_msg = _send_state(user.id).get("user_msg", "")
    if user_msg and response.strip() == user_msg.strip():
        return None

    if len(response.strip()) < 3:
        return None

    return response


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class SendRequest(BaseModel):
    message: str


class SkillSaveRequest(BaseModel):
    content: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/send")
async def send_message(
    body: SendRequest,
    current_user: User = Depends(get_current_user),
):
    """Send a user message to the user's Claude Code tmux session."""
    try:
        _ensure_session(current_user)
        session_name = _user_session_name(current_user)

        full = capture_pane_full(session_name)
        clean = _strip_ansi(full)
        line_count = len(clean.split("\n"))
        h = _pane_hash(clean)

        _last_send_states[current_user.id] = {
            "hash": h,
            "ts": time.time(),
            "lines": line_count,
            "user_msg": body.message,
        }

        _send_to_tmux(session_name, body.message)

        messages = _load_messages(current_user)
        messages.append({
            "role": "user",
            "text": body.message,
            "ts": time.time(),
        })
        _save_messages(current_user, messages)

        return {"ok": True, "session_status": "busy"}

    except Exception:
        return JSONResponse({"error": "Failed to send message"}, status_code=500)


@router.get("/status")
async def get_status(current_user: User = Depends(get_current_user)):
    """Poll the user's session status. Extracts assistant responses when idle."""
    try:
        session_name = _user_session_name(current_user)
        if not _session_exists(session_name):
            messages = _load_messages(current_user)
            return {
                "status": "stopped",
                "detail": "Session not running",
                "messages": messages,
                "session_exists": False,
            }

        activity = detect_activity(current_user.id, session_name)
        state = _send_state(current_user.id)

        if (activity["status"] == "idle"
                and state["ts"] > 0
                and time.time() - state["ts"] > 3):
            full = capture_pane_full(session_name)
            clean = _strip_ansi(full)
            current_hash = _pane_hash(clean)

            if current_hash != state["hash"]:
                response = _extract_response(current_user, session_name, state["lines"])
                if response:
                    messages = _load_messages(current_user)
                    _append_assistant_msg(current_user, messages, response, time.time())
                state["hash"] = current_hash
                state["ts"] = 0

        messages = _load_messages(current_user)
        return {
            "status": activity["status"],
            "detail": activity["detail"],
            "messages": messages,
            "session_exists": True,
        }

    except Exception:
        return JSONResponse({"error": "Failed to get status"}, status_code=500)


@router.post("/interrupt")
async def interrupt_session(current_user: User = Depends(get_current_user)):
    """Send Escape key to interrupt a running Claude Code session."""
    session_name = _user_session_name(current_user)
    if not _session_exists(session_name):
        return JSONResponse({"error": "Session not found"}, status_code=404)
    try:
        subprocess.run(
            ["tmux", "send-keys", "-t", session_name, "Escape"],
            capture_output=True, text=True, timeout=5,
        )
        return {"ok": True, "action": "interrupt"}
    except Exception:
        return JSONResponse({"error": "Interrupt failed"}, status_code=500)


@router.post("/restart")
async def restart_session(
    clear_history: bool = Query(default=False),
    current_user: User = Depends(get_current_user),
):
    """Kill and recreate the user's Claude Code tmux session."""
    try:
        session_name = _user_session_name(current_user)
        if _session_exists(session_name):
            subprocess.run(
                ["tmux", "kill-session", "-t", session_name],
                capture_output=True, text=True, timeout=10,
            )
            time.sleep(0.5)

        if clear_history:
            _save_messages(current_user, [])

        _ensure_session(current_user)
        return {"ok": True, "action": "restart", "history_cleared": clear_history}

    except Exception:
        return JSONResponse({"error": "Restart failed"}, status_code=500)


@router.get("/history")
async def get_history(current_user: User = Depends(get_current_user)):
    """Return all chat messages from the user's history file."""
    messages = _load_messages(current_user)
    return {"messages": messages}


@router.post("/clear")
async def clear_history(
    kill_session: bool = Query(default=False),
    current_user: User = Depends(get_current_user),
):
    """Clear the user's chat messages. Optionally kill their tmux session too."""
    try:
        _save_messages(current_user, [])
        session_name = _user_session_name(current_user)
        if kill_session and _session_exists(session_name):
            subprocess.run(
                ["tmux", "kill-session", "-t", session_name],
                capture_output=True, text=True, timeout=10,
            )
        return {"ok": True, "action": "clear", "session_killed": kill_session}

    except Exception:
        return JSONResponse({"error": "Clear failed"}, status_code=500)


# ---------------------------------------------------------------------------
# Raw terminal output endpoints
# ---------------------------------------------------------------------------

def _get_pane_position(session_name: str) -> int:
    """Get current total line count in the pane (cheap, no content capture)."""
    try:
        result = subprocess.run(
            ["tmux", "display-message", "-t", session_name, "-p",
             "#{history_size}:#{cursor_y}"],
            capture_output=True, text=True, timeout=3,
        )
        if result.returncode == 0:
            parts = result.stdout.strip().split(":")
            return int(parts[0]) + int(parts[1]) + 1
    except Exception:
        pass
    return 0


@router.get("/raw")
async def get_raw_output(current_user: User = Depends(get_current_user)):
    """Return the full raw terminal scrollback for the user's session."""
    session_name = _user_session_name(current_user)
    if not _session_exists(session_name):
        return JSONResponse({"error": "Session not running"}, status_code=404)
    raw = capture_pane_full(session_name)
    activity = detect_activity(current_user.id, session_name)
    return {
        "raw": raw,
        "lines": len(raw.split("\n")),
        "activity_status": activity["status"],
        "activity_detail": activity["detail"],
    }


@router.get("/raw-tail")
async def get_raw_tail(
    known_lines: int = 0,
    current_user: User = Depends(get_current_user),
):
    """Return delta output since the client's last known line count."""
    session_name = _user_session_name(current_user)
    if not _session_exists(session_name):
        return JSONResponse({"error": "Session not running"}, status_code=404)

    current_total = _get_pane_position(session_name)

    if known_lines <= 0 or known_lines > current_total:
        raw = capture_pane_full(session_name)
        return {
            "mode": "full",
            "raw": raw,
            "total_lines": len(raw.split("\n")),
            "pane_total": current_total,
        }

    if current_total <= known_lines:
        activity = detect_activity(current_user.id, session_name)
        return {
            "mode": "none",
            "raw": "",
            "total_lines": known_lines,
            "pane_total": current_total,
            "activity_status": activity.get("status", "unknown"),
        }

    delta_count = current_total - known_lines + 5
    recent = capture_pane_recent(session_name, delta_count)
    return {
        "mode": "delta",
        "raw": recent,
        "total_lines": current_total,
        "pane_total": current_total,
    }


# ---------------------------------------------------------------------------
# Skills / .md file management (per user)
# ---------------------------------------------------------------------------

def _list_skill_files(user: User) -> list[dict]:
    """List all .md skill files in the user's workspace."""
    files = []
    workspace = Path(_user_work_dir(user))
    for d in _user_skills_dirs(user):
        if not d.exists():
            continue
        for p in sorted(d.glob("*.md")):
            if p.name.lower() == "readme.md":
                continue
            try:
                stat = p.stat()
                rel_dir = ""
                try:
                    if d != workspace:
                        rel_dir = str(d.relative_to(workspace))
                except ValueError:
                    rel_dir = ""
                files.append({
                    "name": p.name,
                    "path": str(p),
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                    "dir": rel_dir,
                })
            except Exception:
                pass
    return files


def _resolve_skill_for_user(user: User, filename: str) -> Optional[Path]:
    """Resolve a skill filename to a path inside the user's workspace, or None."""
    for d in _user_skills_dirs(user):
        try:
            d_resolved = d.resolve()
        except Exception:
            continue
        candidate = (d / filename).resolve()
        try:
            candidate.relative_to(d_resolved)
        except ValueError:
            continue
        if candidate.is_file() and candidate.suffix == ".md":
            return candidate
    return None


@router.get("/skills")
async def list_skills(current_user: User = Depends(get_current_user)):
    """List all .md skill/instruction files in the user's workspace."""
    return {"files": _list_skill_files(current_user)}


@router.get("/skills/{filename:path}")
async def read_skill(filename: str, current_user: User = Depends(get_current_user)):
    """Read a skill .md file by name from the user's workspace."""
    candidate = _resolve_skill_for_user(current_user, filename)
    if candidate is None:
        return JSONResponse({"error": "File not found"}, status_code=404)
    return {
        "name": candidate.name,
        "path": str(candidate),
        "content": candidate.read_text(encoding="utf-8"),
        "size": candidate.stat().st_size,
        "modified": candidate.stat().st_mtime,
    }


@router.put("/skills/{filename:path}")
async def save_skill(
    filename: str,
    body: SkillSaveRequest,
    current_user: User = Depends(get_current_user),
):
    """Save/update a skill .md file in the user's workspace."""
    if "/" in filename:
        parts = filename.rsplit("/", 1)
        subdir = parts[0]
        fname = parts[1]
    else:
        subdir = ""
        fname = filename

    if not fname.endswith(".md"):
        fname += ".md"

    workspace = Path(_user_work_dir(current_user))
    target_dir = workspace / subdir if subdir else workspace
    try:
        target_dir_resolved = target_dir.resolve()
    except Exception:
        return JSONResponse({"error": "Invalid path"}, status_code=400)

    allowed = False
    for d in _user_skills_dirs(current_user):
        try:
            if str(target_dir_resolved) == str(d.resolve()):
                allowed = True
                break
        except Exception:
            continue
    if not allowed:
        return JSONResponse({"error": "Directory not allowed"}, status_code=403)

    target_dir.mkdir(parents=True, exist_ok=True)
    filepath = target_dir / fname
    try:
        filepath.write_text(body.content, encoding="utf-8")
        return {
            "ok": True,
            "name": filepath.name,
            "path": str(filepath),
            "size": filepath.stat().st_size,
        }
    except Exception:
        return JSONResponse({"error": "Save failed"}, status_code=500)


@router.post("/skills/new")
async def create_skill(
    body: SkillSaveRequest,
    current_user: User = Depends(get_current_user),
):
    """Create a new skill .md file in the user's .claude/skills/ subdirectory."""
    workspace = Path(_user_work_dir(current_user))
    skills_sub = workspace / ".claude" / "skills"
    skills_sub.mkdir(parents=True, exist_ok=True)

    first_line = body.content.strip().split("\n")[0].strip("# ").strip()
    if first_line:
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', first_line)[:50].strip('_') + ".md"
    else:
        safe_name = f"skill_{int(time.time())}.md"

    filepath = skills_sub / safe_name
    counter = 1
    while filepath.exists():
        stem = safe_name.rsplit('.', 1)[0]
        filepath = skills_sub / f"{stem}_{counter}.md"
        counter += 1

    filepath.write_text(body.content, encoding="utf-8")
    return {
        "ok": True,
        "name": filepath.name,
        "path": str(filepath),
        "size": filepath.stat().st_size,
    }


@router.delete("/skills/{filename:path}")
async def delete_skill(
    filename: str,
    current_user: User = Depends(get_current_user),
):
    """Delete a skill .md file (only from the user's .claude/skills/ subdir)."""
    workspace = Path(_user_work_dir(current_user))
    skills_sub = (workspace / ".claude" / "skills").resolve()
    candidate = (workspace / ".claude" / "skills" / filename).resolve()
    try:
        candidate.relative_to(skills_sub)
    except ValueError:
        return JSONResponse(
            {"error": "Can only delete files from skills/ directory"},
            status_code=403,
        )
    if not candidate.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    try:
        candidate.unlink()
        return {"ok": True, "deleted": filename}
    except Exception:
        return JSONResponse({"error": "Delete failed"}, status_code=500)
