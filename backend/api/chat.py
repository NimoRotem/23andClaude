"""Chat API — bridges a web chat interface to a Claude Code tmux session."""

import hashlib
import json
import shutil
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SESSION_NAME = "genomics-claude"
CLAUDE_CMD = os.getenv("CLAUDE_CMD", shutil.which("claude") or "claude")
WORK_DIR = os.getenv("GENOMICS_WORK_DIR", str(Path(__file__).parent.parent.parent))
MESSAGES_FILE = Path("/data/app/chat_messages.json")

router = APIRouter()

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_auto_approve_sent: float = 0.0

# Track pane state at the moment we send a user message so we can later
# diff and extract Claude's response.
_last_send_state: dict = {"hash": "", "ts": 0, "lines": 0, "user_msg": ""}

# ---------------------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------------------
_ANSI_RE = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub('', text)


# ---------------------------------------------------------------------------
# Tmux helpers (adapted from tmux-dashboard)
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


def _check_auto_approve(session_name: str, visible: str):
    """Detect Claude Code permission prompts and auto-select option 2 (bypass)."""
    global _auto_approve_sent
    if time.time() - _auto_approve_sent < 10:
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
        _auto_approve_sent = time.time()
    except Exception:
        pass


def detect_activity(session_name: str) -> dict:
    """Detect whether the tmux session is busy, idle, or stopped."""
    info = {"status": "unknown", "command": "", "detail": ""}

    if not _session_exists():
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

        # Capture visible pane
        try:
            vis = subprocess.run(
                ["tmux", "capture-pane", "-t", session_name, "-p"],
                capture_output=True, text=True, timeout=5,
            )
            visible = vis.stdout if vis.returncode == 0 else ""
        except Exception:
            visible = ""

        # Auto-approve permission prompts
        _check_auto_approve(session_name, visible)

        all_lines = visible.split("\n")
        while all_lines and not all_lines[-1].strip():
            all_lines.pop()

        bottom = all_lines[-6:] if len(all_lines) >= 6 else all_lines
        bottom_text = "\n".join(bottom)

        # --- "esc to interrupt" = strong busy signal ---
        has_esc_to_interrupt = "esc to interrupt" in bottom_text

        # --- Idle prompt indicators ---
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

        # --- Active spinners / progress (wider window) ---
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

        # Idle prompt + no busy signals → truly idle
        if has_idle_prompt and not has_esc_to_interrupt:
            info["status"] = "idle"
            info["detail"] = "Waiting for input"
            return info

        if has_esc_to_interrupt:
            info["status"] = "busy"
            info["detail"] = "Background tasks"
            return info

        # Shell prompt check
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

def _session_exists() -> bool:
    """Check if the genomics-claude tmux session exists."""
    try:
        result = subprocess.run(
            ["tmux", "has-session", "-t", SESSION_NAME],
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def _ensure_session():
    """Create the genomics-claude tmux session if it doesn't exist.

    Waits for Claude Code to fully initialize (detects the idle prompt)
    before returning, so subsequent send_message calls go to Claude, not
    the shell.
    """
    if _session_exists():
        # Check if Claude is actually running (not just a bare shell)
        activity = detect_activity(SESSION_NAME)
        if activity["command"] in ("claude", "node"):
            return
        # Session exists but Claude isn't running — launch it
        subprocess.run(
            ["tmux", "send-keys", "-t", SESSION_NAME, "-l",
             f"{CLAUDE_CMD} --dangerously-skip-permissions --name genomics-ai"],
            capture_output=True, text=True, timeout=5,
        )
        subprocess.run(
            ["tmux", "send-keys", "-t", SESSION_NAME, "Enter"],
            capture_output=True, text=True, timeout=5,
        )
    else:
        try:
            # Create session with proper PATH for node/claude
            env_setup = f"export PATH={Path.home() / '.local' / 'bin'}:{os.environ.get('PATH', '/usr/bin')}"
            subprocess.run(
                ["tmux", "new-session", "-d", "-s", SESSION_NAME, "-c", WORK_DIR],
                capture_output=True, text=True, timeout=10,
            )
            time.sleep(0.5)
            # Set up PATH first
            subprocess.run(
                ["tmux", "send-keys", "-t", SESSION_NAME, "-l", env_setup],
                capture_output=True, text=True, timeout=5,
            )
            subprocess.run(
                ["tmux", "send-keys", "-t", SESSION_NAME, "Enter"],
                capture_output=True, text=True, timeout=5,
            )
            time.sleep(0.5)
            subprocess.run(
                ["tmux", "send-keys", "-t", SESSION_NAME, "-l",
                 f"{CLAUDE_CMD} --dangerously-skip-permissions --name genomics-ai"],
                capture_output=True, text=True, timeout=5,
            )
            subprocess.run(
                ["tmux", "send-keys", "-t", SESSION_NAME, "Enter"],
                capture_output=True, text=True, timeout=5,
            )
        except Exception:
            pass

    # Wait for Claude Code to be ready (up to 30 seconds)
    for _ in range(30):
        time.sleep(1)
        activity = detect_activity(SESSION_NAME)
        if activity["status"] == "idle" and activity["command"] in ("claude", "node"):
            return
        # Also check for the bypass permissions banner
        visible = capture_pane_recent(SESSION_NAME, 10)
        if "bypass permissions" in visible.lower():
            return


# ---------------------------------------------------------------------------
# Message persistence
# ---------------------------------------------------------------------------

def _load_messages() -> list:
    """Load chat messages from disk."""
    try:
        if MESSAGES_FILE.exists():
            data = json.loads(MESSAGES_FILE.read_text())
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def _save_messages(messages: list):
    """Persist chat messages to disk."""
    try:
        MESSAGES_FILE.parent.mkdir(parents=True, exist_ok=True)
        MESSAGES_FILE.write_text(json.dumps(messages, indent=2))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Similarity / dedup
# ---------------------------------------------------------------------------

def _msg_similarity(a: str, b: str) -> float:
    """Quick word-overlap similarity between two strings."""
    wa = set(a.lower().split())
    wb = set(b.lower().split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def _append_assistant_msg(messages: list, text: str, ts: float):
    """Append an assistant message, skipping if too similar to the last one."""
    for m in reversed(messages):
        if m["role"] == "assistant":
            if m["text"] == text or _msg_similarity(m["text"], text) > 0.7:
                return
            break
    messages.append({"role": "assistant", "text": text, "ts": ts})
    _save_messages(messages)


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
    # Press Enter
    subprocess.run(
        ["tmux", "send-keys", "-t", session_name, "Enter"],
        capture_output=True, text=True, timeout=5,
    )


# ---------------------------------------------------------------------------
# Response extraction
# ---------------------------------------------------------------------------

def _pane_hash(content: str) -> str:
    return hashlib.md5(content.encode()).hexdigest()


def _extract_response(old_lines: int) -> Optional[str]:
    """Extract Claude's response from new pane content since the user message."""
    full = capture_pane_full(SESSION_NAME)
    if not full:
        return None

    clean = _strip_ansi(full)
    all_lines = clean.split("\n")

    # Get lines that appeared after the user message
    if old_lines > 0 and old_lines < len(all_lines):
        new_lines = all_lines[old_lines:]
    else:
        # Fallback: try to find the response in the last portion
        new_lines = all_lines

    # Filter out Claude Code UI chrome and empty lines
    ui_patterns = [
        r'^[❯➜>]\s*$',                        # prompt characters
        r'^─+$',                                # horizontal rules
        r'^\s*$',                                # blank lines
        r'^Tip:',                                # tip lines
        r'esc to interrupt',                    # status bar
        r'bypass permissions',                  # permissions banner
        r'^[A-Z][a-zé]+ for \d+[ms]',          # completion time
        r'^\s*╭',                               # box drawing top
        r'^\s*╰',                               # box drawing bottom
        r'^\s*│\s*$',                           # empty box sides
        r'^⏵⏵\s',                               # bypass mode indicator
        r'^\$ .*$',                              # shell prompt echo
    ]

    filtered = []
    for line in new_lines:
        stripped = line.strip()
        if not stripped:
            # Keep blank lines within content, but skip leading/trailing
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

    # Trim trailing empty lines
    while filtered and not filtered[-1]:
        filtered.pop()

    # Trim leading empty lines
    while filtered and not filtered[0]:
        filtered.pop(0)

    if not filtered:
        return None

    response = "\n".join(filtered)

    # Skip if the response is just the user's own message echoed back
    user_msg = _last_send_state.get("user_msg", "")
    if user_msg and response.strip() == user_msg.strip():
        return None

    # Skip very short responses that are likely just UI artifacts
    if len(response.strip()) < 3:
        return None

    return response


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class SendRequest(BaseModel):
    message: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/send")
async def send_message(body: SendRequest):
    """Send a user message to the Claude Code tmux session."""
    global _last_send_state
    try:
        _ensure_session()

        # Capture current pane state for later response extraction
        full = capture_pane_full(SESSION_NAME)
        clean = _strip_ansi(full)
        line_count = len(clean.split("\n"))
        h = _pane_hash(clean)

        _last_send_state = {
            "hash": h,
            "ts": time.time(),
            "lines": line_count,
            "user_msg": body.message,
        }

        # Send message to tmux
        _send_to_tmux(SESSION_NAME, body.message)

        # Save user message to history
        messages = _load_messages()
        messages.append({
            "role": "user",
            "text": body.message,
            "ts": time.time(),
        })
        _save_messages(messages)

        return {"ok": True, "session_status": "busy"}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/status")
async def get_status():
    """Poll the session status. Extracts assistant responses when idle."""
    global _last_send_state
    try:
        session_exists = _session_exists()
        if not session_exists:
            messages = _load_messages()
            return {
                "status": "stopped",
                "detail": "Session not running",
                "messages": messages,
                "session_exists": False,
            }

        activity = detect_activity(SESSION_NAME)

        # If idle and enough time has passed since last send, try to
        # extract Claude's response from the pane output.
        if (activity["status"] == "idle"
                and _last_send_state["ts"] > 0
                and time.time() - _last_send_state["ts"] > 3):
            full = capture_pane_full(SESSION_NAME)
            clean = _strip_ansi(full)
            current_hash = _pane_hash(clean)

            if current_hash != _last_send_state["hash"]:
                response = _extract_response(_last_send_state["lines"])
                if response:
                    messages = _load_messages()
                    _append_assistant_msg(messages, response, time.time())
                # Mark as processed so we don't re-extract
                _last_send_state["hash"] = current_hash
                _last_send_state["ts"] = 0

        messages = _load_messages()
        return {
            "status": activity["status"],
            "detail": activity["detail"],
            "messages": messages,
            "session_exists": True,
        }

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/interrupt")
async def interrupt_session():
    """Send Escape key to interrupt a running Claude Code session."""
    if not _session_exists():
        return JSONResponse({"error": "Session not found"}, status_code=404)
    try:
        subprocess.run(
            ["tmux", "send-keys", "-t", SESSION_NAME, "Escape"],
            capture_output=True, text=True, timeout=5,
        )
        return {"ok": True, "action": "interrupt"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/restart")
async def restart_session(clear_history: bool = Query(default=False)):
    """Kill and recreate the Claude Code tmux session."""
    try:
        # Kill existing session
        if _session_exists():
            subprocess.run(
                ["tmux", "kill-session", "-t", SESSION_NAME],
                capture_output=True, text=True, timeout=10,
            )
            time.sleep(0.5)

        if clear_history:
            _save_messages([])

        # Recreate
        _ensure_session()
        return {"ok": True, "action": "restart", "history_cleared": clear_history}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/history")
async def get_history():
    """Return all chat messages from the history file."""
    messages = _load_messages()
    return {"messages": messages}


@router.post("/clear")
async def clear_history(kill_session: bool = Query(default=False)):
    """Clear all chat messages. Optionally kill the tmux session too."""
    try:
        _save_messages([])

        if kill_session and _session_exists():
            subprocess.run(
                ["tmux", "kill-session", "-t", SESSION_NAME],
                capture_output=True, text=True, timeout=10,
            )

        return {"ok": True, "action": "clear", "session_killed": kill_session}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# Raw terminal output endpoints
# ---------------------------------------------------------------------------

def _get_pane_position() -> int:
    """Get current total line count in the pane (cheap, no content capture)."""
    try:
        result = subprocess.run(
            ["tmux", "display-message", "-t", SESSION_NAME, "-p",
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
async def get_raw_output():
    """Return the full raw terminal scrollback for the session."""
    if not _session_exists():
        return JSONResponse({"error": "Session not running"}, status_code=404)
    raw = capture_pane_full(SESSION_NAME)
    activity = detect_activity(SESSION_NAME)
    return {
        "raw": raw,
        "lines": len(raw.split("\n")),
        "activity_status": activity["status"],
        "activity_detail": activity["detail"],
    }


@router.get("/raw-tail")
async def get_raw_tail(known_lines: int = 0):
    """Return delta output since the client's last known line count."""
    if not _session_exists():
        return JSONResponse({"error": "Session not running"}, status_code=404)

    current_total = _get_pane_position()

    # First load or reset → full capture
    if known_lines <= 0 or known_lines > current_total:
        raw = capture_pane_full(SESSION_NAME)
        return {
            "mode": "full",
            "raw": raw,
            "total_lines": len(raw.split("\n")),
            "pane_total": current_total,
        }

    # No new content
    if current_total <= known_lines:
        activity = detect_activity(SESSION_NAME)
        return {
            "mode": "none",
            "raw": "",
            "total_lines": known_lines,
            "pane_total": current_total,
            "activity_status": activity.get("status", "unknown"),
        }

    # Delta — capture recent lines
    delta_count = current_total - known_lines + 5  # small overlap for safety
    recent = capture_pane_recent(SESSION_NAME, delta_count)
    return {
        "mode": "delta",
        "raw": recent,
        "total_lines": current_total,
        "pane_total": current_total,
    }


# ---------------------------------------------------------------------------
# Skills / .md file management
# ---------------------------------------------------------------------------

SKILLS_DIR = Path(WORK_DIR)
CLAUDE_SKILLS_DIR = SKILLS_DIR / ".claude" / "skills"
# Expose root .md files + .claude/skills/ directory
SKILLS_ALLOWED_DIRS = [
    SKILLS_DIR,                      # ~/genomics-app/*.md (root CLAUDE.md etc.)
    CLAUDE_SKILLS_DIR,               # ~/genomics-app/.claude/skills/*.md
]


def _list_skill_files() -> list[dict]:
    """List all .md skill files the Claude Code session can see."""
    files = []
    for d in SKILLS_ALLOWED_DIRS:
        if not d.exists():
            continue
        for p in sorted(d.glob("*.md")):
            if p.name.lower() == "readme.md":
                continue
            try:
                stat = p.stat()
                files.append({
                    "name": p.name,
                    "path": str(p),
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                    "dir": str(d.relative_to(SKILLS_DIR)) if d != SKILLS_DIR else "",
                })
            except Exception:
                pass
    return files


class SkillSaveRequest(BaseModel):
    content: str


@router.get("/skills")
async def list_skills():
    """List all .md skill/instruction files."""
    return {"files": _list_skill_files()}


@router.get("/skills/{filename:path}")
async def read_skill(filename: str):
    """Read a skill .md file by name."""
    # Security: only allow files in the allowed directories
    for d in SKILLS_ALLOWED_DIRS:
        candidate = d / filename
        try:
            candidate = candidate.resolve()
            if candidate.is_file() and candidate.suffix == ".md" and str(candidate).startswith(str(d.resolve())):
                return {
                    "name": candidate.name,
                    "path": str(candidate),
                    "content": candidate.read_text(encoding="utf-8"),
                    "size": candidate.stat().st_size,
                    "modified": candidate.stat().st_mtime,
                }
        except Exception:
            continue
    return JSONResponse({"error": "File not found"}, status_code=404)


@router.put("/skills/{filename:path}")
async def save_skill(filename: str, body: SkillSaveRequest):
    """Save/update a skill .md file."""
    # Determine target directory
    if "/" in filename:
        parts = filename.rsplit("/", 1)
        subdir = parts[0]
        fname = parts[1]
    else:
        subdir = ""
        fname = filename

    if not fname.endswith(".md"):
        fname += ".md"

    target_dir = SKILLS_DIR / subdir if subdir else SKILLS_DIR
    # Validate target_dir is in allowed dirs
    target_dir_resolved = target_dir.resolve()
    allowed = False
    for d in SKILLS_ALLOWED_DIRS:
        if str(target_dir_resolved) == str(d.resolve()):
            allowed = True
            break
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
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/skills/new")
async def create_skill(body: SkillSaveRequest):
    """Create a new skill .md file in the skills/ subdirectory."""
    skills_sub = CLAUDE_SKILLS_DIR
    skills_sub.mkdir(parents=True, exist_ok=True)

    # Generate a filename from first line or default
    first_line = body.content.strip().split("\n")[0].strip("# ").strip()
    if first_line:
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', first_line)[:50].strip('_') + ".md"
    else:
        safe_name = f"skill_{int(time.time())}.md"

    filepath = skills_sub / safe_name
    # Avoid overwriting
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
async def delete_skill(filename: str):
    """Delete a skill .md file (only from .claude/skills/ subdirectory, not root)."""
    # Only allow deleting from .claude/skills/, not root CLAUDE.md files
    candidate = (CLAUDE_SKILLS_DIR / filename).resolve()
    skills_resolved = CLAUDE_SKILLS_DIR.resolve()
    if not str(candidate).startswith(str(skills_resolved)):
        return JSONResponse({"error": "Can only delete files from skills/ directory"}, status_code=403)
    if not candidate.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    try:
        candidate.unlink()
        return {"ok": True, "deleted": filename}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
