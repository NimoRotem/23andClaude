"""System monitoring endpoint — lightweight htop-like stats via shell commands."""

import asyncio
import os
import re
import subprocess
import time
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse

from backend.config import (
    GPU_AVAILABLE, GPU_NAME, GPU_MEMORY_MB, CPU_COUNT, RAM_GB,
    DV_SHARDS, ALIGN_THREADS, get_setup_status,
)

router = APIRouter()


def _run(cmd: str, timeout: int = 5) -> str:
    """Run a shell command, return stdout."""
    try:
        r = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return r.stdout.strip()
    except Exception:
        return ""


@router.get("/stats")
async def system_stats():
    # Hostname
    hostname = _run("hostname")

    # Uptime
    uptime_raw = _run("uptime -p")  # "up 5 days, 3 hours, 42 minutes"

    # Load average
    load_raw = _run("cat /proc/loadavg")  # "1.23 0.45 0.67 2/456 12345"
    parts = load_raw.split()
    load_avg = (
        [float(parts[0]), float(parts[1]), float(parts[2])]
        if len(parts) >= 3
        else [0, 0, 0]
    )

    # CPU info
    cpu_model = _run(
        "grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2"
    ).strip()
    threads = int(_run("nproc") or "1")
    cores = int(
        _run("grep 'cpu cores' /proc/cpuinfo | head -1 | cut -d: -f2").strip()
        or str(threads)
    )

    # CPU usage from top -bn1
    top_out = _run("top -bn1 | head -5")
    cpu_usage = 0.0
    for line in top_out.split("\n"):
        if "Cpu(s)" in line or "%Cpu" in line:
            # Match idle percentage
            m = re.search(r"(\d+[\.,]\d+)\s*id", line)
            if m:
                idle = float(m.group(1).replace(",", "."))
                cpu_usage = 100.0 - idle
            break

    # Memory from /proc/meminfo
    meminfo = _run("cat /proc/meminfo")
    mem: dict[str, float] = {}
    for line in meminfo.split("\n"):
        mparts = line.split()
        if len(mparts) >= 2:
            key = mparts[0].rstrip(":")
            val = int(mparts[1]) / 1024 / 1024  # KB to GB
            mem[key] = val

    total_gb = mem.get("MemTotal", 0)
    available_gb = mem.get("MemAvailable", 0)
    used_gb = total_gb - available_gb
    buffers_gb = mem.get("Buffers", 0)
    cached_gb = mem.get("Cached", 0)

    swap_total = mem.get("SwapTotal", 0)
    swap_free = mem.get("SwapFree", 0)
    swap_used = swap_total - swap_free

    # Disks from df
    df_out = _run(
        "df -BG --output=source,target,fstype,size,used,avail,pcent 2>/dev/null || df -h"
    )
    disks = []
    for line in df_out.split("\n")[1:]:  # skip header
        dparts = line.split()
        if len(dparts) >= 7 and (
            dparts[1].startswith("/")
            and not dparts[1].startswith("/snap")
            and not dparts[1].startswith("/boot")
        ):
            try:
                disks.append(
                    {
                        "device": dparts[0],
                        "mount": dparts[1],
                        "filesystem": dparts[2],
                        "total_gb": float(dparts[3].rstrip("G")),
                        "used_gb": float(dparts[4].rstrip("G")),
                        "available_gb": float(dparts[5].rstrip("G")),
                        "usage_pct": float(dparts[6].rstrip("%")),
                    }
                )
            except Exception:
                pass

    # GPU (check nvidia-smi)
    gpu_available = False
    gpu_devices: list[dict] = []
    nvidia = _run(
        "nvidia-smi --query-gpu=name,memory.total,memory.used,utilization.gpu,temperature.gpu "
        "--format=csv,noheader,nounits 2>/dev/null"
    )
    if nvidia:
        gpu_available = True
        for gline in nvidia.split("\n"):
            gparts = [p.strip() for p in gline.split(",")]
            if len(gparts) >= 4:
                gpu_devices.append(
                    {
                        "name": gparts[0],
                        "memory_total_mb": float(gparts[1]),
                        "memory_used_mb": float(gparts[2]),
                        "utilization_pct": float(gparts[3]),
                        "temperature_c": float(gparts[4]) if len(gparts) >= 5 else None,
                    }
                )

    # Processes — top 50 by CPU usage
    ps_out = _run("ps aux --sort=-%cpu | head -51")  # top 50 + header
    processes = []
    for line in ps_out.split("\n")[1:]:  # skip header
        pparts = line.split(None, 10)
        if len(pparts) >= 11:
            try:
                processes.append(
                    {
                        "pid": int(pparts[1]),
                        "user": pparts[0],
                        "cpu_pct": float(pparts[2]),
                        "mem_pct": float(pparts[3]),
                        "vsz_mb": round(int(pparts[4]) / 1024, 1),
                        "rss_mb": round(int(pparts[5]) / 1024, 1),
                        "state": pparts[7],
                        "started": pparts[8],
                        "time": pparts[9],
                        "command": pparts[10][:200],
                    }
                )
            except Exception:
                pass

    # Network interfaces
    interfaces = []
    ip_out = _run("ip -4 -o addr show")
    for line in ip_out.split("\n"):
        nparts = line.split()
        if len(nparts) >= 4:
            iface_name = nparts[1]
            ip_addr = nparts[3].split("/")[0]
            if iface_name != "lo":
                # Get rx/tx bytes from /sys/class/net
                rx = _run(
                    f"cat /sys/class/net/{iface_name}/statistics/rx_bytes 2>/dev/null"
                )
                tx = _run(
                    f"cat /sys/class/net/{iface_name}/statistics/tx_bytes 2>/dev/null"
                )
                interfaces.append(
                    {
                        "name": iface_name,
                        "ip": ip_addr,
                        "rx_mb": round(int(rx) / 1024 / 1024, 1) if rx else 0,
                        "tx_mb": round(int(tx) / 1024 / 1024, 1) if tx else 0,
                    }
                )

    return {
        "hostname": hostname,
        "uptime": (
            uptime_raw.replace("up ", "") if uptime_raw.startswith("up ") else uptime_raw
        ),
        "load_avg": load_avg,
        "cpu": {
            "cores": cores,
            "threads": threads,
            "model": cpu_model,
            "usage_pct": round(cpu_usage, 1),
            "per_core": [],
        },
        "memory": {
            "total_gb": round(total_gb, 1),
            "used_gb": round(used_gb, 1),
            "available_gb": round(available_gb, 1),
            "usage_pct": round(used_gb / total_gb * 100, 1) if total_gb > 0 else 0,
            "buffers_gb": round(buffers_gb, 1),
            "cached_gb": round(cached_gb, 1),
        },
        "swap": {
            "total_gb": round(swap_total, 1),
            "used_gb": round(swap_used, 1),
            "usage_pct": (
                round(swap_used / swap_total * 100, 1) if swap_total > 0 else 0
            ),
        },
        "disks": disks,
        "gpu": {"available": gpu_available, "devices": gpu_devices},
        "processes": processes,
        "network": {"interfaces": interfaces},
        "timestamp": time.time(),
    }


@router.get("/capabilities")
async def system_capabilities():
    """Return hardware capabilities and detected configuration."""
    return {
        "cpu_count": CPU_COUNT,
        "ram_gb": RAM_GB,
        "gpu_available": GPU_AVAILABLE,
        "gpu_name": GPU_NAME,
        "gpu_memory_mb": GPU_MEMORY_MB,
        "dv_shards": DV_SHARDS,
        "align_threads": ALIGN_THREADS,
    }


@router.get("/setup-status")
async def setup_status():
    """Check what components are installed and what's missing."""
    return get_setup_status()


@router.post("/setup-run")
async def run_setup():
    """Run setup.sh and stream output via SSE."""
    import pathlib
    script = pathlib.Path(__file__).parent.parent.parent / "setup.sh"
    if not script.exists():
        return JSONResponse({"error": "setup.sh not found"}, 404)

    async def stream():
        proc = await asyncio.create_subprocess_exec(
            "bash", str(script),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace")
            yield f"data: {text}\n\n"
        await proc.wait()
        yield f"data: [SETUP_EXIT_CODE:{proc.returncode}]\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


# ── Settings / Environment Management ──────────────────────────────

_ENV_FILE = Path(__file__).parent.parent.parent / ".env"
_SAFE_KEYS = {"ANTHROPIC_API_KEY", "OPENAI_API_KEY", "JWT_SECRET", "REDIS_URL",
              "GENOMICS_DATA_DIR", "GENOMICS_SCRATCH_DIR", "GENOMICS_PORT",
              "AI_REPORT_MODEL"}


def _load_env_file() -> dict:
    """Load .env file as dict."""
    env = {}
    if _ENV_FILE.exists():
        for line in _ENV_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _save_env_file(env: dict):
    """Save dict to .env file."""
    lines = []
    for k, v in sorted(env.items()):
        lines.append(f'{k}="{v}"')
    _ENV_FILE.write_text("\n".join(lines) + "\n")


@router.get("/settings")
async def get_settings():
    """Return current settings (env vars). Masks sensitive values."""
    env = _load_env_file()
    # Also check actual os.environ for keys not in .env
    for k in _SAFE_KEYS:
        if k not in env and os.environ.get(k):
            env[k] = os.environ[k]

    # Mask sensitive values
    masked = {}
    for k, v in env.items():
        if k in _SAFE_KEYS:
            if "KEY" in k or "SECRET" in k:
                masked[k] = v[:8] + "..." + v[-4:] if len(v) > 12 else ("***" if v else "")
            else:
                masked[k] = v
    # Add empty entries for unset keys
    for k in _SAFE_KEYS:
        if k not in masked:
            masked[k] = ""

    return {"settings": masked, "ai_enabled": bool(os.environ.get("ANTHROPIC_API_KEY") or env.get("ANTHROPIC_API_KEY"))}


from pydantic import BaseModel as _BM

class _SetEnvReq(_BM):
    key: str
    value: str


@router.post("/settings")
async def set_setting(req: _SetEnvReq):
    """Set an environment variable. Persists to .env file and applies immediately."""
    if req.key not in _SAFE_KEYS:
        return JSONResponse({"error": f"Key '{req.key}' is not allowed. Allowed: {sorted(_SAFE_KEYS)}"}, 400)

    # Save to .env file
    env = _load_env_file()
    if req.value:
        env[req.key] = req.value
    else:
        env.pop(req.key, None)
    _save_env_file(env)

    # Apply to current process
    if req.value:
        os.environ[req.key] = req.value
    else:
        os.environ.pop(req.key, None)

    # Reload AI client if API key changed
    if req.key == "ANTHROPIC_API_KEY":
        try:
            import backend.config as _cfg
            _cfg.ANTHROPIC_API_KEY = req.value
            import backend.api.ai_reports as _ai
            _ai._client = None  # Force re-init
        except Exception:
            pass

    return {"ok": True, "key": req.key, "applied": True}


@router.post("/claude-login")
async def claude_login():
    """Start Claude Code login flow in the tmux session."""
    import subprocess
    try:
        # Check if tmux session exists
        result = subprocess.run(["tmux", "has-session", "-t", "genomics-claude"],
                                capture_output=True, timeout=5)
        if result.returncode != 0:
            # Create session
            subprocess.run(["tmux", "new-session", "-d", "-s", "genomics-claude", "-c", str(Path(__file__).parent.parent.parent)],
                           capture_output=True, timeout=5)
        # Send claude login command
        subprocess.run(["tmux", "send-keys", "-t", "genomics-claude", "claude login", "Enter"],
                        capture_output=True, timeout=5)
        return {"ok": True, "message": "Claude login started. Check the AI Assistant terminal tab for the login prompt."}
    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)


@router.get("/claude-status")
async def claude_status():
    """Check if Claude Code is authenticated."""
    import subprocess
    try:
        result = subprocess.run(["claude", "--version"], capture_output=True, text=True, timeout=5)
        installed = result.returncode == 0
        version = result.stdout.strip() if installed else None
    except Exception:
        installed = False
        version = None

    return {
        "installed": installed,
        "version": version,
        "session_active": _check_tmux_session(),
    }


def _check_tmux_session():
    import subprocess
    try:
        result = subprocess.run(["tmux", "has-session", "-t", "genomics-claude"],
                                capture_output=True, timeout=5)
        return result.returncode == 0
    except Exception:
        return False

# --- Analysis docs endpoint ---
import pathlib as _pathlib

_DOCS_DIR = _pathlib.Path(__file__).parent.parent / "data"

@router.get("/docs/{doc_name}")
async def get_doc(doc_name: str):
    """Return markdown content for a documentation file."""
    safe_name = doc_name.replace("..", "").replace("/", "")
    if not safe_name.endswith(".md"):
        safe_name += ".md"
    path = _DOCS_DIR / safe_name
    if not path.exists():
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Document not found")
    return {"name": safe_name, "content": path.read_text(encoding="utf-8")}
