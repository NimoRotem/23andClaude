#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# Genomics Dashboard — Auto Setup
# Downloads reference genome, 1000G panel, containers, installs deps
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${GENOMICS_DATA_DIR:-/data}"
SCRATCH_DIR="${GENOMICS_SCRATCH_DIR:-/scratch}"
CONDA_ENV_NAME="genomics"
SETUP_LOG="$DATA_DIR/app/setup.log"
SETUP_STATUS="$DATA_DIR/app/setup_status.json"

# ── Colors ─────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

step_num=0
total_steps=0

log()  { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*"; }
info() { echo -e "${BLUE}ℹ${NC}  $*"; }
ok()   { echo -e "${GREEN}✓${NC}  $*"; }
warn() { echo -e "${YELLOW}⚠${NC}  $*"; }
err()  { echo -e "${RED}✗${NC}  $*"; }
step() {
  step_num=$((step_num + 1))
  echo ""
  echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
  echo -e "${BOLD}  Step ${step_num}/${total_steps}: $1${NC}"
  echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
  update_status "$1" "running"
}

update_status() {
  local step_name="$1" status="$2"
  mkdir -p "$(dirname "$SETUP_STATUS")"
  cat > "$SETUP_STATUS" <<STATUSEOF
{
  "current_step": "$step_name",
  "step_number": $step_num,
  "total_steps": $total_steps,
  "status": "$status",
  "timestamp": "$(date -Iseconds)",
  "hardware": {
    "cpu_count": $CPU_COUNT,
    "ram_gb": $RAM_GB,
    "gpu_available": $GPU_AVAILABLE,
    "gpu_name": "$GPU_NAME"
  }
}
STATUSEOF
}

# ── Hardware Detection ─────────────────────────────────────────────
detect_hardware() {
  echo -e "${BOLD}Detecting hardware...${NC}"
  echo ""

  CPU_COUNT=$(nproc 2>/dev/null || echo 4)
  RAM_KB=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo 8000000)
  RAM_GB=$((RAM_KB / 1024 / 1024))

  if command -v nvidia-smi &>/dev/null && nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 | grep -qi .; then
    GPU_AVAILABLE=true
    GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 | xargs)
    GPU_MEM=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null | head -1 | xargs)
    info "GPU:  ${GREEN}$GPU_NAME${NC} (${GPU_MEM} MiB)"
  else
    GPU_AVAILABLE=false
    GPU_NAME="none"
    GPU_MEM=0
    info "GPU:  ${YELLOW}Not detected${NC} (DeepVariant will use CPU only)"
  fi

  info "CPU:  ${CPU_COUNT} cores"
  info "RAM:  ${RAM_GB} GB"

  # Determine optimal thread counts
  if [ "$CPU_COUNT" -ge 32 ]; then
    DV_SHARDS=20
    ALIGN_THREADS=24
  elif [ "$CPU_COUNT" -ge 16 ]; then
    DV_SHARDS=12
    ALIGN_THREADS=12
  elif [ "$CPU_COUNT" -ge 8 ]; then
    DV_SHARDS=8
    ALIGN_THREADS=6
  else
    DV_SHARDS=4
    ALIGN_THREADS=3
  fi
  info "Optimal shards: $DV_SHARDS | Align threads: $ALIGN_THREADS"
  echo ""
}

# ── Count steps ────────────────────────────────────────────────────
count_steps() {
  total_steps=6  # dirs, conda, frontend, reference, panel, containers
  if [ "$GPU_AVAILABLE" = true ]; then
    total_steps=7  # +GPU container
  fi
}

# ── Step 1: Create directories ─────────────────────────────────────
setup_directories() {
  step "Create data directories"

  local dirs=(
    "$DATA_DIR/aligned_bams"
    "$DATA_DIR/vcfs"
    "$DATA_DIR/pgs_cache"
    "$DATA_DIR/runs"
    "$DATA_DIR/app"
    "$DATA_DIR/uploads"
    "$DATA_DIR/refs"
    "$DATA_DIR/pgen_cache"
    "$DATA_DIR/containers"
    "$DATA_DIR/pgs2/ref_panel"
    "$DATA_DIR/pgs2/ref_panel/pop_samples"
    "$DATA_DIR/pgs2/plink2_scoring_files"
    "$DATA_DIR/pgs2/ref_panel_stats"
    "$SCRATCH_DIR/nimog_output"
    "$SCRATCH_DIR/runs"
    "$SCRATCH_DIR/pipeline"
    "$SCRATCH_DIR/alignments"
    "$SCRATCH_DIR/tmp"
    "$SCRATCH_DIR/vcfs"
    "$SCRATCH_DIR/bams"
    "$SCRATCH_DIR/refs"
    "$SCRATCH_DIR/pgs_cache"
  )

  for d in "${dirs[@]}"; do
    mkdir -p "$d" 2>/dev/null || sudo mkdir -p "$d"
    ok "$d"
  done

  # Ensure writable
  if [ "$(stat -c %U "$DATA_DIR" 2>/dev/null)" != "$USER" ]; then
    warn "Setting ownership of $DATA_DIR to $USER..."
    sudo chown -R "$USER:$USER" "$DATA_DIR" 2>/dev/null || true
  fi
  if [ "$(stat -c %U "$SCRATCH_DIR" 2>/dev/null)" != "$USER" ]; then
    sudo chown -R "$USER:$USER" "$SCRATCH_DIR" 2>/dev/null || true
  fi

  ok "All directories created"
}

# ── Step 2: Conda environment ──────────────────────────────────────
setup_conda() {
  step "Set up conda environment"

  # Find conda
  local conda_bin=""
  if command -v conda &>/dev/null; then
    conda_bin="conda"
  elif [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
    source "$HOME/miniconda3/etc/profile.d/conda.sh"
    conda_bin="conda"
  elif [ -f "$HOME/miniforge3/etc/profile.d/conda.sh" ]; then
    source "$HOME/miniforge3/etc/profile.d/conda.sh"
    conda_bin="conda"
  elif [ -f "$HOME/mambaforge/etc/profile.d/conda.sh" ]; then
    source "$HOME/mambaforge/etc/profile.d/conda.sh"
    conda_bin="conda"
  fi

  if [ -z "$conda_bin" ]; then
    warn "Conda not found. Installing Miniconda..."
    curl -sSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p "$HOME/miniconda3"
    rm /tmp/miniconda.sh
    source "$HOME/miniconda3/etc/profile.d/conda.sh"
    conda_bin="conda"
    ok "Miniconda installed"
  fi

  # Check if env exists
  if conda env list | grep -q "^${CONDA_ENV_NAME} "; then
    info "Conda env '${CONDA_ENV_NAME}' already exists"
    conda activate "$CONDA_ENV_NAME"

    # Verify key tools
    local missing=()
    for tool in bcftools samtools plink2 bwa minimap2; do
      if ! command -v "$tool" &>/dev/null; then
        missing+=("$tool")
      fi
    done

    if [ ${#missing[@]} -gt 0 ]; then
      warn "Missing tools: ${missing[*]}. Installing..."
      conda install -y -c bioconda -c conda-forge "${missing[@]}"
    fi
  else
    info "Creating conda env '${CONDA_ENV_NAME}'..."
    conda env create -f "$SCRIPT_DIR/environment.yml"
    conda activate "$CONDA_ENV_NAME"
    ok "Conda env created"
  fi

  # Install Python deps
  info "Installing Python packages..."
  pip install -q -r "$SCRIPT_DIR/requirements.txt"

  # Verify
  for tool in bcftools samtools plink2 bwa minimap2; do
    if command -v "$tool" &>/dev/null; then
      local ver=$($tool --version 2>&1 | head -1 || echo "?")
      ok "$tool: $ver"
    else
      err "$tool: NOT FOUND"
    fi
  done

  ok "Conda environment ready"
}

# ── Step 3: Build frontend ─────────────────────────────────────────
setup_frontend() {
  step "Build frontend"

  if ! command -v node &>/dev/null; then
    warn "Node.js not found. Installing via nvm..."
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
    export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && source "$NVM_DIR/nvm.sh"
    nvm install 20
    ok "Node.js installed"
  fi

  info "Node $(node --version), npm $(npm --version)"

  cd "$SCRIPT_DIR/frontend"
  if [ ! -d "node_modules" ]; then
    info "Installing npm dependencies..."
    npm install --silent
  fi

  info "Building React app..."
  npx vite build
  ok "Frontend built: frontend/dist/"
  cd "$SCRIPT_DIR"
}

# ── Step 4: Reference genome ──────────────────────────────────────
setup_reference() {
  step "Download GRCh38 reference genome"

  local ref_dir="$DATA_DIR/refs"
  local ref_fa="$ref_dir/GRCh38.fa"

  if [ -f "$ref_fa" ] && [ -f "${ref_fa}.fai" ]; then
    ok "Reference genome already exists ($(du -sh "$ref_fa" | cut -f1))"
    return
  fi

  # Also check for existing reference at common paths
  for candidate in "$DATA_DIR/refs/GRCh38.fa" "$DATA_DIR/refs/GRCh38.fa" "$HOME/reference/GRCh38.fa"; do
    if [ -f "$candidate" ] && [ -f "${candidate}.fai" ]; then
      info "Found existing reference at $candidate"
      if [ "$candidate" != "$ref_fa" ]; then
        ln -sf "$candidate" "$ref_fa"
        ln -sf "${candidate}.fai" "${ref_fa}.fai"
        # Link BWA indices if they exist
        for ext in bwt pac sa amb ann; do
          [ -f "${candidate}.${ext}" ] && ln -sf "${candidate}.${ext}" "${ref_fa}.${ext}"
        done
      fi
      ok "Reference genome linked"
      return
    fi
  done

  info "Downloading GRCh38 reference (~800 MB compressed, ~3.1 GB uncompressed)..."
  info "This may take 10-30 minutes depending on your connection."
  local url="https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz"
  curl -L --progress-bar "$url" -o "${ref_fa}.gz"

  info "Decompressing..."
  gunzip "${ref_fa}.gz"
  ok "Reference genome: $(du -sh "$ref_fa" | cut -f1)"

  info "Indexing with samtools faidx..."
  samtools faidx "$ref_fa"
  ok "FAI index created"

  info "Indexing with bwa (this takes ~1 hour for WGS)..."
  bwa index "$ref_fa" &
  local bwa_pid=$!
  info "BWA indexing running in background (PID $bwa_pid). You can start using the app while it completes."
  info "Check progress: ls -la ${ref_fa}.bwt"
}

# ── Step 5: 1000 Genomes reference panel ──────────────────────────
setup_ref_panel() {
  step "Download 1000 Genomes reference panel"

  local panel_dir="$DATA_DIR/pgs2/ref_panel"
  local grch38_pgen="$panel_dir/GRCh38_1000G_ALL.pgen"

  if [ -f "$grch38_pgen" ]; then
    ok "Reference panel already exists ($(du -sh "$panel_dir" | cut -f1))"
    return
  fi

  local base_url="https://ftp.ebi.ac.uk/pub/databases/spot/pgs/resources/pgsc_1000G_v1/GRCh38"

  info "Downloading GRCh38 1000G panel (~700 MB total)..."
  cd "$panel_dir"

  for f in pgsc_1000G_v1_ALL_GRCh38_no_dups.pgen pgsc_1000G_v1_ALL_GRCh38_no_dups.pvar.zst pgsc_1000G_v1_ALL_GRCh38_no_dups.psam; do
    local ext="${f#*.}"
    local target="GRCh38_1000G_ALL.${ext}"
    if [ -f "$target" ]; then
      ok "$target exists"
    else
      info "Downloading $f..."
      curl -L --progress-bar "$base_url/$f" -o "$target"
      ok "$target ($(du -sh "$target" | cut -f1))"
    fi
  done

  # King cutoff file
  if [ ! -f "$panel_dir/GRCh38_1000G.king.cutoff.out.id" ]; then
    info "Downloading KING cutoff file..."
    curl -L --progress-bar "$base_url/pgsc_1000G_v1_ALL_GRCh38_no_dups.king.cutoff.out.id" \
      -o "$panel_dir/GRCh38_1000G.king.cutoff.out.id" 2>/dev/null || warn "KING cutoff file not available (non-critical)"
  fi

  cd "$SCRIPT_DIR"
  ok "Reference panel ready"
}

# ── Step 6: DeepVariant containers ─────────────────────────────────
setup_containers() {
  step "Download DeepVariant container (CPU)"

  local containers_dir="$DATA_DIR/containers"
  local cpu_sif="$containers_dir/deepvariant_1.6.1.sif"
  local gpu_sif="$containers_dir/deepvariant_1.6.1-gpu.sif"

  if ! command -v apptainer &>/dev/null && ! command -v singularity &>/dev/null; then
    warn "Apptainer/Singularity not found. Skipping container download."
    warn "Install apptainer to enable DeepVariant variant calling."
    warn "  See: https://apptainer.org/docs/admin/main/installation.html"
    return
  fi

  local runner="apptainer"
  command -v apptainer &>/dev/null || runner="singularity"

  if [ -f "$cpu_sif" ]; then
    ok "CPU container exists ($(du -sh "$cpu_sif" | cut -f1))"
  else
    info "Pulling DeepVariant CPU image (~2.8 GB)..."
    $runner pull "$cpu_sif" docker://google/deepvariant:1.6.1
    ok "CPU container: $(du -sh "$cpu_sif" | cut -f1)"
  fi

  if [ "$GPU_AVAILABLE" = true ]; then
    step "Download DeepVariant container (GPU)"
    if [ -f "$gpu_sif" ]; then
      ok "GPU container exists ($(du -sh "$gpu_sif" | cut -f1))"
    else
      info "Pulling DeepVariant GPU image (~11 GB)..."
      $runner pull "$gpu_sif" docker://google/deepvariant:1.6.1-gpu
      ok "GPU container: $(du -sh "$gpu_sif" | cut -f1)"
    fi
  else
    info "No GPU detected — skipping GPU container download"
  fi
}

# ── Step 7: Redis ──────────────────────────────────────────────────
setup_redis() {
  if command -v redis-server &>/dev/null; then
    if redis-cli ping &>/dev/null; then
      ok "Redis is running"
    else
      info "Starting Redis..."
      redis-server --daemonize yes 2>/dev/null || sudo systemctl start redis-server 2>/dev/null || warn "Could not start Redis. Start manually: redis-server --daemonize yes"
    fi
  else
    warn "Redis not installed. Install with: sudo apt install redis-server"
    warn "PGS search caching will be disabled until Redis is available."
  fi
}

# ── Write server config ───────────────────────────────────────────
write_server_config() {
  local config_file="$DATA_DIR/app/server_config.json"
  cat > "$config_file" <<CFGEOF
{
  "cpu_count": $CPU_COUNT,
  "ram_gb": $RAM_GB,
  "gpu_available": $GPU_AVAILABLE,
  "gpu_name": "$GPU_NAME",
  "gpu_memory_mb": $GPU_MEM,
  "dv_shards": $DV_SHARDS,
  "align_threads": $ALIGN_THREADS,
  "data_dir": "$DATA_DIR",
  "scratch_dir": "$SCRATCH_DIR",
  "conda_env": "$CONDA_ENV_NAME",
  "setup_completed": "$(date -Iseconds)"
}
CFGEOF
  ok "Server config written: $config_file"
}

# ── Main ───────────────────────────────────────────────────────────
main() {
  echo ""
  echo -e "${BOLD}╔═══════════════════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}║        Genomics Dashboard — Auto Setup               ║${NC}"
  echo -e "${BOLD}╚═══════════════════════════════════════════════════════╝${NC}"
  echo ""

  detect_hardware
  count_steps

  mkdir -p "$DATA_DIR/app" 2>/dev/null || sudo mkdir -p "$DATA_DIR/app"

  setup_directories
  setup_conda
  setup_frontend
  setup_reference
  setup_ref_panel
  setup_containers
  setup_redis
  write_server_config

  update_status "complete" "completed"

  echo ""
  echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
  echo -e "${GREEN}${BOLD}  Setup complete!${NC}"
  echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
  echo ""
  echo -e "  ${BOLD}Hardware:${NC} ${CPU_COUNT} CPU cores, ${RAM_GB} GB RAM"
  if [ "$GPU_AVAILABLE" = true ]; then
    echo -e "  ${BOLD}GPU:${NC}      ${GREEN}$GPU_NAME${NC} (${GPU_MEM} MiB)"
  else
    echo -e "  ${BOLD}GPU:${NC}      ${YELLOW}None — bcftools mode only${NC}"
  fi
  echo ""
  echo -e "  Start the app:"
  echo -e "    ${CYAN}conda activate genomics${NC}"
  echo -e "    ${CYAN}cd $SCRIPT_DIR${NC}"
  echo -e "    ${CYAN}python -m uvicorn backend.main:app --host 0.0.0.0 --port 8600${NC}"
  echo ""
  echo -e "  Then open: ${BLUE}http://localhost:8600/genomics/${NC}"
  echo ""
}

# Tee to log file if DATA_DIR exists
if [ -d "$DATA_DIR/app" ] 2>/dev/null; then
  main "$@" 2>&1 | tee "$SETUP_LOG"
else
  main "$@" 2>&1
fi
