#!/usr/bin/env bash
#
# setup_data.sh — Install all external data dependencies for simple-genomics
#
# Run on the target server (genom-beast-gpu) as a user with sudo access.
# Assumes the genomics conda environment is at:
#   /home/nimo/miniconda3/envs/genomics/
#
# Usage:
#   bash setup_data.sh [--all | --clinvar | --haplogroups | --t1k | --haplogrep3 | --prereqs | --verify]
#   Without arguments, installs everything.
#
set -euo pipefail

BCFTOOLS="/home/nimo/miniconda3/envs/genomics/bin/bcftools"
PIP="/home/nimo/miniconda3/envs/genomics/bin/pip"
PYTHON="/home/nimo/miniconda3/envs/genomics/bin/python"
GENOMICS_BIN="/home/nimo/miniconda3/envs/genomics/bin"

# ── Helpers ──────────────────────────────────────────────────────────
log() { echo "[$(date +%H:%M:%S)] $*"; }
ensure_dir() { sudo mkdir -p "$1" && sudo chown "$(whoami):$(id -gn)" "$1"; }

# ── 0. System prerequisites ─────────────────────────────────────────
install_prereqs() {
    log "Checking system prerequisites..."

    local MISSING=()

    command -v java &>/dev/null || MISSING+=("default-jre-headless")
    command -v unzip &>/dev/null || MISSING+=("unzip")
    command -v wget &>/dev/null || MISSING+=("wget")
    command -v git &>/dev/null || MISSING+=("git")
    command -v make &>/dev/null || MISSING+=("build-essential")

    if [ ${#MISSING[@]} -gt 0 ]; then
        log "  Installing missing packages: ${MISSING[*]}"
        sudo apt-get update -qq
        sudo apt-get install -y -qq "${MISSING[@]}"
    else
        log "  All system prerequisites present"
    fi

    # Python packages needed by the scoring engine
    log "  Checking Python packages..."
    $PIP install -q pyliftover 2>/dev/null || true
}

# ── 1. ClinVar VCF (NCBI, GRCh38) ───────────────────────────────────
install_clinvar() {
    log "Installing ClinVar VCF..."
    ensure_dir /data/clinvar
    cd /data/clinvar

    # Bare-chromosome version (1, 2, ... X, Y, MT)
    if [ ! -f clinvar.vcf.gz ] || [ ! -f clinvar.vcf.gz.tbi ]; then
        log "  Downloading ClinVar GRCh38 (bare chrom)..."
        wget -q "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz" -O clinvar.vcf.gz
        wget -q "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz.tbi" -O clinvar.vcf.gz.tbi
    else
        log "  clinvar.vcf.gz already exists, skipping download"
    fi

    # chr-prefixed version (chr1, chr2, ... chrX, chrY, chrM)
    if [ ! -f clinvar_chr.vcf.gz ]; then
        log "  Creating chr-prefixed version..."
        $BCFTOOLS annotate \
            --rename-chrs <(for i in $(seq 1 22) X Y MT; do echo "$i chr$i"; done) \
            clinvar.vcf.gz -Oz -o clinvar_chr.vcf.gz
        $BCFTOOLS index -t clinvar_chr.vcf.gz
    else
        log "  clinvar_chr.vcf.gz already exists, skipping"
    fi

    log "  ClinVar installed: $(ls -lh clinvar.vcf.gz clinvar_chr.vcf.gz | awk '{print $5, $9}')"
}

# ── 2. Haplogroup reference data ─────────────────────────────────────
install_haplogroups() {
    log "Installing haplogroup reference data..."
    ensure_dir /data/haplogroup_data

    # Install pyliftover if needed (for Y-DNA liftover from hg19→hg38)
    $PIP install -q pyliftover 2>/dev/null || true

    # Run the build script
    cd /home/nimrod_rotem/simple-genomics
    if [ -f scripts/build_haplogroup_data.py ]; then
        log "  Running build_haplogroup_data.py..."
        $PYTHON scripts/build_haplogroup_data.py
    else
        log "  WARNING: scripts/build_haplogroup_data.py not found"
        log "  Creating minimal haplogroup data from built-in markers..."
        $PYTHON -c "
import json
from pathlib import Path
OUT = Path('/data/haplogroup_data')
OUT.mkdir(parents=True, exist_ok=True)

# mtDNA markers (PhyloTree Build 17)
mt = [
    (16223,'T','L'),(146,'C','L'),(182,'T','L1'),(247,'A','L1b'),
    (769,'A','L1'),(825,'A','L0'),(1018,'A','L0'),(2758,'A','L0'),
    (2885,'C','L1'),(7256,'T','L2'),(8655,'T','L2'),(10115,'C','L2'),
    (12693,'A','L2'),(13789,'C','L2'),(15784,'C','L2'),(16278,'T','L2'),
    (16390,'A','L2'),(2352,'C','L3'),(3594,'T','L3'),(4104,'G','L3'),
    (4312,'T','L3'),(8618,'C','L3'),(9540,'C','L3'),(10398,'G','L3'),
    (15301,'A','L3'),(10873,'C','M'),(10400,'T','M'),(10398,'G','N'),
    (12705,'T','R'),(4580,'A','V'),(15904,'T','V'),(12612,'G','J'),
    (13708,'A','J'),(295,'T','J'),(13368,'A','T'),(14905,'A','T'),
    (15607,'G','T'),(15928,'A','T'),(10550,'G','K'),(11299,'C','K'),
    (14798,'C','K'),(11467,'G','U'),(12308,'G','U'),(12372,'A','U'),
    (8251,'A','W'),(8994,'A','W'),(11947,'G','W'),(6371,'T','X'),
    (10034,'C','I'),(4883,'T','D'),(5178,'A','D'),(4824,'G','A'),
    (8794,'T','A'),(16290,'T','A'),(16189,'C','B'),(3552,'A','C'),
    (13263,'G','C'),(709,'A','G'),(4833,'G','G'),(3970,'T','F'),
    (10310,'A','F'),(12406,'A','F'),
]
json.dump([{'pos':p,'alt':a,'haplogroup':h} for p,a,h in mt], open(OUT/'mtdna_snps.json','w'))
print(f'  mtDNA: {len(mt)} markers')

# Neanderthal tag SNVs
ne = [
    ('chr9',16409501,'C','T'),('chr11',120315373,'G','A'),
    ('chr3',50319389,'T','C'),('chr6',33071555,'A','G'),
    ('chr12',56738814,'G','A'),('chr7',114654925,'A','G'),
    ('chr17',6878655,'T','C'),('chr17',6879190,'G','C'),
    ('chr13',32912813,'C','G'),('chr12',52813570,'G','T'),
    ('chr1',118594854,'C','T'),('chr19',10491135,'G','A'),
    ('chr4',33977175,'A','G'),('chr18',34877650,'T','C'),
    ('chr8',6327185,'C','T'),('chr12',112913362,'T','C'),
    ('chr2',102412147,'G','A'),('chr4',38822136,'A','G'),
    ('chr6',32362488,'C','T'),('chr6',29942437,'G','A'),
]
json.dump([{'chrom':c,'pos':p,'ref':r,'neanderthal_allele':n} for c,p,r,n in ne],
          open(OUT/'neanderthal_snps_grch38.json','w'))
print(f'  Neanderthal: {len(ne)} tag SNVs')
"
    fi

    log "  Haplogroup data: $(ls /data/haplogroup_data/)"
}

# ── 3. T1K (HLA typing) ─────────────────────────────────────────────
install_t1k() {
    log "Installing T1K for HLA typing..."

    # Build T1K from source
    if [ ! -f "$GENOMICS_BIN/run-t1k" ]; then
        log "  Building T1K from source..."
        cd /tmp
        rm -rf t1k_build
        git clone --quiet https://github.com/mourisl/T1K.git t1k_build
        cd t1k_build
        make -j4 > /dev/null 2>&1
        sudo cp run-t1k bam-extractor genotyper "$GENOMICS_BIN/"
        sudo chmod +x "$GENOMICS_BIN/run-t1k" "$GENOMICS_BIN/bam-extractor" "$GENOMICS_BIN/genotyper"

        # Build HLA reference database
        ensure_dir /data/t1k_ref/hla
        perl t1k-build.pl --download IPD-IMGT/HLA -o /data/t1k_ref/hla --prefix hla

        # Generate coordinate file from GENCODE annotation
        log "  Downloading GENCODE GTF for coord file..."
        cd /data/t1k_ref/hla
        wget -q "https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_44/gencode.v44.chr_patch_hapl_scaff.annotation.gtf.gz" -O gencode.gtf.gz
        gunzip -f gencode.gtf.gz
        perl /tmp/t1k_build/AddGeneCoord.pl hla_dna_seq.fa gencode.gtf > hla_dna_coord.fa

        # Cleanup
        rm -f gencode.gtf hla.dat hla.dat.zip
        rm -rf /tmp/t1k_build
        log "  T1K installed with HLA reference"
    else
        log "  T1K already installed at $GENOMICS_BIN/run-t1k"
    fi
}

# ── 4. HaploGrep3 (mtDNA haplogroup classification) ─────────────────
install_haplogrep3() {
    log "Installing HaploGrep3..."

    HGDIR="/home/nimrod_rotem/tools/haplogrep3"

    # Install Java if missing
    if ! command -v java &>/dev/null; then
        log "  Installing Java JRE..."
        sudo apt-get update -qq
        sudo apt-get install -y -qq default-jre-headless unzip
    fi

    if [ ! -f "$HGDIR/haplogrep3" ]; then
        mkdir -p "$HGDIR"
        cd "$HGDIR"
        log "  Downloading HaploGrep3 v3.2.1..."
        wget -q "https://github.com/genepi/haplogrep3/releases/download/v3.2.1/haplogrep3-3.2.1-linux.zip" -O haplogrep3.zip
        unzip -o haplogrep3.zip >/dev/null
        chmod +x haplogrep3
        rm -f haplogrep3.zip
    else
        log "  HaploGrep3 already installed"
    fi

    # Install PhyloTree
    "$HGDIR/haplogrep3" install-tree phylotree-rcrs@17.2 2>/dev/null || true
    log "  HaploGrep3 ready with PhyloTree 17.2"
}

# ── 5. Verification ─────────────────────────────────────────────────
verify_install() {
    log "=== Verifying installation ==="
    local ERRORS=0

    # Check tools
    for tool in bcftools samtools plink2 plink; do
        if [ -x "$GENOMICS_BIN/$tool" ]; then
            log "  [OK] $tool: $($GENOMICS_BIN/$tool --version 2>&1 | head -1)"
        else
            log "  [FAIL] $tool not found at $GENOMICS_BIN/$tool"
            ERRORS=$((ERRORS + 1))
        fi
    done

    # Check Java
    if command -v java &>/dev/null; then
        log "  [OK] java: $(java -version 2>&1 | head -1)"
    else
        log "  [FAIL] java not found"
        ERRORS=$((ERRORS + 1))
    fi

    # Check data files
    local FILES=(
        "/data/clinvar/clinvar.vcf.gz"
        "/data/clinvar/clinvar_chr.vcf.gz"
        "/data/haplogroup_data/mtdna_snps.json"
        "/data/haplogroup_data/neanderthal_snps_grch38.json"
        "/data/refs/hs38DH.fa"
        "/data/pgs2/ref_panel/GRCh38_1000G_ALL.pgen"
        "/data/pgs2/ref_panel/GRCh38_1000G_ALL.psam"
    )
    for f in "${FILES[@]}"; do
        if [ -f "$f" ]; then
            log "  [OK] $f ($(du -h "$f" | cut -f1))"
        else
            log "  [MISSING] $f"
            ERRORS=$((ERRORS + 1))
        fi
    done

    # Check ref panel stats
    local STATS_COUNT
    STATS_COUNT=$(find /data/pgs2/ref_panel_stats -name "*.json" 2>/dev/null | wc -l)
    if [ "$STATS_COUNT" -gt 0 ]; then
        log "  [OK] ref_panel_stats: $STATS_COUNT precomputed stat files"
    else
        log "  [WARN] No precomputed ref panel stats found — run scripts/build_ref_panel_stats.py"
    fi

    # Check HaploGrep3
    if [ -x "/home/nimrod_rotem/tools/haplogrep3/haplogrep3" ]; then
        log "  [OK] HaploGrep3 installed"
    else
        log "  [MISSING] HaploGrep3"
        ERRORS=$((ERRORS + 1))
    fi

    # Check T1K
    if [ -x "$GENOMICS_BIN/run-t1k" ]; then
        log "  [OK] T1K installed"
    else
        log "  [MISSING] T1K (run-t1k)"
        ERRORS=$((ERRORS + 1))
    fi

    # Check Python imports
    if $PYTHON -c "import fastapi, uvicorn, pydantic" 2>/dev/null; then
        log "  [OK] Python packages (fastapi, uvicorn, pydantic)"
    else
        log "  [FAIL] Missing Python packages — run: pip install -r requirements.txt"
        ERRORS=$((ERRORS + 1))
    fi

    echo
    if [ $ERRORS -eq 0 ]; then
        log "=== All checks passed ==="
    else
        log "=== $ERRORS issue(s) found ==="
    fi
    return $ERRORS
}

# ── Main ─────────────────────────────────────────────────────────────


# ── 5. ExpansionHunter (repeat expansion caller) ─────────────────────
install_expansion_hunter() {
    log "Installing ExpansionHunter v5.0.0..."
    local EH_VER="v5.0.0"
    local EH_URL="https://github.com/Illumina/ExpansionHunter/releases/download/${EH_VER}/ExpansionHunter-${EH_VER}-linux_x86_64.tar.gz"

    if [ -f /usr/local/bin/ExpansionHunter ]; then
        log "  ExpansionHunter already installed"
        return
    fi

    cd /tmp
    wget -q "$EH_URL" -O eh.tar.gz
    tar xzf eh.tar.gz
    sudo cp "ExpansionHunter-${EH_VER}-linux_x86_64/bin/ExpansionHunter" /usr/local/bin/
    sudo chmod +x /usr/local/bin/ExpansionHunter
    sudo mkdir -p /opt/expansion-hunter
    sudo cp -r "ExpansionHunter-${EH_VER}-linux_x86_64/variant_catalog" /opt/expansion-hunter/
    rm -rf eh.tar.gz "ExpansionHunter-${EH_VER}-linux_x86_64"
    log "  ExpansionHunter installed"
}

# ── 6. Cyrius (CYP2D6 star-allele caller) ────────────────────────────
install_cyrius() {
    log "Installing Cyrius..."
    if [ -d /opt/cyrius ]; then
        log "  Cyrius already installed"
        return
    fi
    sudo git clone https://github.com/Illumina/Cyrius.git /opt/cyrius
    $PIP install -q pysam scipy statsmodels 2>/dev/null || true
    log "  Cyrius installed at /opt/cyrius"
}

# ── 7. UCSC liftOver binary ──────────────────────────────────────────
install_liftover() {
    log "Installing UCSC liftOver..."
    local SG_DIR="/home/nimrod_rotem/simple-genomics"
    mkdir -p "$SG_DIR/liftover"
    if [ -f "$SG_DIR/liftover/liftOver" ]; then
        log "  liftOver already installed"
        return
    fi
    cd "$SG_DIR/liftover"
    wget -q "https://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64/liftOver" -O liftOver
    chmod +x liftOver
    wget -q "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/liftOver/hg38ToHg19.over.chain.gz" -O hg38ToHg19.over.chain.gz
    log "  liftOver installed"
}

main() {
    local target="${1:---all}"

    log "=== simple-genomics data setup ==="

    case "$target" in
        --all)
            install_prereqs
            install_clinvar
            install_haplogroups
            install_t1k
            install_haplogrep3
            install_expansion_hunter
            install_cyrius
            install_liftover
            verify_install
            ;;
        --prereqs)       install_prereqs ;;
        --clinvar)       install_clinvar ;;
        --haplogroups)   install_haplogroups ;;
        --t1k)           install_t1k ;;
        --haplogrep3)    install_haplogrep3 ;;
        --eh)            install_expansion_hunter ;;
        --cyrius)        install_cyrius ;;
        --liftover)      install_liftover ;;
        --verify)        verify_install ;;
        *)
            echo "Usage: $0 [--all | --prereqs | --clinvar | --haplogroups | --t1k | --haplogrep3 | --verify]"
            exit 1
            ;;
    esac

    if [ "$target" != "--verify" ]; then
        log "=== Setup complete ==="
        echo
        echo "Data locations:"
        echo "  ClinVar:     /data/clinvar/{clinvar.vcf.gz, clinvar_chr.vcf.gz}"
        echo "  Haplogroups: /data/haplogroup_data/{ydna_snps_grch38.json, mtdna_snps.json, neanderthal_snps_grch38.json}"
        echo "  T1K HLA:     /data/t1k_ref/hla/{hla_dna_seq.fa, hla_dna_coord.fa}"
        echo "  HaploGrep3:  /home/nimrod_rotem/tools/haplogrep3/haplogrep3"
        echo "  ExpHunter:   /usr/local/bin/ExpansionHunter + /opt/expansion-hunter/variant_catalog/"
        echo "  Cyrius:      /opt/cyrius/star_caller.py"
        echo "  liftOver:    simple-genomics/liftover/liftOver"
        echo
        echo "Restart the server: sudo supervisorctl restart simple-genomics"
    fi
}

main "$@"
