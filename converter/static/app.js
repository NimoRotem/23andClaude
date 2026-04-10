// nimog frontend — file browser, parameter form, SSE progress, QC display

const BASE = '/convert';
let currentMode = 'bcftools';
let selectedBam = null;       // single-BAM mode (bcftools)
let selectedBams = [];        // multi-BAM mode (deepvariant)
let currentJobId = null;
let gpuAvailable = false;
let eventSource = null;

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
    initChromGrid();
    browsePath('/data/aligned_bams');
    loadHistory();
    checkGpuStatus();
    updateModeUI();
});

// --- GPU Status ---
async function checkGpuStatus() {
    try {
        const res = await fetch(`${BASE}/api/gpu-status`);
        const data = await res.json();
        gpuAvailable = data.available;
        const toggle = document.getElementById('use-gpu');
        const statusText = document.getElementById('gpu-status-text');
        const icon = document.getElementById('gpu-icon');
        const badge = document.getElementById('gpu-badge');
        const badgeIcon = document.getElementById('gpu-badge-icon');
        const badgeText = document.getElementById('gpu-badge-text');
        if (data.available) {
            toggle.checked = true;
            toggle.disabled = false;
            statusText.textContent = data.gpu || 'GPU available';
            icon.textContent = '\u26A1';
            statusText.style.color = '#3fb950';
            // Update always-visible badge
            if (badge) {
                badge.style.borderColor = '#238636';
                badgeIcon.textContent = '\u26A1';
                badgeText.textContent = data.gpu || 'GPU Available';
                badgeText.style.color = '#3fb950';
            }
        } else {
            toggle.checked = false;
            toggle.disabled = true;
            statusText.textContent = 'No GPU detected — CPU mode only';
            icon.textContent = '\uD83D\uDCBB';
            statusText.style.color = '#8b949e';
            if (badge) {
                badge.style.borderColor = '#30363d';
                badgeIcon.textContent = '\uD83D\uDCBB';
                badgeText.textContent = 'No GPU — CPU mode';
                badgeText.style.color = '#8b949e';
            }
        }
    } catch (e) {
        console.error('GPU check failed:', e);
        const badge = document.getElementById('gpu-badge');
        if (badge) {
            document.getElementById('gpu-badge-text').textContent = 'GPU check failed';
        }
    }
}

// --- Pipeline Mode ---
function setMode(mode) {
    currentMode = mode;
    updateModeUI();
}

function updateModeUI() {
    const isBcf = currentMode === 'bcftools';

    // Toggle mode buttons
    document.getElementById('mode-bcftools').classList.toggle('active', isBcf);
    document.getElementById('mode-deepvariant').classList.toggle('active', !isBcf);

    // Show/hide mode-specific params
    document.querySelectorAll('.bcftools-param').forEach(el => {
        el.style.display = isBcf ? '' : 'none';
    });
    document.querySelectorAll('.dv-param').forEach(el => {
        el.style.display = isBcf ? 'none' : '';
    });

    // Update button text
    document.getElementById('btn-convert-text').textContent =
        isBcf ? 'Convert to VCF' : 'Run DeepVariant Pipeline';

    // Update hint
    const hint = document.getElementById('action-hint');
    if (!isBcf) {
        const n = selectedBams.length;
        if (n === 0) {
            hint.textContent = 'Select one or more BAM/CRAM files to start';
        } else if (n === 1) {
            hint.textContent = 'Single sample — DeepVariant only (no joint genotyping)';
        } else {
            hint.textContent = `${n} samples — will run DeepVariant + GLnexus joint genotyping`;
        }
    } else {
        hint.textContent = '';
    }

    // Show multi vs single selection
    document.getElementById('bam-plural').textContent = isBcf ? '' : 's';

    // Refresh file browser to update click behavior
    refreshBrowserSelection();
}

// --- Chromosome Grid ---
function initChromGrid() {
    const grid = document.getElementById('chrom-grid');
    for (let i = 1; i <= 22; i++) {
        const box = document.createElement('div');
        box.className = 'chrom-box selected';
        box.dataset.chrom = i;
        box.textContent = i;
        box.onclick = () => box.classList.toggle('selected');
        grid.appendChild(box);
    }
}

function toggleAllChroms(select) {
    document.querySelectorAll('#chrom-grid .chrom-box').forEach(b => {
        b.classList.toggle('selected', select);
    });
}

function getSelectedChroms() {
    return Array.from(document.querySelectorAll('#chrom-grid .chrom-box.selected'))
        .map(b => b.dataset.chrom);
}

// --- File Browser ---
async function browsePath(path) {
    document.getElementById('browse-path').value = path;
    const list = document.getElementById('browser-list');
    list.innerHTML = '<div style="padding:12px;color:var(--text-dim)">Loading...</div>';

    try {
        const res = await fetch(`${BASE}/api/browse?path=${encodeURIComponent(path)}`);
        const data = await res.json();

        if (data.error) {
            list.innerHTML = `<div style="padding:12px;color:var(--red)">${data.error}</div>`;
            return;
        }

        list.innerHTML = '';

        // Parent directory link
        if (data.parent && data.parent !== data.path) {
            const item = document.createElement('div');
            item.className = 'browser-item';
            item.innerHTML = '<span class="icon">..</span><span class="name">..</span>';
            item.onclick = () => browsePath(data.parent);
            list.appendChild(item);
        }

        for (const entry of data.entries) {
            const item = document.createElement('div');
            item.className = 'browser-item';

            if (entry.is_dir) {
                item.innerHTML = `
                    <span class="icon">&#128193;</span>
                    <span class="name">${entry.name}</span>
                `;
                item.onclick = () => browsePath(entry.path);
            } else {
                const sizeStr = formatSize(entry.size);
                const idxExt = entry.name.endsWith('.cram') ? '.crai' : '.bai';
                const idxBadge = entry.has_index
                    ? `<span class="idx-badge">${idxExt}</span>`
                    : '<span class="no-idx-badge">no index</span>';

                // Check if already selected (multi mode)
                const isSelected = selectedBams.some(b => b.path === entry.path);
                if (isSelected) item.classList.add('browser-selected');

                item.innerHTML = `
                    <span class="icon">&#128196;</span>
                    <span class="name">${entry.name}</span>
                    <span class="size">${sizeStr}</span>
                    ${idxBadge}
                `;
                item.onclick = () => {
                    if (currentMode === 'deepvariant') {
                        toggleMultiSelect(entry, item);
                    } else {
                        selectFile(entry);
                    }
                };
            }
            list.appendChild(item);
        }

        if (data.entries.length === 0) {
            list.innerHTML = '<div style="padding:12px;color:var(--text-dim)">No BAM/CRAM files found</div>';
        }
    } catch (e) {
        list.innerHTML = `<div style="padding:12px;color:var(--red)">Error: ${e.message}</div>`;
    }
}

function refreshBrowserSelection() {
    document.querySelectorAll('.browser-item').forEach(item => {
        item.classList.remove('browser-selected');
    });
    if (currentMode === 'deepvariant') {
        // Re-highlight selected items
        document.querySelectorAll('.browser-item .name').forEach(nameEl => {
            const name = nameEl.textContent;
            if (selectedBams.some(b => b.path.endsWith('/' + name) || b.name === name)) {
                nameEl.closest('.browser-item').classList.add('browser-selected');
            }
        });
    }
}

// --- Single file selection (bcftools mode) ---
function selectFile(entry) {
    selectedBam = entry.path;
    const el = document.getElementById('selected-file');
    el.style.display = 'flex';
    document.getElementById('selected-files-list').style.display = 'none';
    document.getElementById('selected-file-name').textContent = entry.path;
    document.getElementById('selected-file-size').textContent = formatSize(entry.size);
    document.getElementById('selected-file-index').innerHTML = entry.has_index
        ? '<span class="idx-badge">Index found</span>'
        : '<span class="no-idx-badge">No index!</span>';
}

function clearSelection() {
    selectedBam = null;
    document.getElementById('selected-file').style.display = 'none';
}

// --- Multi-BAM selection (DeepVariant mode) ---
function toggleMultiSelect(entry, itemEl) {
    const idx = selectedBams.findIndex(b => b.path === entry.path);
    if (idx >= 0) {
        selectedBams.splice(idx, 1);
        itemEl.classList.remove('browser-selected');
    } else {
        selectedBams.push({ path: entry.path, name: entry.name, size: entry.size, has_index: entry.has_index });
        itemEl.classList.add('browser-selected');
    }
    updateMultiSelectUI();
    updateModeUI();  // refresh hint
}

function updateMultiSelectUI() {
    const container = document.getElementById('selected-files-list');
    const items = document.getElementById('selected-files-items');

    if (selectedBams.length === 0) {
        container.style.display = 'none';
        return;
    }

    container.style.display = 'block';
    document.getElementById('selected-file').style.display = 'none';
    document.getElementById('selected-count').textContent = `${selectedBams.length} file${selectedBams.length > 1 ? 's' : ''} selected`;

    items.innerHTML = selectedBams.map((b, i) => {
        const idxExt = b.name.endsWith('.cram') ? '.crai' : '.bai';
        return `
        <div class="selected-file-row">
            <span class="sf-num">${i + 1}</span>
            <span class="sf-name">${b.name}</span>
            <span class="sf-size">${formatSize(b.size)}</span>
            ${b.has_index ? `<span class="idx-badge">${idxExt}</span>` : '<span class="no-idx-badge">no idx</span>'}
            <button class="btn-small btn-remove" onclick="removeMultiSelect(${i})">&#10005;</button>
        </div>`;
    }).join('');
}

function removeMultiSelect(index) {
    selectedBams.splice(index, 1);
    updateMultiSelectUI();
    updateModeUI();
    refreshBrowserSelection();
}

function clearAllSelections() {
    selectedBams = [];
    selectedBam = null;
    document.getElementById('selected-files-list').style.display = 'none';
    document.getElementById('selected-file').style.display = 'none';
    refreshBrowserSelection();
    updateModeUI();
}

// --- Convert ---
async function startConvert() {
    const isDV = currentMode === 'deepvariant';

    // Validate selection
    if (isDV) {
        if (selectedBams.length === 0) {
            alert('Please select at least one alignment file');
            return;
        }
    } else {
        if (!selectedBam && selectedBams.length === 0) {
            alert('Please select an alignment file first');
            return;
        }
    }

    const chroms = getSelectedChroms();
    if (!isDV && chroms.length === 0) {
        alert('Please select at least one chromosome');
        return;
    }

    const btn = document.getElementById('btn-convert');
    btn.disabled = true;
    document.getElementById('btn-convert-text').textContent = 'Starting...';

    const params = {
        bam_path: isDV ? '' : (selectedBam || (selectedBams[0] && selectedBams[0].path) || ''),
        bam_paths: isDV ? selectedBams.map(b => b.path) : (selectedBam ? [selectedBam] : selectedBams.map(b => b.path)),
        output_dir: document.getElementById('output-dir').value,
        reference: document.getElementById('reference').value,
        cores: parseInt(document.getElementById('cores').value),
        min_base_qual: parseInt(document.getElementById('min-base-qual').value),
        min_map_qual: parseInt(document.getElementById('min-map-qual').value),
        max_depth: parseInt(document.getElementById('max-depth').value),
        qual_filter: parseInt(document.getElementById('qual-filter').value),
        min_dp: parseInt(document.getElementById('min-dp').value),
        max_dp: parseInt(document.getElementById('max-dp').value),
        chromosomes: chroms,
        mode: currentMode,
        dv_shards: isDV ? parseInt(document.getElementById('dv-shards').value) : 20,
        use_gpu: isDV ? document.getElementById('use-gpu').checked : false,
    };

    try {
        const res = await fetch(`${BASE}/api/convert`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(params),
        });
        const data = await res.json();

        if (data.error) {
            alert('Error: ' + data.error);
            btn.disabled = false;
            document.getElementById('btn-convert-text').textContent =
                isDV ? 'Run DeepVariant Pipeline' : 'Convert to VCF';
            return;
        }

        currentJobId = data.job_id;
        document.getElementById('btn-convert-text').textContent = 'Running...';
        showProgress(isDV ? [] : chroms, isDV);
        connectSSE(data.job_id);
    } catch (e) {
        alert('Error: ' + e.message);
        btn.disabled = false;
        document.getElementById('btn-convert-text').textContent =
            isDV ? 'Run DeepVariant Pipeline' : 'Convert to VCF';
    }
}

// --- Progress ---
function showProgress(chroms, isDV) {
    document.getElementById('progress-section').style.display = 'block';
    document.getElementById('result-section').style.display = 'none';
    document.getElementById('qc-section').style.display = 'none';

    // Toggle step indicators
    document.getElementById('steps-bcftools').style.display = isDV ? 'none' : 'flex';
    document.getElementById('steps-dv').style.display = isDV ? 'flex' : 'none';

    // Toggle stats
    document.getElementById('stat-chroms').style.display = isDV ? 'none' : '';
    document.getElementById('stat-samples').style.display = isDV ? '' : 'none';

    // Build chromosome progress grid (bcftools mode only)
    const grid = document.getElementById('chrom-progress-grid');
    grid.innerHTML = '';
    if (!isDV) {
        for (const c of chroms) {
            const box = document.createElement('div');
            box.className = 'chrom-prog-box';
            box.id = `cprog-${c}`;
            box.textContent = c;
            grid.appendChild(box);
        }
    }
}

function connectSSE(jobId) {
    if (eventSource) eventSource.close();

    eventSource = new EventSource(`${BASE}/api/jobs/${jobId}/stream`);

    eventSource.addEventListener('progress', (e) => {
        const data = JSON.parse(e.data);
        updateProgress(data);

        if (data.status === 'completed' || data.status === 'failed') {
            eventSource.close();
            eventSource = null;
            showResult(data);
            loadHistory();
        }
    });

    eventSource.onerror = () => {
        setTimeout(() => {
            if (eventSource && eventSource.readyState === EventSource.CLOSED) {
                eventSource = null;
            }
        }, 3000);
    };
}

function updateProgress(data) {
    const isDV = data.mode === 'deepvariant';

    // Progress bar
    const pct = Math.round(data.progress * 100);
    const bar = document.getElementById('progress-bar');
    bar.style.width = pct + '%';
    document.getElementById('progress-text').textContent = pct + '%';

    // Stats
    if (isDV) {
        document.getElementById('samples-progress').textContent =
            `${data.samples_done}/${data.total_samples}`;
    } else {
        document.getElementById('chroms-progress').textContent =
            `${data.chroms_done}/${data.total_chroms}`;
    }
    document.getElementById('elapsed-time').textContent = formatTime(data.elapsed);
    document.getElementById('eta-time').textContent =
        data.eta != null ? formatTime(data.eta) : '--';

    // Chromosome grid (bcftools)
    if (!isDV && data.chroms_completed) {
        for (const c of data.chroms_completed) {
            const box = document.getElementById(`cprog-${c}`);
            if (box) box.className = 'chrom-prog-box done';
        }
    }

    // Step indicator
    if (isDV) {
        updateDVSteps(data.step);
    } else {
        updateBcfSteps(data.step);
    }

    // Logs
    const logEl = document.getElementById('log-output');
    logEl.textContent = (data.logs || []).join('\n');
    logEl.scrollTop = logEl.scrollHeight;

    // QC results
    if (data.qc_result) {
        showQCResults(data.qc_result);
    }
}

function updateBcfSteps(step) {
    const steps = ['calling', 'concatenating', 'normalizing', 'filtering', 'qc', 'done'];
    const stepMap = {
        'validating': 'calling',
        'calling': 'calling',
        'concatenating': 'concatenating',
        'normalizing': 'normalizing',
        'filtering': 'filtering',
        'qc': 'qc',
        'cleanup': 'done',
        'done': 'done',
    };
    const currentStep = stepMap[step] || step;
    const currentIdx = steps.indexOf(currentStep);

    for (let i = 0; i < steps.length; i++) {
        const el = document.getElementById(`step-${steps[i]}`);
        if (!el) continue;
        el.className = 'step';
        if (i < currentIdx) el.classList.add('done');
        else if (i === currentIdx) el.classList.add('active');
    }
}

function updateDVSteps(step) {
    const steps = ['calling', 'joint', 'filter', 'qc', 'done'];
    const stepMap = {
        'validating': 'calling',
        'dv-calling': 'calling',
        'joint-genotyping': 'joint',
        'filtering': 'filter',
        'qc': 'qc',
        'cleanup': 'done',
        'done': 'done',
    };
    const currentStep = stepMap[step] || 'calling';
    const currentIdx = steps.indexOf(currentStep);

    for (let i = 0; i < steps.length; i++) {
        const el = document.getElementById(`dv-step-${steps[i]}`);
        if (!el) continue;
        el.className = 'step';
        if (i < currentIdx) el.classList.add('done');
        else if (i === currentIdx) el.classList.add('active');
    }
}

// --- QC Results Display ---
function showQCResults(qc) {
    const section = document.getElementById('qc-section');
    section.style.display = 'block';
    const grid = document.getElementById('qc-grid');

    grid.innerHTML = `
        <div class="qc-card ${qc.variant_count_pass ? 'qc-pass' : 'qc-fail'}">
            <div class="qc-status">${qc.variant_count_pass ? 'PASS' : 'FAIL'}</div>
            <div class="qc-label">Variant Count</div>
            <div class="qc-value">${Number(qc.variant_count).toLocaleString()}</div>
        </div>
        <div class="qc-card ${qc.titv_pass ? 'qc-pass' : 'qc-fail'}">
            <div class="qc-status">${qc.titv_pass ? 'PASS' : 'FAIL'}</div>
            <div class="qc-label">Ti/Tv Ratio</div>
            <div class="qc-value">${qc.titv_ratio}</div>
            <div class="qc-range">expect 1.9 - 2.2</div>
        </div>
        <div class="qc-card ${qc.sample_count_pass ? 'qc-pass' : 'qc-fail'}">
            <div class="qc-status">${qc.sample_count_pass ? 'PASS' : 'FAIL'}</div>
            <div class="qc-label">Sample Count</div>
            <div class="qc-value">${qc.sample_count}</div>
        </div>
        ${qc.per_sample_counts && Object.keys(qc.per_sample_counts).length > 0 ? `
        <div class="qc-card ${qc.per_sample_pass ? 'qc-pass' : 'qc-fail'}">
            <div class="qc-status">${qc.per_sample_pass ? 'PASS' : 'FAIL'}</div>
            <div class="qc-label">Per-Sample Counts</div>
            <div class="qc-per-sample">
                ${Object.entries(qc.per_sample_counts).map(([s, c]) =>
                    `<div>${s}: ${Number(c).toLocaleString()}</div>`
                ).join('')}
            </div>
        </div>` : ''}
        <div class="qc-card ${qc.het_hom_extracted ? 'qc-pass' : 'qc-fail'}">
            <div class="qc-status">${qc.het_hom_extracted ? 'PASS' : 'FAIL'}</div>
            <div class="qc-label">Het/Hom Stats</div>
            <div class="qc-value">${qc.het_hom_extracted ? 'Extracted' : 'Missing'}</div>
        </div>
    `;

    const summary = document.getElementById('qc-summary');
    const allPass = qc.overall_pass;
    summary.className = `qc-summary ${allPass ? 'qc-summary-pass' : 'qc-summary-fail'}`;
    summary.textContent = allPass
        ? `QC: ${qc.checks_passed}/${qc.checks_total} checks passed — ALL CLEAR`
        : `QC: ${qc.checks_passed}/${qc.checks_total} checks passed — REVIEW FAILURES`;
}

function showResult(data) {
    document.getElementById('result-section').style.display = 'block';
    const btn = document.getElementById('btn-convert');
    btn.disabled = false;
    document.getElementById('btn-convert-text').textContent =
        currentMode === 'deepvariant' ? 'Run DeepVariant Pipeline' : 'Convert to VCF';

    if (data.status === 'completed') {
        document.getElementById('result-success').style.display = 'block';
        document.getElementById('result-error').style.display = 'none';
        document.getElementById('download-link').href = `${BASE}/api/download/${currentJobId}`;
        document.getElementById('download-qc-link').href = `${BASE}/api/download/${currentJobId}/qc`;

        const details = document.getElementById('result-details');
        const modeLabel = data.mode === 'deepvariant' ? 'DeepVariant + GLnexus' : 'bcftools';
        let text = `Mode: ${modeLabel}\nOutput: ${data.output_vcf}\nElapsed: ${formatTime(data.elapsed)}`;
        if (data.qc_result) {
            text += `\nQC: ${data.qc_result.checks_passed}/${data.qc_result.checks_total} passed`;
            text += `\nVariants: ${Number(data.qc_result.variant_count).toLocaleString()}`;
            text += `\nTi/Tv: ${data.qc_result.titv_ratio}`;
        }
        details.textContent = text;
    } else {
        document.getElementById('result-success').style.display = 'none';
        document.getElementById('result-error').style.display = 'block';
        document.getElementById('error-details').textContent = data.error || 'Unknown error';
    }
}

// --- Job History ---
async function loadHistory() {
    try {
        const res = await fetch(`${BASE}/api/jobs`);
        const jobs = await res.json();
        const container = document.getElementById('job-history');

        if (jobs.length === 0) {
            container.innerHTML = '<div style="color:var(--text-dim);padding:8px 0">No jobs yet</div>';
            return;
        }

        container.innerHTML = jobs.map(j => {
            const time = j.started_at
                ? new Date(j.started_at * 1000).toLocaleString()
                : 'Pending';
            const pct = Math.round((j.progress || 0) * 100);
            const modeTag = j.mode === 'deepvariant'
                ? '<span class="mode-tag mode-tag-dv">DV</span>'
                : '<span class="mode-tag mode-tag-bcf">BCF</span>';
            const bamInfo = j.bam_count > 1
                ? `${j.bam_path} +${j.bam_count - 1}`
                : j.bam_path;
            return `
                <div class="history-item" onclick="viewJob('${j.job_id}')" style="cursor:pointer">
                    <div class="history-status ${j.status}"></div>
                    ${modeTag}
                    <div class="history-info">
                        <div class="history-file">${bamInfo} <span style="color:var(--text-dim)">(${j.job_id})</span></div>
                        <div class="history-meta">${time} &middot; ${j.status} &middot; ${pct}% &middot; ${j.phase_name || j.step || ''}</div>
                    </div>
                    <span class="history-arrow">&#8250;</span>
                </div>
            `;
        }).join('');
    } catch (e) {
        // ignore
    }
}

// --- Job Detail View ---
async function viewJob(jobId) {
    const overlay = document.getElementById('detail-overlay');
    const header = document.getElementById('detail-header');
    const body = document.getElementById('detail-body');

    header.innerHTML = '<h2 style="color:var(--text-dim)">Loading...</h2>';
    body.innerHTML = '';
    overlay.classList.add('visible');

    try {
        const res = await fetch(`${BASE}/api/jobs/${jobId}`);
        const j = await res.json();
        if (!j.job_id) { body.innerHTML = `<p style="color:var(--red)">${j.error || 'Job not found'}</p>`; return; }

        const startTime = j.started_at ? new Date(j.started_at * 1000).toLocaleString() : '--';
        const endTime = j.completed_at ? new Date(j.completed_at * 1000).toLocaleString() : '--';
        const elapsed = (j.started_at && j.completed_at)
            ? formatTime(j.completed_at - j.started_at)
            : (j.started_at ? formatTime(Date.now()/1000 - j.started_at) : '--');
        const modeLabel = j.mode === 'deepvariant' ? 'DeepVariant + GLnexus' : 'bcftools (Quick)';
        const statusClass = j.status === 'completed' ? 'detail-status-ok' :
                            j.status === 'failed' ? 'detail-status-fail' : 'detail-status-run';

        // Header
        header.innerHTML = `
            <div class="detail-title-row">
                <h2>Job ${j.job_id}</h2>
                <span class="detail-status ${statusClass}">${j.status.toUpperCase()}</span>
            </div>
        `;

        // Info grid
        let html = `<div class="detail-grid">
            <div class="dg-item"><span class="dg-label">Mode</span><span class="dg-val">${modeLabel}</span></div>
            <div class="dg-item"><span class="dg-label">Started</span><span class="dg-val">${startTime}</span></div>
            <div class="dg-item"><span class="dg-label">Finished</span><span class="dg-val">${endTime}</span></div>
            <div class="dg-item"><span class="dg-label">Elapsed</span><span class="dg-val">${elapsed}</span></div>
            <div class="dg-item"><span class="dg-label">Progress</span><span class="dg-val">${Math.round(j.progress * 100)}%</span></div>
            <div class="dg-item"><span class="dg-label">Phase</span><span class="dg-val">${j.phase_name || j.step || '--'}</span></div>
        </div>`;

        // BAM files
        const bams = j.bam_paths && j.bam_paths.length > 0 ? j.bam_paths : [j.bam_path];
        html += `<h3>Input Files</h3><div class="detail-bams">`;
        for (const b of bams) {
            html += `<div class="detail-bam-row"><code>${b}</code></div>`;
        }
        html += `</div>`;

        // Parameters
        html += `<h3>Parameters</h3><div class="detail-grid">
            <div class="dg-item"><span class="dg-label">Reference</span><span class="dg-val dg-mono">${j.reference}</span></div>
            <div class="dg-item"><span class="dg-label">Cores</span><span class="dg-val">${j.cores}</span></div>
            <div class="dg-item"><span class="dg-label">QUAL Filter</span><span class="dg-val">&ge;${j.qual_filter}</span></div>
            <div class="dg-item"><span class="dg-label">DP Range</span><span class="dg-val">${j.min_dp} - ${j.max_dp}</span></div>`;
        if (j.mode !== 'deepvariant') {
            html += `
            <div class="dg-item"><span class="dg-label">Base Qual</span><span class="dg-val">&ge;${j.min_base_qual}</span></div>
            <div class="dg-item"><span class="dg-label">Map Qual</span><span class="dg-val">&ge;${j.min_map_qual}</span></div>
            <div class="dg-item"><span class="dg-label">Max Depth</span><span class="dg-val">${j.max_depth}</span></div>
            <div class="dg-item"><span class="dg-label">Chromosomes</span><span class="dg-val">${(j.chromosomes||[]).length} selected</span></div>`;
        }
        html += `</div>`;

        // Output
        if (j.output_vcf) {
            html += `<h3>Output</h3><div class="detail-output">
                <code>${j.output_vcf}</code>
                <a class="btn-small" href="${BASE}/api/download/${j.job_id}" style="margin-left:8px;text-decoration:none">Download VCF</a>
                <a class="btn-small" href="${BASE}/api/download/${j.job_id}/qc" style="margin-left:4px;text-decoration:none">QC Report</a>
            </div>`;
        }

        // Error
        if (j.error) {
            html += `<h3>Error</h3><pre class="detail-error">${escapeHtml(j.error)}</pre>`;
        }

        // QC Results
        if (j.qc_result) {
            const qc = j.qc_result;
            html += `<h3>QC Results</h3><div class="detail-qc-grid">`;
            html += qcCard('Variant Count', Number(qc.variant_count).toLocaleString(), qc.variant_count_pass);
            html += qcCard('Ti/Tv Ratio', qc.titv_ratio, qc.titv_pass);
            html += qcCard('Sample Count', qc.sample_count, qc.sample_count_pass);
            if (qc.per_sample_counts && Object.keys(qc.per_sample_counts).length > 0) {
                const perSample = Object.entries(qc.per_sample_counts)
                    .map(([s, c]) => `${s}: ${Number(c).toLocaleString()}`).join('<br>');
                html += qcCard('Per-Sample', perSample, qc.per_sample_pass, true);
            }
            html += qcCard('Het/Hom', qc.het_hom_extracted ? 'Extracted' : 'Missing', qc.het_hom_extracted);
            html += `</div>`;
            const allPass = qc.overall_pass;
            html += `<div class="qc-summary ${allPass ? 'qc-summary-pass' : 'qc-summary-fail'}" style="margin-top:8px">
                QC: ${qc.checks_passed}/${qc.checks_total} checks passed ${allPass ? '- ALL CLEAR' : '- REVIEW FAILURES'}
            </div>`;
        }

        // Logs (collapsible)
        if (j.logs && j.logs.length > 0) {
            html += `<h3 class="detail-log-toggle" onclick="this.nextElementSibling.classList.toggle('collapsed')">
                Pipeline Log <span class="toggle-arrow">&#9660;</span>
            </h3>
            <pre class="detail-log">${escapeHtml(j.logs.join('\n'))}</pre>`;
        }

        // Action buttons
        html += `<div class="detail-actions" id="detail-actions-${j.job_id}">`;
        if (j.status === 'failed') {
            html += `<span class="detail-checking-hint" id="resume-check-${j.job_id}">Checking resumable state...</span>`;
        }
        if (j.status === 'failed' || j.status === 'completed') {
            html += `<button class="btn-rerun btn-rerun-secondary" onclick="rerunJob('${j.job_id}')">Rerun from Scratch</button>`;
        }
        if (j.status === 'running') {
            html += `<span class="detail-running-hint">Job is currently running...</span>`;
        }
        html += `</div>`;

        body.innerHTML = html;

        // Check if failed job is resumable
        if (j.status === 'failed') {
            checkResumable(j.job_id);
        }
    } catch (e) {
        body.innerHTML = `<p style="color:var(--red)">Failed to load job: ${e.message}</p>`;
    }
}

function qcCard(label, value, pass, isHtml) {
    const cls = pass ? 'qc-pass' : 'qc-fail';
    const status = pass ? 'PASS' : 'FAIL';
    const valHtml = isHtml ? value : escapeHtml(String(value));
    return `<div class="qc-card ${cls}">
        <div class="qc-status">${status}</div>
        <div class="qc-label">${label}</div>
        <div class="qc-value" style="font-size:0.9rem">${valHtml}</div>
    </div>`;
}

async function rerunJob(jobId) {
    try {
        const res = await fetch(`${BASE}/api/jobs/${jobId}`);
        const j = await res.json();
        if (j.error && !j.bam_path) { alert('Cannot load job data'); return; }

        // Close detail panel
        closeDetail();

        // Set mode
        setMode(j.mode || 'bcftools');

        // Populate BAMs
        const bams = j.bam_paths && j.bam_paths.length > 0 ? j.bam_paths : [j.bam_path];
        if (currentMode === 'deepvariant') {
            selectedBams = bams.map(b => ({
                path: b,
                name: b.split('/').pop(),
                size: null,
                has_index: true,
            }));
            selectedBam = null;
            updateMultiSelectUI();
        } else {
            selectedBam = bams[0];
            selectedBams = [];
            const el = document.getElementById('selected-file');
            el.style.display = 'flex';
            document.getElementById('selected-files-list').style.display = 'none';
            document.getElementById('selected-file-name').textContent = bams[0];
            document.getElementById('selected-file-size').textContent = '';
            document.getElementById('selected-file-index').innerHTML = '<span class="idx-badge">from rerun</span>';
        }

        // Populate parameters
        document.getElementById('cores').value = j.cores || 8;
        document.getElementById('cores-val').textContent = j.cores || 8;
        document.getElementById('min-base-qual').value = j.min_base_qual || 20;
        document.getElementById('min-map-qual').value = j.min_map_qual || 20;
        document.getElementById('max-depth').value = j.max_depth || 5000;
        document.getElementById('qual-filter').value = j.qual_filter || 30;
        document.getElementById('min-dp').value = j.min_dp || 10;
        document.getElementById('max-dp').value = j.max_dp || 1800;
        document.getElementById('reference').value = j.reference || '/data/genom-nimo/reference.fasta';
        document.getElementById('output-dir').value = (j.output_dir || '/scratch/nimog_output').replace(/\/[^/]+$/, '');

        // Set chromosomes
        if (j.chromosomes) {
            document.querySelectorAll('#chrom-grid .chrom-box').forEach(b => {
                b.classList.toggle('selected', j.chromosomes.includes(b.dataset.chrom));
            });
        }

        if (document.getElementById('dv-shards')) {
            document.getElementById('dv-shards').value = j.dv_shards || 20;
        }

        updateModeUI();

        // Scroll to top
        window.scrollTo({ top: 0, behavior: 'smooth' });
    } catch (e) {
        alert('Failed to load job for rerun: ' + e.message);
    }
}

async function checkResumable(jobId) {
    const el = document.getElementById(`resume-check-${jobId}`);
    if (!el) return;
    try {
        const res = await fetch(`${BASE}/api/jobs/${jobId}/resume-check`);
        const data = await res.json();
        if (data.resumable) {
            el.outerHTML = `<button class="btn-rerun" onclick="resumeJob('${jobId}')">
                Resume from ${escapeHtml(data.label)}
            </button>`;
        } else {
            el.outerHTML = `<span class="detail-no-resume">${escapeHtml(data.reason)}</span>`;
        }
    } catch (e) {
        el.textContent = 'Could not check resume state';
    }
}

async function resumeJob(jobId) {
    if (!confirm('Resume this job from where it left off?')) return;
    try {
        const res = await fetch(`${BASE}/api/jobs/${jobId}/resume`, { method: 'POST' });
        const data = await res.json();
        if (data.error) {
            alert('Cannot resume: ' + data.error);
            return;
        }
        // Close detail, show progress, connect SSE
        closeDetail();
        currentJobId = jobId;
        const isDV = false; // resume is currently bcftools only
        showProgress([], isDV);
        document.getElementById('btn-convert').disabled = true;
        document.getElementById('btn-convert-text').textContent = 'Resuming...';
        connectSSE(jobId);
    } catch (e) {
        alert('Resume failed: ' + e.message);
    }
}

function closeDetail(e) {
    if (e && e.target !== e.currentTarget) return;
    document.getElementById('detail-overlay').classList.remove('visible');
}

function escapeHtml(str) {
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
}

// --- Help Popups ---
const HELP_CONTENT = {
    'cores': {
        title: 'CPU Cores',
        body: `<p>Number of parallel tasks. In <strong>bcftools mode</strong>, each core processes one chromosome. In <strong>DeepVariant mode</strong>, cores are divided among shards.</p>
<div class="help-detail">
<strong>Low (1-4):</strong> Safer for shared servers<br>
<strong>Medium (8-12):</strong> Good balance<br>
<strong>High (20-44):</strong> Max speed on this 44-core server
</div>
<p>This server has <strong>44 vCPUs and 176 GB RAM</strong>.</p>`,
        video: 'https://www.youtube.com/embed/2dP3gKfRqOg?si=nK4FYr8ewJ3bI3UQ&start=45'
    },
    'base-qual': {
        title: 'Min Base Quality (-Q)',
        body: `<p>Minimum <strong>Phred-scaled base quality</strong> for pileup. Only used in bcftools mode.</p>
<div class="help-detail">
<strong>Phred 20</strong> = 99% accuracy<br>
<strong>Phred 30</strong> = 99.9% accuracy
</div>
<p><strong>Default 20</strong> is standard for Illumina WGS.</p>`,
        video: 'https://www.youtube.com/embed/brFMGSVNDCg?si=1'
    },
    'map-qual': {
        title: 'Min Mapping Quality (-q)',
        body: `<p>Minimum <strong>MAPQ</strong> for a read. Only used in bcftools mode.</p>
<div class="help-detail">
<strong>MAPQ 0:</strong> Multi-mapped read<br>
<strong>MAPQ 20:</strong> 1% chance wrong<br>
<strong>MAPQ 60:</strong> Extremely confident
</div>`,
        video: 'https://www.youtube.com/embed/brFMGSVNDCg?si=2'
    },
    'max-depth': {
        title: 'Max Read Depth (-d)',
        body: `<p>Maximum reads per position. Only used in bcftools mode. DeepVariant handles depth internally.</p>
<div class="help-detail">
<strong>Default 5000:</strong> Very permissive for 30x WGS<br>
<strong>250-1000:</strong> Faster but may miss high-depth regions
</div>`,
        video: 'https://www.youtube.com/embed/sn3p4gKZVgQ?si=1'
    },
    'qual-filter': {
        title: 'QUAL Filter Threshold',
        body: `<p>Minimum variant <strong>QUAL score</strong> in the final VCF. Applied after normalization.</p>
<div class="help-detail">
<strong>QUAL 20:</strong> Moderate filtering<br>
<strong>QUAL 30:</strong> Standard (default)<br>
<strong>QUAL 50+:</strong> Strict
</div>`,
        video: 'https://www.youtube.com/embed/If4rDqYZoIo?si=1'
    },
    'min-dp': {
        title: 'Min Depth Filter (DP)',
        body: `<p>Minimum <strong>total read depth (INFO/DP)</strong> for a variant to pass filtering. Variants at positions with very low coverage are unreliable.</p>
<div class="help-detail">
<strong>Default 10:</strong> Requires at least 10 reads supporting the call<br>
<strong>For 30x WGS:</strong> 10-20 is reasonable (some regions have lower coverage)<br>
<strong>For joint calling:</strong> Consider DP = 10 &times; N_samples (e.g., 60 for 6 samples)
</div>`,
    },
    'max-dp': {
        title: 'Max Depth Filter (DP)',
        body: `<p>Maximum <strong>total read depth (INFO/DP)</strong>. Extremely high depth usually indicates mapping artifacts (duplications, centromeres).</p>
<div class="help-detail">
<strong>Default 1800:</strong> Good for 6-sample joint calling at 30x (~300 per sample &times; 6)<br>
<strong>For single sample:</strong> 500-1000 is typical<br>
<strong>Rule of thumb:</strong> 3&times; expected total depth across all samples
</div>`,
    },
    'dv-shards': {
        title: 'DeepVariant Shards',
        body: `<p>Number of CPU shards for each DeepVariant run. DeepVariant divides the genome into this many pieces and processes them in parallel.</p>
<div class="help-detail">
<strong>Default 20:</strong> Good balance (uses ~20 cores per sample)<br>
<strong>With 2 parallel samples:</strong> 20 shards &times; 2 = 40 cores total<br>
<strong>Lower (8-12):</strong> Less memory, allows more parallel samples
</div>
<p>Total cores used = shards &times; concurrent samples. Don't exceed your CPU count.</p>`,
    },
    'reference': {
        title: 'Reference Genome',
        body: `<p>Path to the <strong>FASTA reference genome</strong>. Must match the BAM alignment.</p>
<div class="help-detail">
<strong>Required files:</strong> .fai index (samtools faidx) and .dict (Picard) alongside it.
</div>
<p>Default: GRCh38 reference used for all family BAMs.</p>`,
        video: 'https://www.youtube.com/embed/lGa1bLkATzk?si=1'
    },
    'output-dir': {
        title: 'Output Directory',
        body: `<p>Where pipeline writes output. A subdirectory with the job ID is created for each run.</p>
<div class="help-detail">
<strong>/scratch/:</strong> Fast NVMe SSD, best for pipeline I/O<br>
<strong>/data/:</strong> Persistent storage
</div>`,
    },
    'chromosomes': {
        title: 'Chromosomes',
        body: `<p>Autosomes (1-22) to process in bcftools mode. DeepVariant processes all chromosomes automatically.</p>
<div class="help-detail">
<strong>All 22:</strong> Full analysis<br>
<strong>Chr22 only:</strong> Fast testing (~3 min)
</div>`,
    }
};

// Attach click handlers to all help icons
document.addEventListener('click', (e) => {
    const icon = e.target.closest('.help-icon');
    if (!icon) return;
    e.preventDefault();
    e.stopPropagation();
    const key = icon.dataset.help;
    const content = HELP_CONTENT[key];
    if (!content) return;

    document.getElementById('help-title').textContent = content.title;
    document.getElementById('help-body').innerHTML = content.body;
    const videoEl = document.getElementById('help-video');
    if (content.video) {
        videoEl.innerHTML = `<iframe src="${content.video}" allowfullscreen loading="lazy"></iframe>`;
    } else {
        videoEl.innerHTML = '';
    }
    document.getElementById('help-overlay').classList.add('visible');
});

function closeHelp(e) {
    if (e && e.target !== e.currentTarget) return;
    const overlay = document.getElementById('help-overlay');
    overlay.classList.remove('visible');
    document.getElementById('help-video').innerHTML = '';
}

document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') { closeHelp(); closeDetail(); }
});

// --- Helpers ---
function formatSize(bytes) {
    if (bytes == null) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    return (bytes / (1024 * 1024 * 1024)).toFixed(1) + ' GB';
}

function formatTime(seconds) {
    if (seconds == null || isNaN(seconds)) return '--';
    seconds = Math.round(seconds);
    if (seconds < 60) return `0:${String(seconds).padStart(2, '0')}`;
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    if (m < 60) return `${m}:${String(s).padStart(2, '0')}`;
    const h = Math.floor(m / 60);
    const rm = m % 60;
    return `${h}h ${String(rm).padStart(2, '0')}m`;
}
