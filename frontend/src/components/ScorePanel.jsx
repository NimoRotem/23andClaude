import { useState, useEffect, useRef, useCallback } from 'react';
import { filesApi, runApi, connectRunProgress, ancestryApi } from '../api.js';
import { useAppState, useAppDispatch } from '../context.jsx';
import PGSSelector from './PGSSelector.jsx';

function formatElapsed(seconds) {
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

function fmtBytes(b) {
  if (!b) return '--';
  if (b > 1e9) return (b / 1e9).toFixed(1) + ' GB';
  if (b > 1e6) return (b / 1e6).toFixed(1) + ' MB';
  if (b > 1024) return (b / 1024).toFixed(1) + ' KB';
  return b + ' B';
}

function fileTypeBadge(type) {
  const map = {
    vcf: { label: 'VCF', cls: 'badge-green' },
    'vcf.gz': { label: 'VCF.GZ', cls: 'badge-green' },
    gvcf: { label: 'gVCF', cls: 'badge-blue' },
    'gvcf.gz': { label: 'gVCF.GZ', cls: 'badge-blue' },
    bam: { label: 'BAM', cls: 'badge-purple' },
    cram: { label: 'CRAM', cls: 'badge-purple' },
  };
  const t = (type || '').toLowerCase();
  return map[t] || { label: type || 'File', cls: 'badge-gray' };
}

function isDone(s) {
  return s === 'complete' || s === 'completed' || s === 'failed';
}

/* ---------- styles (inline, matching existing dark theme) ---------- */

const sectionStyle = {
  background: '#161b22',
  border: '1px solid #30363d',
  borderRadius: 8,
  padding: 20,
  marginBottom: 20,
};

const sectionTitleStyle = {
  fontSize: 15,
  fontWeight: 600,
  color: '#e6edf3',
  marginTop: 0,
  marginBottom: 14,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
};

const fileCardStyle = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  padding: '10px 14px',
  background: '#0d1117',
  border: '1px solid #30363d',
  borderRadius: 6,
  cursor: 'pointer',
  transition: 'border-color 0.15s',
  gap: 12,
};

const selectedFileCardStyle = {
  ...fileCardStyle,
  borderColor: '#58a6ff',
  background: 'rgba(88, 166, 255, 0.05)',
};

const engineDescriptions = {
  auto: 'Automatically selects the best engine based on input files and PGS requirements.',
  custom: 'Lightweight custom scoring calculator. Fast for single-sample VCFs with pre-downloaded scores.',
  pgsc_calc: 'Official pgsc_calc Nextflow pipeline. Supports ancestry adjustment via FRAPOSA and population normalization.',
};


export default function ScorePanel() {
  const { selectedSourceFiles, selectedPgsIds, scoringSettings, activeRunId } = useAppState();
  const dispatch = useAppDispatch();

  // ---- Per-file reference population ----
  const [filePopulations, setFilePopulations] = useState({});
  // key: file path, value: population string (e.g. "EUR", "AFR")
  // If not set for a file, uses the global scoringSettings.refPopulation

  // ---- Section A: Source Files ----
  const [scannedFiles, setScannedFiles] = useState([]);
  const [filesLoading, setFilesLoading] = useState(true);
  const [filesError, setFilesError] = useState(null);

  // ---- Ancestry context for file cards ----
  const [ancestryData, setAncestryData] = useState({});
  useEffect(() => {
    ancestryApi.all().then(data => {
      const map = {};
      for (const s of data) {
        map[s.sample_id] = s;
      }
      setAncestryData(map);
    }).catch(() => {});
  }, []);

  // ---- Section D: Estimate & Run ----
  const [estimate, setEstimate] = useState(null);
  const [estimating, setEstimating] = useState(false);
  const [starting, setStarting] = useState(false);
  const [toast, setToast] = useState(null);

  // ---- Progress state ----
  const [progress, setProgress] = useState(null);
  const [elapsed, setElapsed] = useState(0);
  const wsRef = useRef(null);
  const timerRef = useRef(null);

  // ---- Section E: Recent Runs ----
  const [recentRuns, setRecentRuns] = useState([]);

  const showToast = (msg, type = 'success') => {
    setToast({ msg, type });
    setTimeout(() => setToast(null), 4000);
  };

  // Load scanned files
  useEffect(() => {
    setFilesLoading(true);
    filesApi.scan()
      .then((data) => {
        const list = Array.isArray(data) ? data : (data?.files || []);
        // Exclude FASTQ — can't score PGS directly from raw reads
        setScannedFiles(list.filter(f => (f.file_type || f.type) !== 'fastq'));
        setFilesError(null);
      })
      .catch((err) => {
        setFilesError(err.message);
        setScannedFiles([]);
      })
      .finally(() => setFilesLoading(false));
  }, []);

  // Load recent runs
  useEffect(() => {
    runApi.list()
      .then((data) => {
        const all = Array.isArray(data) ? data : (data?.runs || []);
        setRecentRuns(all.slice(0, 5));
      })
      .catch(() => {});
  }, []);

  // Refresh recent runs helper
  function refreshRuns() {
    runApi.list()
      .then((data) => {
        const all = Array.isArray(data) ? data : (data?.runs || []);
        setRecentRuns(all.slice(0, 5));
      })
      .catch(() => {});
  }

  // WebSocket for run progress
  // Store run start time so elapsed is always correct (survives tab switches)
  const [runStartedAt, setRunStartedAt] = useState(null);

  const pollRunState = useCallback(async (runId) => {
    try {
      const run = await fetch(`/genomics/api/runs/${runId}`).then(r => r.json());
      if (run.started_at) {
        setRunStartedAt(new Date(run.started_at).getTime());
      }
      setProgress(prev => ({
        ...prev,
        status: run.status,
        pct: run.progress_pct || prev.pct || 0,
        step: run.current_step || run.status || prev.step,
        error: run.error_message,
        run_detail: run,
      }));
      return run;
    } catch { return null; }
  }, []);

  // Compute elapsed from runStartedAt — ticks every second via timer
  useEffect(() => {
    if (!runStartedAt || (progress && isDone(progress.status))) return;
    const tick = () => {
      setElapsed(Math.max(0, Math.round((Date.now() - runStartedAt) / 1000)));
    };
    tick(); // immediate
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [runStartedAt, progress?.status]);

  const connectWS = useCallback((runId) => {
    if (wsRef.current) wsRef.current.close();

    setProgress(prev => prev?.run_detail ? prev : {
      overall: 0, step: '', pgs_progress: [], status: 'running', run_detail: null, pct: 0,
    });

    // Poll actual state right away (handles page reload / tab switch)
    pollRunState(runId).then(run => {
      if (run && isDone(run.status)) {
        refreshRuns();
        return;
      }
    });

    const ws = connectRunProgress(
      runId,
      (data) => {
        setProgress(prev => ({ ...prev, ...data }));
        // Set started_at from WS if we haven't got it yet
        if (!runStartedAt && data.status === 'scoring') {
          setRunStartedAt(Date.now());
        }
        if (isDone(data.status)) {
          refreshRuns();
        }
      },
      () => {
        // WS closed — poll final state
        pollRunState(runId).then(() => refreshRuns());
      }
    );

    wsRef.current = ws;
  }, [pollRunState]);

  // Connect to active run on mount + poll fallback every 5s
  const pollRef = useRef(null);
  useEffect(() => {
    if (activeRunId) {
      connectWS(activeRunId);
      // Fallback poll every 5s in case WS drops or doesn't update
      pollRef.current = setInterval(() => {
        pollRunState(activeRunId).then(run => {
          if (run && isDone(run.status)) {
            clearInterval(pollRef.current);

            refreshRuns();
          }
        });
      }, 5000);
    }
    return () => {
      if (wsRef.current) wsRef.current.close();
      if (timerRef.current) clearInterval(timerRef.current);
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [activeRunId, connectWS, pollRunState]);

  // ---- Source file selection helpers ----
  const isFileSelected = (file) => selectedSourceFiles.some((f) => f.path === file.path);

  const toggleFile = (file) => {
    if (isFileSelected(file)) {
      dispatch({ type: 'REMOVE_SOURCE_FILE', payload: file.path });
    } else {
      dispatch({ type: 'ADD_SOURCE_FILE', payload: file });
    }
  };

  // ---- PGS handlers ----
  const handleAddPgs = (pgs) => {
    dispatch({ type: 'ADD_PGS', payload: pgs });
  };

  const handleRemovePgs = (pgsId) => {
    dispatch({ type: 'REMOVE_PGS', payload: pgsId });
  };

  const handleClearPgs = () => {
    dispatch({ type: 'CLEAR_PGS' });
  };

  // ---- Settings handlers ----
  const updateSetting = (key, value) => {
    dispatch({ type: 'UPDATE_SETTINGS', payload: { [key]: value } });
  };

  // ---- Estimate ----
  const handleEstimate = async () => {
    if (selectedSourceFiles.length === 0 || selectedPgsIds.length === 0) return;
    setEstimating(true);
    setEstimate(null);
    try {
      const sourcePayload = selectedSourceFiles.map((f) => {
        const ftype = f.file_type || f.type || 'vcf';
        const filePop = filePopulations[f.path];
        const base = ftype === 'bam' || ftype === 'cram'
          ? { path: f.path, type: ftype }
          : { vcf_id: f.vcf_id, path: f.path, type: ftype };
        // Only include ref_population if it differs from global default
        if (filePop && filePop !== scoringSettings.refPopulation) {
          base.ref_population = filePop;
        }
        return base;
      });
      const data = await runApi.estimate({
        source_files: sourcePayload,
        pgs_ids: selectedPgsIds.map((p) => p.id),
      });
      setEstimate(data);
    } catch (err) {
      console.error('Estimate error:', err);
      showToast('Estimate failed: ' + err.message, 'error');
    } finally {
      setEstimating(false);
    }
  };

  // ---- Run ----
  const canRun = selectedSourceFiles.length > 0 && selectedPgsIds.length > 0;

  const handleStartRun = async () => {
    if (!canRun) return;
    setStarting(true);
    try {
      const sourcePayload = selectedSourceFiles.map((f) => {
        const ftype = f.file_type || f.type || 'vcf';
        const filePop = filePopulations[f.path];
        const base = ftype === 'bam' || ftype === 'cram'
          ? { path: f.path, type: ftype }
          : { vcf_id: f.vcf_id, path: f.path, type: ftype };
        // Only include ref_population if it differs from global default
        if (filePop && filePop !== scoringSettings.refPopulation) {
          base.ref_population = filePop;
        }
        return base;
      });
      const data = await runApi.create({
        source_files: sourcePayload,
        pgs_ids: selectedPgsIds.map((p) => p.id),
        engine: scoringSettings.engine,
        ref_population: scoringSettings.refPopulation,
        freq_source: scoringSettings.freqSource,
      });
      const newRunId = data?.id || data?.run_id;
      if (newRunId) {
        dispatch({ type: 'SET_ACTIVE_RUN', payload: newRunId });
        setRunStartedAt(Date.now());
        setElapsed(0);
        connectWS(newRunId);
        showToast('Scoring run started');
      }
    } catch (err) {
      showToast('Failed to start: ' + err.message, 'error');
    } finally {
      setStarting(false);
    }
  };

  // ---------- RENDER ----------

  return (
    <div>
      <div className="section-header">
        <h2 className="section-title">Score</h2>
        <p style={{ color: '#8b949e', fontSize: 13, margin: 0 }}>
          Select source files, choose PGS scores, configure settings, and run scoring.
        </p>
      </div>

      {/* ============ Section A: Source Files ============ */}
      <div style={sectionStyle}>
        <h3 style={sectionTitleStyle}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M13 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V9z" />
            <polyline points="13 2 13 9 20 9" />
          </svg>
          Source Files
          {selectedSourceFiles.length > 0 && (
            <span className="badge badge-blue" style={{ marginLeft: 4 }}>
              {selectedSourceFiles.length} selected
            </span>
          )}
        </h3>

        {filesLoading && (
          <div style={{ textAlign: 'center', padding: '20px 0' }}>
            <div className="spinner" style={{ margin: '0 auto 8px' }} />
            <p style={{ color: '#8b949e', fontSize: 13 }}>Scanning files...</p>
          </div>
        )}

        {filesError && (
          <p style={{ color: '#f85149', fontSize: 13, padding: '8px 0' }}>
            Error scanning files: {filesError}
          </p>
        )}

        {!filesLoading && !filesError && scannedFiles.length === 0 && (
          <div style={{ textAlign: 'center', padding: '20px 0', color: '#8b949e', fontSize: 13 }}>
            No files found. Use the BAM-to-VCF tab or register a VCF first.
          </div>
        )}

        {!filesLoading && scannedFiles.length > 0 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <select
              style={{
                width: '100%', padding: '8px 10px', borderRadius: 6,
                border: '1px solid #30363d', background: '#0d1117', color: '#c9d1d9',
                fontSize: 13, cursor: 'pointer', outline: 'none',
              }}
              value=""
              onChange={(e) => {
                const file = scannedFiles.find(f => f.path === e.target.value);
                if (file) toggleFile(file);
              }}
            >
              <option value="" disabled>
                {selectedSourceFiles.length > 0
                  ? `Add another file (${scannedFiles.length - selectedSourceFiles.length} available)...`
                  : `Select source files (${scannedFiles.length} available)...`}
              </option>
              {scannedFiles.filter(f => !isFileSelected(f)).map((file) => {
                const badge = fileTypeBadge(file.type);
                const name = file.filename || file.path.split('/').pop();
                const sample = file.sample_name ? ` (${file.sample_name})` : '';
                const size = file.file_size_bytes ? ` - ${fmtBytes(file.file_size_bytes)}` : '';
                return (
                  <option key={file.path} value={file.path}>
                    [{badge.label}] {name}{sample}{size}
                  </option>
                );
              })}
            </select>
          </div>
        )}

        {/* Selected file cards */}
        {selectedSourceFiles.length > 0 && (
          <div style={{ marginTop: 14, paddingTop: 14, borderTop: '1px solid #21262d' }}>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {selectedSourceFiles.map((file) => {
                const badge = fileTypeBadge(file.type);
                const name = file.sample_name || file.filename || file.path.split('/').pop();
                return (
                  <span key={file.path} style={{
                    display: 'inline-flex', alignItems: 'center', gap: 5,
                    padding: '3px 8px', background: 'rgba(88,166,255,0.08)',
                    border: '1px solid rgba(88,166,255,0.2)', borderRadius: 12, fontSize: 12,
                  }}>
                    <span style={{ color: badge.cls.includes('green') ? '#3fb950' : badge.cls.includes('blue') ? '#58a6ff' : '#bc8cff', fontWeight: 600, fontSize: 10 }}>{badge.label}</span>
                    <span style={{ color: '#e6edf3' }}>{name}</span>
                    <span onClick={() => dispatch({ type: 'REMOVE_SOURCE_FILE', payload: file.path })}
                      style={{ cursor: 'pointer', color: '#f85149', fontSize: 14, lineHeight: 1 }}>&times;</span>
                  </span>
                );
              })}
            </div>
          </div>
        )}
      </div>

      {/* ============ Section B: PGS Selection ============ */}
      <div style={sectionStyle}>
        <h3 style={sectionTitleStyle}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="11" cy="11" r="8" />
            <path d="M21 21l-4.35-4.35" />
          </svg>
          PGS Selection
          {selectedPgsIds.length > 0 && (
            <span className="badge badge-blue" style={{ marginLeft: 4 }}>
              {selectedPgsIds.length} selected
            </span>
          )}
        </h3>
        <PGSSelector
          selectedPgsIds={selectedPgsIds}
          onAdd={handleAddPgs}
          onRemove={handleRemovePgs}
          onClear={handleClearPgs}
        />
      </div>

      {/* ============ Section C: Settings (compact row) ============ */}
      <div style={{ ...sectionStyle, padding: 14 }}>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'flex-end' }}>
          <div style={{ flex: '1 1 140px', minWidth: 120 }}>
            <label style={{ fontSize: 11, color: '#8b949e', display: 'block', marginBottom: 3 }}>Population</label>
            <select className="input" value={scoringSettings.refPopulation} onChange={(e) => updateSetting('refPopulation', e.target.value)}
              style={{ width: '100%', padding: '6px 8px', fontSize: 13, background: '#0d1117', border: '1px solid #30363d', color: '#c9d1d9', borderRadius: 6 }}>
              <option value="EUR">EUR</option>
              <option value="EAS">EAS</option>
              <option value="AFR">AFR</option>
              <option value="SAS">SAS</option>
              <option value="AMR">AMR</option>
              <option value="MULTI">Multi</option>
            </select>
          </div>
          <div style={{ flex: '1 1 160px', minWidth: 140 }}>
            <label style={{ fontSize: 11, color: '#8b949e', display: 'block', marginBottom: 3 }}>Freq Source</label>
            <select className="input" value={scoringSettings.freqSource} onChange={(e) => updateSetting('freqSource', e.target.value)}
              style={{ width: '100%', padding: '6px 8px', fontSize: 13, background: '#0d1117', border: '1px solid #30363d', color: '#c9d1d9', borderRadius: 6 }}>
              <option value="auto">Auto</option>
              <option value="pgs_file">PGS File</option>
              <option value="1kg_plink2">1000G (plink2)</option>
              <option value="vcf_af">VCF AF</option>
              <option value="fallback">Fallback</option>
            </select>
          </div>
          <div style={{ flex: '1 1 160px', minWidth: 140 }}>
            <label style={{ fontSize: 11, color: '#8b949e', display: 'block', marginBottom: 3 }}>Engine</label>
            <select className="input" value={scoringSettings.engine} onChange={(e) => updateSetting('engine', e.target.value)}
              style={{ width: '100%', padding: '6px 8px', fontSize: 13, background: '#0d1117', border: '1px solid #30363d', color: '#c9d1d9', borderRadius: 6 }}>
              <option value="auto">Auto (recommended)</option>
              <option value="custom">Custom (fast, local)</option>
              <option value="pgsc_calc">pgsc_calc (full pipeline)</option>
            </select>
          </div>
        </div>
      </div>

      {/* ============ Section D: Estimate & Run ============ */}
      <div style={sectionStyle}>
        <h3 style={sectionTitleStyle}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polygon points="5 3 19 12 5 21 5 3" />
          </svg>
          Estimate & Run
        </h3>

        {/* Summary — show run info if active, otherwise show selections */}
        {progress && progress.run_detail ? (
          <div style={{
            display: 'flex', gap: 24, fontSize: 13, color: '#c9d1d9', marginBottom: 16, flexWrap: 'wrap',
          }}>
            <span>
              Source Files: <strong style={{ color: '#3fb950' }}>
                {progress.run_detail.source_files?.length || '?'}
              </strong>
            </span>
            <span>
              PGS Scores: <strong style={{ color: '#3fb950' }}>
                {progress.run_detail.pgs_ids?.length || '?'}
              </strong>
            </span>
          </div>
        ) : (
          <div style={{
            display: 'flex', gap: 24, fontSize: 13, color: '#c9d1d9', marginBottom: 16, flexWrap: 'wrap',
          }}>
            <span>
              Source Files: <strong style={{ color: selectedSourceFiles.length > 0 ? '#3fb950' : '#f85149' }}>
                {selectedSourceFiles.length}
              </strong>
            </span>
            <span>
              PGS Scores: <strong style={{ color: selectedPgsIds.length > 0 ? '#3fb950' : '#f85149' }}>
                {selectedPgsIds.length}
              </strong>
            </span>
          <span>Engine: <strong>{scoringSettings.engine}</strong></span>
          <span>Population: <strong>{scoringSettings.refPopulation}</strong></span>
        </div>
        )}

        {/* Action buttons — hide when a run is actively in progress */}
        {!(progress && !isDone(progress.status)) && (
          <>
            <div style={{ display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap' }}>
              <button className="btn" disabled={!canRun || estimating} onClick={handleEstimate}>
                {estimating ? 'Estimating...' : 'Estimate'}
              </button>
              <button className="btn btn-primary" disabled={!canRun || starting} onClick={handleStartRun}>
                {starting ? 'Starting...' : 'Start Scoring Run'}
              </button>
            </div>
            {!canRun && (
              <div style={{ fontSize: 12, color: '#d29922', marginBottom: 12 }}>
                Select at least 1 source file and 1 PGS score to enable scoring.
              </div>
            )}
          </>
        )}

        {/* Estimate results */}
        {estimate && (
          <div style={{
            background: '#0d1117',
            border: '1px solid #30363d',
            borderRadius: 6,
            padding: 14,
            marginBottom: 16,
            fontSize: 13,
          }}>
            <div style={{ fontWeight: 600, color: '#e6edf3', marginBottom: 8 }}>Estimate</div>
            {(estimate.estimated_display || estimate.estimated_time) && (
              <div style={{ color: '#c9d1d9', marginBottom: 6 }}>
                Estimated Time: <strong style={{ color: '#58a6ff', fontSize: 16 }}>{estimate.estimated_display || estimate.estimated_time}</strong>
                {estimate.estimated_seconds && <span style={{ color: '#8b949e', marginLeft: 8 }}>({Math.round(estimate.estimated_seconds)}s)</span>}
              </div>
            )}
            {estimate.breakdown && Array.isArray(estimate.breakdown) && (
              <div style={{ marginBottom: 6 }}>
                {estimate.breakdown.map((item, idx) => {
                  const name = item.source_file?.path?.split('/').pop() || item.file || item.name || 'Unknown';
                  const ftype = item.file_type || item.source_file?.type || '?';
                  const time = item.subtotal_sec ? `${Math.round(item.subtotal_sec)}s` : (item.time || '--');
                  return (
                    <div key={idx} style={{ color: '#8b949e', marginLeft: 12, fontSize: 12 }}>
                      {name} ({ftype}): {time} — {(item.total_variants||0).toLocaleString()} variants
                    </div>
                  );
                })}
              </div>
            )}
            {estimate.warnings && estimate.warnings.length > 0 && (
              <div style={{ marginTop: 8 }}>
                {estimate.warnings.map((w, idx) => (
                  <div key={idx} style={{ color: '#d29922', fontSize: 12 }}>
                    Warning: {w}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Progress section */}
        {progress && (
          <div style={{
            background: '#0d1117',
            border: '1px solid #30363d',
            borderRadius: 8,
            padding: 16,
            marginTop: 4,
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
              <span style={{ fontSize: 15, fontWeight: 600, color: '#e6edf3' }}>
                {isDone(progress.status)
                  ? (progress.status === 'failed' ? 'Run Failed' : 'Run Completed')
                  : 'Scoring in Progress...'}
              </span>
              <span style={{ fontSize: 13, color: '#8b949e', fontVariantNumeric: 'tabular-nums' }}>
                {formatElapsed(elapsed)}
              </span>
            </div>

            {/* Show run info from the progress data or fetched run detail */}
            {progress.run_detail && (
              <div style={{ fontSize: 12, color: '#8b949e', marginBottom: 10, display: 'flex', gap: 16, flexWrap: 'wrap' }}>
                <span>PGS: <strong style={{ color: '#c9d1d9' }}>{(progress.run_detail.pgs_ids || []).join(', ')}</strong></span>
                <span>Files: <strong style={{ color: '#c9d1d9' }}>{(progress.run_detail.source_files || []).map(f => f.filename || f.path?.split('/').pop()).join(', ')}</strong></span>
              </div>
            )}
            <div style={{ fontSize: 13, color: '#c9d1d9', marginBottom: 10 }}>
              Status: <strong>{progress.status || 'running'}</strong>
              {progress.step_name && (
                <> &middot; Step: <strong>{progress.step_name}</strong></>
              )}
              {progress.step && !progress.step_name && (
                <> &middot; Step: <strong>{progress.step}</strong></>
              )}
            </div>

            {/* Overall progress bar */}
            <div className="progress-bar-track progress-blue" style={{ height: 16, marginBottom: 6 }}>
              <div
                className="progress-bar-fill"
                style={{ width: `${progress.pct ?? progress.overall ?? 0}%` }}
              />
            </div>
            <div style={{ fontSize: 12, color: '#c9d1d9', textAlign: 'right', marginBottom: 12 }}>
              {Math.round(progress.pct ?? progress.overall ?? 0)}%
            </div>

            {/* Per-PGS sub-progress */}
            {progress.pgs_progress && progress.pgs_progress.length > 0 && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {progress.pgs_progress.map((pg) => (
                  <div key={pg.id || pg.pgs_id} style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 12 }}>
                    <span style={{ color: '#c9d1d9', minWidth: 120, flexShrink: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {pg.id || pg.pgs_id}
                    </span>
                    <div style={{ flex: 1 }}>
                      <div className="progress-bar-track progress-green" style={{ height: 8 }}>
                        <div className="progress-bar-fill" style={{ width: `${pg.progress ?? 0}%` }} />
                      </div>
                    </div>
                    <span style={{ color: '#8b949e', minWidth: 36, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                      {Math.round(pg.progress ?? 0)}%
                    </span>
                  </div>
                ))}
              </div>
            )}

            {/* Completed -> go to results */}
            {isDone(progress.status) && progress.status !== 'failed' && (
              <div style={{ marginTop: 16, textAlign: 'center' }}>
                <button
                  className="btn btn-accent"
                  onClick={() => dispatch({ type: 'GO_TO_RUN', payload: activeRunId })}
                >
                  View Results &rarr;
                </button>
              </div>
            )}

            {/* Error display */}
            {progress.status === 'failed' && (progress.error || progress.error_message) && (
              <div style={{
                marginTop: 12, padding: 12,
                background: 'rgba(248,81,73,0.1)', borderRadius: 6,
                color: '#f85149', fontSize: 13,
              }}>
                Error: {progress.error || progress.error_message}
              </div>
            )}
          </div>
        )}
      </div>

      {/* ============ Section E: Recent Runs ============ */}
      {recentRuns.length > 0 && (
        <div style={sectionStyle}>
          <h3 style={sectionTitleStyle}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="10" />
              <polyline points="12 6 12 12 16 14" />
            </svg>
            Recent Runs
          </h3>
          <div className="pgs-run-list">
            {recentRuns.map((run) => (
              <div key={run.id} className="pgs-run-item" style={{ cursor: 'pointer' }}
                onClick={() => {
                  if (run.status === 'completed' || run.status === 'complete') {
                    dispatch({ type: 'GO_TO_RUN', payload: run.id });
                  } else {
                    dispatch({ type: 'SET_ACTIVE_RUN', payload: run.id });
                  }
                }}
              >
                <div className="pgs-run-item-info">
                  <span className="pgs-run-item-name">Run #{run.id}</span>
                  <span className="pgs-run-item-detail">
                    {' '}{run.created_at || run.started_at || run.date || ''}
                    {run.pgs_count != null && ` | ${run.pgs_count} PGS`}
                    {run.pgs_ids && !run.pgs_count && ` | ${run.pgs_ids.length} PGS`}
                    {run.engine && ` | ${run.engine}`}
                  </span>
                </div>
                <span className={`badge badge-${
                  run.status === 'completed' || run.status === 'complete' ? 'green' :
                  run.status === 'failed' ? 'red' :
                  run.status === 'running' ? 'blue' : 'gray'
                }`}>
                  {run.status || 'unknown'}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Toast */}
      {toast && (
        <div className={`toast ${toast.type === 'error' ? 'toast-error' : 'toast-success'}`}>
          {toast.msg}
        </div>
      )}
    </div>
  );
}
