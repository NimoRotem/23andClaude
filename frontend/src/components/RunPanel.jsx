import { useState, useEffect, useRef, useCallback } from 'react';
import { vcfApi, runApi, connectRunProgress } from '../api.js';
import { useAppState, useAppDispatch } from '../context.jsx';

function formatElapsed(seconds) {
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

export default function RunPanel() {
  const { selectedVcfId, selectedPgsIds, activeRunId } = useAppState();
  const dispatch = useAppDispatch();

  const [vcfs, setVcfs] = useState([]);
  const [vcfId, setVcfId] = useState(selectedVcfId || '');
  const [engine, setEngine] = useState('pgsc_calc');
  const [refPop, setRefPop] = useState('EUR');
  const [freqSource, setFreqSource] = useState('auto');
  const [runs, setRuns] = useState([]);
  const [starting, setStarting] = useState(false);
  const [toast, setToast] = useState(null);

  // Progress state
  const [progress, setProgress] = useState(null);
  const [elapsed, setElapsed] = useState(0);
  const wsRef = useRef(null);
  const timerRef = useRef(null);

  const showToast = (msg, type = 'success') => {
    setToast({ msg, type });
    setTimeout(() => setToast(null), 3000);
  };

  // Load VCFs and runs
  useEffect(() => {
    fetch('/api/vcfs/').then(r => r.json()).then(data => {
      setVcfs(Array.isArray(data) ? data : []);
    }).catch(() => {});

    fetch('/api/runs/').then(r => r.json()).then(data => {
      setRuns(Array.isArray(data) ? data : (data?.runs || []));
    }).catch(() => {});
  }, []);

  // Sync selectedVcfId from context
  useEffect(() => {
    if (selectedVcfId) setVcfId(selectedVcfId);
  }, [selectedVcfId]);

  // Connect WS when activeRunId is set
  function isDone(s) { return s === 'complete' || s === 'completed' || s === 'failed'; }

  const connectWS = useCallback((runId) => {
    if (wsRef.current) wsRef.current.close();

    setProgress({
      overall: 0,
      step: 0,
      total_steps: 5,
      step_name: 'Initializing...',
      pgs_progress: [],
      status: 'running',
    });
    setElapsed(0);

    if (timerRef.current) clearInterval(timerRef.current);
    timerRef.current = setInterval(() => {
      setElapsed((e) => e + 1);
    }, 1000);

    const ws = connectRunProgress(
      runId,
      (data) => {
        setProgress((prev) => ({ ...prev, ...data }));
        if (isDone(data.status)) {
          if (timerRef.current) clearInterval(timerRef.current);
          refreshRuns();
        }
      },
      () => {
        // WS closed — poll the run status once to get final state
        if (timerRef.current) clearInterval(timerRef.current);
        fetch(`/api/runs/${runId}`).then(r => r.json()).then(run => {
          setProgress(prev => ({
            ...prev,
            status: run.status,
            pct: run.progress_pct || 100,
            step: run.current_step || run.status,
            error: run.error_message,
          }));
          refreshRuns();
        }).catch(() => {});
      }
    );

    wsRef.current = ws;
  }, []);

  function refreshRuns() {
    fetch('/api/runs/').then(r => r.json()).then(data => {
      setRuns(Array.isArray(data) ? data : (data?.runs || []));
    }).catch(() => {});
  }

  // Connect to active run on mount if one exists
  useEffect(() => {
    if (activeRunId) {
      connectWS(activeRunId);
    }
    return () => {
      if (wsRef.current) wsRef.current.close();
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [activeRunId, connectWS]);

  const handleRemovePgs = (id) => {
    dispatch({ type: 'REMOVE_PGS', payload: id });
  };

  const handleStartRun = async () => {
    if (!vcfId || selectedPgsIds.length === 0) return;
    setStarting(true);
    try {
      const data = await runApi.create({
        vcf_id: vcfId,
        pgs_ids: selectedPgsIds.map((p) => p.id),
        engine,
        ref_population: refPop,
        freq_source: freqSource,
      });
      const newRunId = data?.id || data?.run_id;
      if (newRunId) {
        dispatch({ type: 'SET_ACTIVE_RUN', payload: newRunId });
        connectWS(newRunId);
        showToast('Scoring run started');
      }
    } catch (err) {
      showToast(`Failed to start: ${err.message}`, 'error');
    } finally {
      setStarting(false);
    }
  };

  const selectedVcf = vcfs.find((v) => v.id === vcfId || String(v.id) === String(vcfId));
  const stepNames = ['Prepare', 'Validate', 'Match', 'Score', 'Report'];

  return (
    <div>
      <div className="section-header">
        <h2 className="section-title">Run Scoring</h2>
      </div>

      {/* Configuration */}
      <div className="card" style={{ marginBottom: 20 }}>
        <div className="run-config">
          {/* VCF Selector */}
          <div className="form-group">
            <label>VCF File</label>
            <select
              className="input"
              value={vcfId}
              onChange={(e) => {
                setVcfId(e.target.value);
                dispatch({ type: 'SELECT_VCF', payload: e.target.value });
              }}
            >
              <option value="">Select a VCF file...</option>
              {vcfs.map((v) => (
                <option key={v.id} value={v.id}>
                  {v.filename || v.file_path} {v.genome_build ? `[${v.genome_build}]` : ''}
                </option>
              ))}
            </select>
            {selectedVcf?.genome_build && (
              <div style={{ marginTop: 6 }}>
                <span className="badge badge-purple">{selectedVcf.genome_build}</span>
              </div>
            )}
          </div>

          {/* Engine Selector */}
          <div className="form-group">
            <label>Scoring Engine</label>
            <div className="engine-options">
              <div
                className={`engine-option ${engine === 'pgsc_calc' ? 'selected' : ''}`}
                onClick={() => setEngine('pgsc_calc')}
              >
                <div className="engine-option-name">pgsc_calc</div>
                <div className="engine-option-desc">Recommended</div>
              </div>
              <div
                className={`engine-option ${engine === 'custom' ? 'selected' : ''}`}
                onClick={() => setEngine('custom')}
              >
                <div className="engine-option-name">Custom</div>
                <div className="engine-option-desc">Calculator</div>
              </div>
            </div>
          </div>

          {/* Reference Population */}
          <div className="form-group">
            <label>Reference Population</label>
            <select className="input" value={refPop} onChange={e => setRefPop(e.target.value)}>
              <option value="EUR">European (EUR)</option>
              <option value="EAS">East Asian (EAS)</option>
              <option value="AFR">African (AFR)</option>
              <option value="SAS">South Asian (SAS)</option>
              <option value="AMR">American (AMR)</option>
              <option value="MULTI">Multi-ancestry</option>
            </select>
            <div style={{ fontSize: 11, color: '#8b949e', marginTop: 4 }}>
              Used for Z-score normalization and ancestry context in results
            </div>
          </div>

          {/* Frequency Source */}
          <div className="form-group">
            <label>Allele Frequency Source</label>
            <select className="input" value={freqSource} onChange={e => setFreqSource(e.target.value)}>
              <option value="auto">Auto (best available)</option>
              <option value="pgs_file">PGS File Frequencies</option>
              <option value="1kg_plink2">1000 Genomes (3,202 samples via plink2)</option>
              <option value="vcf_af">VCF Allele Frequencies</option>
              <option value="fallback">Estimated (no reference)</option>
            </select>
            <div style={{ fontSize: 11, color: '#8b949e', marginTop: 4 }}>
              Auto tries: PGS file &rarr; 1000 Genomes (plink2) &rarr; VCF AF &rarr; fallback
            </div>
          </div>

          {/* Selected PGS */}
          <div className="form-group run-config-full">
            <label>Selected PGS Scores ({selectedPgsIds.length})</label>
            {selectedPgsIds.length === 0 ? (
              <p className="text-muted text-sm" style={{ marginTop: 6 }}>
                No PGS scores selected. Go to the PGS Search tab to add scores.
              </p>
            ) : (
              <div className="pgs-run-list">
                {selectedPgsIds.map((pgs) => {
                  const buildMatch = selectedVcf?.genome_build
                    ? pgs.available_builds?.includes(selectedVcf.genome_build)
                    : null;
                  return (
                    <div key={pgs.id} className="pgs-run-item">
                      <div className="pgs-run-item-info">
                        <span className="pgs-run-item-name">{pgs.id}</span>
                        {' '}
                        <span className="pgs-run-item-detail">
                          {pgs.trait_name || pgs.trait || ''}
                          {pgs.variant_count != null && ` | ${Number(pgs.variant_count).toLocaleString()} variants`}
                        </span>
                      </div>
                      {buildMatch != null && (
                        <span className={`badge ${buildMatch ? 'badge-green' : 'badge-yellow'}`}>
                          {buildMatch ? 'Build OK' : 'Liftover needed'}
                        </span>
                      )}
                      <button className="btn btn-sm btn-danger" onClick={() => handleRemovePgs(pgs.id)}>
                        Remove
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
          <button
            className="btn btn-primary"
            disabled={!vcfId || selectedPgsIds.length === 0 || starting}
            onClick={handleStartRun}
          >
            {starting ? 'Starting...' : 'Start Scoring Run'}
          </button>
        </div>
      </div>

      {/* Progress Section */}
      {progress && (
        <div className="progress-section">
          <div className="progress-header">
            <span className="progress-title">
              {isDone(progress.status)
                ? (progress.status === 'failed' ? 'Run Failed' : 'Run Completed')
                : 'Scoring in Progress...'}
            </span>
            <span className="progress-elapsed">{formatElapsed(elapsed)}</span>
          </div>

          <div style={{ fontSize: 14, color: '#c9d1d9', marginBottom: 12 }}>
            Status: <strong>{progress.status || 'running'}</strong>
            {progress.step && <> &middot; Step: <strong>{progress.step}</strong></>}
          </div>

          {/* Overall progress bar */}
          <div className="progress-bar-track progress-blue" style={{ height: 16, marginBottom: 8 }}>
            <div
              className="progress-bar-fill"
              style={{ width: `${progress.pct ?? progress.overall ?? 0}%` }}
            />
          </div>
          <div style={{ fontSize: 13, color: '#c9d1d9', marginBottom: 16, textAlign: 'right' }}>
            {Math.round(progress.pct ?? progress.overall ?? 0)}%
          </div>

          {/* Per-PGS sub-progress */}
          {progress.pgs_progress && progress.pgs_progress.length > 0 && (
            <div className="sub-progress-list">
              {progress.pgs_progress.map((pg) => (
                <div key={pg.id || pg.pgs_id} className="sub-progress-row">
                  <span className="sub-progress-label">{pg.id || pg.pgs_id}</span>
                  <div className="sub-progress-bar">
                    <div className="progress-bar-track progress-green">
                      <div
                        className="progress-bar-fill"
                        style={{ width: `${pg.progress ?? 0}%` }}
                      />
                    </div>
                  </div>
                  <span className="sub-progress-pct">{Math.round(pg.progress ?? 0)}%</span>
                </div>
              ))}
            </div>
          )}

          {isDone(progress.status) && progress.status !== 'failed' && (
            <div style={{ marginTop: 16, textAlign: 'center' }}>
              <button
                className="btn btn-accent"
                onClick={() => dispatch({ type: 'SET_TAB', payload: 3 })}
              >
                View Results &rarr;
              </button>
            </div>
          )}

          {progress.status === 'failed' && (progress.error || progress.error_message) && (
            <div style={{ marginTop: 12, padding: 12, background: 'rgba(248,81,73,0.1)', borderRadius: 6, color: '#f85149', fontSize: 13 }}>
              Error: {progress.error || progress.error_message}
            </div>
          )}
        </div>
      )}

      {/* Recent runs list */}
      {runs.length > 0 && (
        <div className="mt-lg">
          <h3 className="section-title" style={{ fontSize: 16, marginBottom: 12 }}>Recent Runs</h3>
          <div className="pgs-run-list">
            {runs.slice(0, 10).map((run) => (
              <div key={run.id} className="pgs-run-item">
                <div className="pgs-run-item-info">
                  <span className="pgs-run-item-name">Run #{run.id}</span>
                  <span className="pgs-run-item-detail">
                    {' '}{run.created_at || run.date || ''}
                    {run.pgs_count != null && ` | ${run.pgs_count} PGS`}
                    {run.engine && ` | ${run.engine}`}
                  </span>
                </div>
                <span className={`badge badge-${run.status === 'completed' ? 'green' : run.status === 'failed' ? 'red' : run.status === 'running' ? 'blue' : 'gray'}`}>
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
