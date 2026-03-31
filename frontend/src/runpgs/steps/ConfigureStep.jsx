import { useState } from 'react';
import { runApi } from '../../api.js';
import { useRunPGS, useRunPGSDispatch } from '../RunPGSState.jsx';
import { fileTypeBadgeInfo, fmtBytes } from '../utils.js';

const POPULATIONS = ['EUR', 'EAS', 'AFR', 'SAS', 'AMR', 'MULTI'];

export default function ConfigureStep() {
  const { selectedFiles, selectedPgsIds, settings, filePopulations, estimate } = useRunPGS();
  const dispatch = useRunPGSDispatch();
  const [estimating, setEstimating] = useState(false);
  const [starting, setStarting] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);

  const showToast = (msg, type = 'success') =>
    dispatch({ type: 'SHOW_TOAST', payload: { msg, type } });

  function buildSourcePayload() {
    return selectedFiles.map(f => {
      const ftype = f.file_type || f.type || 'vcf';
      const filePop = filePopulations[f.path];
      const base = ftype === 'bam' || ftype === 'cram'
        ? { path: f.path, type: ftype }
        : { vcf_id: f.vcf_id, path: f.path, type: ftype };
      if (filePop && filePop !== settings.refPopulation) base.ref_population = filePop;
      return base;
    });
  }

  async function handleEstimate() {
    if (selectedFiles.length === 0 || selectedPgsIds.length === 0) return;
    setEstimating(true);
    dispatch({ type: 'SET_ESTIMATE', payload: null });
    try {
      const data = await runApi.estimate({
        source_files: buildSourcePayload(),
        pgs_ids: selectedPgsIds.map(p => p.id),
      });
      dispatch({ type: 'SET_ESTIMATE', payload: data });
    } catch (err) {
      showToast('Estimate failed: ' + err.message, 'error');
    } finally {
      setEstimating(false);
    }
  }

  async function handleStartRun() {
    setStarting(true);
    try {
      const data = await runApi.create({
        source_files: buildSourcePayload(),
        pgs_ids: selectedPgsIds.map(p => p.id),
        engine: settings.engine,
        ref_population: settings.refPopulation,
        freq_source: settings.freqSource,
      });
      const newRunId = data?.id || data?.run_id;
      if (newRunId) {
        dispatch({ type: 'SET_RUN_STARTED_AT', payload: Date.now() });
        dispatch({ type: 'SET_ACTIVE_RUN', payload: newRunId });
        showToast('Scoring run started');
      }
    } catch (err) {
      showToast('Failed to start: ' + err.message, 'error');
    } finally {
      setStarting(false);
    }
  }

  return (
    <div>
      {/* Summary */}
      <div className="rpgs-section">
        <h3 className="rpgs-section-title">Run Summary</h3>
        <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap', fontSize: 14, color: '#c9d1d9' }}>
          <div>
            <span style={{ color: '#8b949e' }}>Files: </span>
            <strong style={{ color: '#3fb950' }}>{selectedFiles.length}</strong>
          </div>
          <div>
            <span style={{ color: '#8b949e' }}>PGS Scores: </span>
            <strong style={{ color: '#3fb950' }}>{selectedPgsIds.length}</strong>
          </div>
          <div>
            <span style={{ color: '#8b949e' }}>Combinations: </span>
            <strong>{selectedFiles.length * selectedPgsIds.length}</strong>
          </div>
        </div>

        {/* Selected files chips */}
        <div style={{ marginTop: 12, display: 'flex', flexWrap: 'wrap', gap: 6 }}>
          {selectedFiles.map(f => {
            const badge = fileTypeBadgeInfo(f.file_type || f.type);
            const name = f.sample_name || f.filename || f.path.split('/').pop();
            return (
              <span key={f.path} style={{
                display: 'inline-flex', alignItems: 'center', gap: 5,
                padding: '3px 8px', background: 'rgba(88,166,255,0.06)',
                border: '1px solid rgba(88,166,255,0.15)', borderRadius: 12, fontSize: 12,
              }}>
                <span style={{ color: badge.color, fontWeight: 600, fontSize: 10 }}>{badge.label}</span>
                <span style={{ color: '#e6edf3' }}>{name}</span>
              </span>
            );
          })}
        </div>

        {/* Selected PGS chips */}
        <div style={{ marginTop: 8, display: 'flex', flexWrap: 'wrap', gap: 6 }}>
          {selectedPgsIds.map(p => (
            <span key={p.id} style={{
              padding: '3px 8px', background: 'rgba(63,185,80,0.06)',
              border: '1px solid rgba(63,185,80,0.15)', borderRadius: 12, fontSize: 12, color: '#3fb950',
            }}>
              {p.id}
              {p.trait_reported && <span style={{ color: '#8b949e', marginLeft: 4 }}>({p.trait_reported})</span>}
            </span>
          ))}
        </div>
      </div>

      {/* Settings */}
      <div className="rpgs-section">
        <h3 className="rpgs-section-title">Settings</h3>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'flex-end' }}>
          <div style={{ flex: '1 1 140px', minWidth: 120 }}>
            <label style={{ fontSize: 11, color: '#8b949e', display: 'block', marginBottom: 3 }}>Population</label>
            <select className="input" value={settings.refPopulation}
              onChange={e => dispatch({ type: 'UPDATE_SETTINGS', payload: { refPopulation: e.target.value } })}
              style={{ width: '100%', padding: '8px 10px', fontSize: 13, background: '#0d1117', border: '1px solid #30363d', color: '#c9d1d9', borderRadius: 6 }}>
              {POPULATIONS.map(p => <option key={p} value={p}>{p}</option>)}
            </select>
          </div>
          <div style={{ flex: '1 1 160px', minWidth: 140 }}>
            <label style={{ fontSize: 11, color: '#8b949e', display: 'block', marginBottom: 3 }}>Freq Source</label>
            <select className="input" value={settings.freqSource}
              onChange={e => dispatch({ type: 'UPDATE_SETTINGS', payload: { freqSource: e.target.value } })}
              style={{ width: '100%', padding: '8px 10px', fontSize: 13, background: '#0d1117', border: '1px solid #30363d', color: '#c9d1d9', borderRadius: 6 }}>
              <option value="auto">Auto</option>
              <option value="pgs_file">PGS File</option>
              <option value="1kg_plink2">1000G (plink2)</option>
              <option value="vcf_af">VCF AF</option>
              <option value="fallback">Fallback</option>
            </select>
          </div>
          <div style={{ flex: '1 1 160px', minWidth: 140 }}>
            <label style={{ fontSize: 11, color: '#8b949e', display: 'block', marginBottom: 3 }}>Engine</label>
            <select className="input" value={settings.engine}
              onChange={e => dispatch({ type: 'UPDATE_SETTINGS', payload: { engine: e.target.value } })}
              style={{ width: '100%', padding: '8px 10px', fontSize: 13, background: '#0d1117', border: '1px solid #30363d', color: '#c9d1d9', borderRadius: 6 }}>
              <option value="auto">Auto (recommended)</option>
              <option value="custom">Custom (fast, local)</option>
              <option value="pgsc_calc">pgsc_calc (full pipeline)</option>
            </select>
          </div>
        </div>

        {/* Advanced: per-file population */}
        {selectedFiles.length > 1 && (
          <div style={{ marginTop: 12 }}>
            <button
              onClick={() => setShowAdvanced(!showAdvanced)}
              style={{ background: 'transparent', border: 'none', color: '#58a6ff', fontSize: 12, cursor: 'pointer', padding: 0 }}
            >
              {showAdvanced ? 'Hide' : 'Show'} per-file population overrides
            </button>
            {showAdvanced && (
              <div style={{ marginTop: 8, display: 'flex', flexDirection: 'column', gap: 6 }}>
                {selectedFiles.map(f => {
                  const name = f.sample_name || f.path.split('/').pop();
                  return (
                    <div key={f.path} style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                      <span style={{ fontSize: 12, color: '#c9d1d9', minWidth: 140 }}>{name}</span>
                      <select
                        value={filePopulations[f.path] || settings.refPopulation}
                        onChange={e => dispatch({ type: 'SET_FILE_POPULATION', payload: { path: f.path, pop: e.target.value } })}
                        style={{ padding: '4px 8px', fontSize: 12, background: '#0d1117', border: '1px solid #30363d', color: '#c9d1d9', borderRadius: 4 }}
                      >
                        {POPULATIONS.map(p => <option key={p} value={p}>{p}</option>)}
                      </select>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Estimate */}
      <div className="rpgs-section">
        <div style={{ display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap' }}>
          <button className="btn" disabled={estimating} onClick={handleEstimate}>
            {estimating ? 'Estimating...' : 'Estimate Time'}
          </button>
          <button className="btn btn-primary" disabled={starting} onClick={handleStartRun}>
            {starting ? 'Starting...' : 'Start Scoring Run'}
          </button>
        </div>

        {estimate && (
          <div style={{
            background: '#0d1117', border: '1px solid #30363d', borderRadius: 6,
            padding: 14, fontSize: 13,
          }}>
            <div style={{ fontWeight: 600, color: '#e6edf3', marginBottom: 8 }}>Estimate</div>
            {(estimate.estimated_display || estimate.estimated_time) && (
              <div style={{ color: '#c9d1d9', marginBottom: 6 }}>
                Estimated Time: <strong style={{ color: '#58a6ff', fontSize: 16 }}>
                  {estimate.estimated_display || estimate.estimated_time}
                </strong>
                {estimate.estimated_seconds && (
                  <span style={{ color: '#8b949e', marginLeft: 8 }}>({Math.round(estimate.estimated_seconds)}s)</span>
                )}
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
                      {name} ({ftype}): {time} — {(item.total_variants || 0).toLocaleString()} variants
                    </div>
                  );
                })}
              </div>
            )}
            {estimate.warnings && estimate.warnings.length > 0 && (
              <div style={{ marginTop: 8 }}>
                {estimate.warnings.map((w, idx) => (
                  <div key={idx} style={{ color: '#d29922', fontSize: 12 }}>Warning: {w}</div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      <div className="rpgs-step-nav">
        <button className="btn" onClick={() => dispatch({ type: 'SET_STEP', payload: 1 })}>
          &larr; Back
        </button>
        <div />
      </div>
    </div>
  );
}
