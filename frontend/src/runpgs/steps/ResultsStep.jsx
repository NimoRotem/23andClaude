import { useState, useEffect, useCallback } from 'react';
import { runApi, getRawFileUrl } from '../../api.js';
import { useRunPGS, useRunPGSDispatch } from '../RunPGSState.jsx';
import { zscoreColor, riskLabel, matchRateColor, pctColor, shortFilename, fmtBytes, isDone } from '../utils.js';

/* ---- Inline style tokens (dark theme) ---- */
const S = {
  card: { background: '#161b22', border: '1px solid #30363d', borderRadius: 8, padding: 20, marginBottom: 16 },
  cardInner: { background: '#0d1117', border: '1px solid #30363d', borderRadius: 8, padding: 16, marginBottom: 10 },
  label: { fontSize: 11, color: '#8b949e', textTransform: 'uppercase', letterSpacing: 0.5 },
  bigNum: { fontSize: 28, fontWeight: 800, fontFamily: 'monospace', marginTop: 2 },
  medNum: { fontSize: 20, fontWeight: 700, fontFamily: 'monospace', marginTop: 2 },
  sectionTitle: { fontSize: 16, fontWeight: 700, color: '#e6edf3', marginBottom: 12 },
  th: { textAlign: 'left', padding: '10px 12px', color: '#8b949e', fontSize: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 0.5, borderBottom: '2px solid #30363d', position: 'sticky', top: 0, background: '#161b22', zIndex: 1 },
  td: { padding: '10px 12px', fontSize: 13, color: '#c9d1d9', borderBottom: '1px solid #21262d' },
  warning: { background: 'rgba(210,153,34,0.1)', border: '1px solid rgba(210,153,34,0.3)', borderRadius: 8, padding: 12, marginBottom: 16, fontSize: 13, color: '#d29922' },
  select: { width: '100%', padding: '10px 12px', background: '#0d1117', color: '#c9d1d9', border: '1px solid #30363d', borderRadius: 6, fontSize: 14, outline: 'none' },
  button: { padding: '8px 16px', borderRadius: 6, border: '1px solid #30363d', background: '#21262d', color: '#c9d1d9', fontSize: 13, cursor: 'pointer', fontWeight: 600 },
  accentButton: { padding: '8px 16px', borderRadius: 6, border: '1px solid #58a6ff', background: 'rgba(88,166,255,0.1)', color: '#58a6ff', fontSize: 13, cursor: 'pointer', fontWeight: 600 },
};

function FileTypeBadge({ type }) {
  const colors = {
    vcf: { bg: 'rgba(63,185,80,0.15)', fg: '#3fb950', label: 'VCF' },
    gvcf: { bg: 'rgba(88,166,255,0.15)', fg: '#58a6ff', label: 'gVCF' },
    bam: { bg: 'rgba(188,140,255,0.15)', fg: '#bc8cff', label: 'BAM' },
  };
  const c = colors[(type || '').toLowerCase()] || { bg: 'rgba(139,148,158,0.15)', fg: '#8b949e', label: type || '?' };
  return (
    <span style={{ display: 'inline-block', padding: '2px 8px', borderRadius: 4, fontSize: 11, fontWeight: 700, letterSpacing: 0.5, background: c.bg, color: c.fg, textTransform: 'uppercase' }}>
      {c.label}
    </span>
  );
}

function PercentileBar({ pct, color }) {
  if (pct == null) return null;
  return (
    <div style={{ marginTop: 14 }}>
      <div style={{ fontSize: 11, color: '#8b949e', marginBottom: 4 }}>Population distribution</div>
      <div style={{ position: 'relative', height: 24, background: '#21262d', borderRadius: 6, overflow: 'hidden' }}>
        <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, background: 'linear-gradient(to right, #58a6ff, #3fb950 25%, #3fb950 50%, #d29922 75%, #f85149)', opacity: 0.15 }} />
        <div style={{ position: 'absolute', left: `${pct}%`, top: 0, bottom: 0, width: 3, background: color, borderRadius: 2, transform: 'translateX(-1px)', boxShadow: `0 0 6px ${color}` }} />
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: '#484f58', marginTop: 2 }}>
        <span>0%</span><span>25%</span><span>50%</span><span>75%</span><span>100%</span>
      </div>
    </div>
  );
}

function VariantDetailLog({ runId, pgsId, sourceType, sourcePath }) {
  const [open, setOpen] = useState(false);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchDetail = useCallback(() => {
    if (data) { setOpen(o => !o); return; }
    setLoading(true);
    setError(null);
    fetch(`/genomics/api/runs/${runId}/results/detail/${pgsId}`)
      .then(r => { if (r.status === 404) throw new Error('not_found'); if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d => { setData(d); setOpen(true); })
      .catch(e => setError(e.message === 'not_found' ? 'No variant detail available.' : e.message))
      .finally(() => setLoading(false));
  }, [runId, pgsId, data]);

  const sources = data?.sources || (data?.detail ? [data.detail] : []);
  const srcFilename = sourcePath ? sourcePath.split('/').pop() : '';
  const matchingSource = sourcePath
    ? (sources.find(s => s.source_file_path === sourcePath) || sources.find(s => s.source_file_path?.endsWith(srcFilename)) || sources.find(s => s.source_file_type === sourceType))
    : sources[0];
  const allVariants = matchingSource?.variants || [];
  const isTruncated = matchingSource?.variants_truncated || false;
  const variantsInLog = matchingSource?.variants_in_log || allVariants.length;
  const variantsTotal = matchingSource?.variants_total || allVariants.length;
  const found = allVariants.filter(v => v.status === 'found').length;
  const missing = allVariants.filter(v => v.status === 'missing').length;

  const statusColor = (s) => {
    if (s === 'found') return { bg: 'rgba(63,185,80,0.1)', fg: '#3fb950' };
    if (s === 'missing') return { bg: 'rgba(248,81,73,0.1)', fg: '#f85149' };
    if (s === 'imputed') return { bg: 'rgba(88,166,255,0.1)', fg: '#58a6ff' };
    return { bg: 'transparent', fg: '#8b949e' };
  };

  const sampleNames = [];
  for (const v of allVariants) {
    if (v.samples) { for (const s of Object.keys(v.samples)) sampleNames.push(s); break; }
  }

  return (
    <div style={{ marginTop: 10 }}>
      <button onClick={fetchDetail} disabled={loading} style={S.accentButton}>
        {loading ? 'Loading...' : open ? 'Hide Variant Details' : 'View Variant Details'}
      </button>
      {error && <p style={{ color: '#8b949e', fontSize: 13, marginTop: 8, fontStyle: 'italic' }}>{error}</p>}
      {open && allVariants.length > 0 && (
        <div style={{ marginTop: 12 }}>
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 8, fontSize: 13 }}>
            <span style={{ color: '#e6edf3', fontWeight: 600 }}>{allVariants.length} variants</span>
            <span style={{ color: '#3fb950', fontWeight: 600 }}>{found} found</span>
            <span style={{ color: '#f85149', fontWeight: 600 }}>{missing} missing</span>
            <span style={{ color: '#8b949e' }}>Match: {allVariants.length > 0 ? ((found / allVariants.length) * 100).toFixed(1) : 0}%</span>
          </div>
          {isTruncated && (
            <div style={{ fontSize: 12, color: '#d29922', marginBottom: 12, padding: '6px 10px', background: 'rgba(210,169,34,0.08)', borderRadius: 6, border: '1px solid rgba(210,169,34,0.2)' }}>
              Showing first {variantsInLog.toLocaleString()} of {variantsTotal.toLocaleString()} variants.
            </div>
          )}
          <div style={{ maxHeight: 500, overflow: 'auto', borderRadius: 8, border: '1px solid #30363d' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12, fontFamily: 'monospace' }}>
              <thead>
                <tr style={{ position: 'sticky', top: 0, background: '#161b22', zIndex: 1 }}>
                  {['rsID', 'Chr:Pos', 'Effect', 'Other', 'Weight', 'Status', ...sampleNames.map(s => `${s} (GT/Dosage)`)].map(h => (
                    <th key={h} style={{ padding: '8px 10px', textAlign: 'left', color: '#8b949e', fontSize: 11, borderBottom: '1px solid #30363d', whiteSpace: 'nowrap' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {allVariants.map((v, i) => {
                  const sc = statusColor(v.status);
                  return (
                    <tr key={i} style={{ background: i % 2 === 0 ? '#0d1117' : '#161b22', borderBottom: '1px solid #21262d' }}>
                      <td style={{ padding: '5px 10px', color: v.rsid ? '#58a6ff' : '#484f58' }}>{v.rsid || `${v.chr}:${v.pos}`}</td>
                      <td style={{ padding: '5px 10px', color: '#c9d1d9' }}>{v.chr}:{v.pos}</td>
                      <td style={{ padding: '5px 10px', color: '#e6edf3', fontWeight: 700 }}>{v.effect_allele}</td>
                      <td style={{ padding: '5px 10px', color: '#8b949e' }}>{v.other_allele || '--'}</td>
                      <td style={{ padding: '5px 10px', color: '#8b949e' }}>{Number(v.weight).toFixed(4)}</td>
                      <td style={{ padding: '5px 10px' }}>
                        <span style={{ padding: '2px 6px', borderRadius: 4, fontSize: 10, fontWeight: 600, background: sc.bg, color: sc.fg }}>{v.status}</span>
                      </td>
                      {sampleNames.map(s => {
                        const sd = v.samples?.[s];
                        if (!sd) return <td key={s} style={{ padding: '5px 10px', color: '#484f58' }}>--</td>;
                        const gt = sd.gt || './.';
                        const dos = sd.dosage != null ? sd.dosage : '--';
                        return <td key={s} style={{ padding: '5px 10px', color: v.status === 'found' ? '#c9d1d9' : '#484f58' }}>{gt}({dos})</td>;
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
      {open && allVariants.length === 0 && !loading && !error && (
        <p style={{ color: '#8b949e', fontSize: 13, marginTop: 8 }}>No variant-level detail returned.</p>
      )}
    </div>
  );
}

/* ---- Main Results Step ---- */
export default function ResultsStep() {
  const { activeRunId, runDetail, resultsData, rawFiles, selectedPgsId } = useRunPGS();
  const dispatch = useRunPGSDispatch();

  const [runs, setRuns] = useState([]);
  const [selectedRunId, setSelectedRunId] = useState(activeRunId || '');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Derived
  const data = resultsData;
  const sourceFiles = data?.source_files || [];
  const allResults = data?.results || [];
  const pgsIds = [...new Set(allResults.map(r => r.pgs_id))];
  const refPop = runDetail?.config_snapshot?.ref_population || runDetail?.ref_population || 'EUR';
  const localSelectedPgs = selectedPgsId || pgsIds[0] || '';
  const selectedPgsResults = allResults.filter(r => r.pgs_id === localSelectedPgs);

  // Fetch run list
  useEffect(() => {
    runApi.list().then(data => {
      const all = Array.isArray(data) ? data : [];
      setRuns(all.filter(r => r.status === 'complete' || r.status === 'completed'));
    }).catch(() => {});
  }, []);

  // Sync with activeRunId
  useEffect(() => {
    if (activeRunId) setSelectedRunId(String(activeRunId));
  }, [activeRunId]);

  // Fetch run data
  useEffect(() => {
    if (!selectedRunId) return;
    setLoading(true);
    setError(null);
    dispatch({ type: 'SET_RUN_DETAIL', payload: null });
    dispatch({ type: 'SET_RESULTS_DATA', payload: null });
    dispatch({ type: 'SET_RAW_FILES', payload: [] });
    dispatch({ type: 'SET_SELECTED_PGS_ID', payload: '' });

    Promise.all([
      runApi.get(selectedRunId),
      runApi.results(selectedRunId),
      runApi.rawFiles(selectedRunId).catch(() => []),
    ]).then(([detail, resData, raw]) => {
      dispatch({ type: 'SET_RUN_DETAIL', payload: detail });
      const normalised = {
        source_files: resData?.source_files || [],
        results: resData?.results || (Array.isArray(resData) ? resData : []),
        results_by_source: resData?.results_by_source || {},
      };
      dispatch({ type: 'SET_RESULTS_DATA', payload: normalised });
      const ids = [...new Set((normalised.results || []).map(r => r.pgs_id))];
      if (ids.length > 0) dispatch({ type: 'SET_SELECTED_PGS_ID', payload: ids[0] });
      dispatch({ type: 'SET_RAW_FILES', payload: Array.isArray(raw) ? raw : (raw?.files || []) });
    }).catch(err => setError(err.message)).finally(() => setLoading(false));
  }, [selectedRunId, dispatch]);

  return (
    <div>
      {/* Run selector */}
      <div style={S.card}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
          <div style={S.sectionTitle}>Select Run</div>
          <button className="btn" onClick={() => dispatch({ type: 'SET_STEP', payload: 0 })}>
            + New Run
          </button>
        </div>
        <select style={S.select} value={selectedRunId} onChange={e => setSelectedRunId(e.target.value)}>
          <option value="">Select a completed run...</option>
          {runs.map(r => {
            const pgsCount = r.pgs_ids?.length || '?';
            const ts = r.started_at ? new Date(r.started_at).toLocaleString() : 'N/A';
            return <option key={r.id} value={r.id}>Run {r.id} — {pgsCount} PGS — {ts}</option>;
          })}
        </select>
      </div>

      {!selectedRunId && (
        <div style={{ textAlign: 'center', padding: 60, color: '#8b949e' }}>
          <div style={{ fontSize: 48, marginBottom: 16, opacity: 0.3 }}>&#x1F4CA;</div>
          <p style={{ fontSize: 15 }}>No results to display. Complete a scoring run first.</p>
        </div>
      )}

      {loading && <div style={{ textAlign: 'center', padding: 40, color: '#8b949e' }}>Loading results...</div>}
      {error && <div style={{ ...S.warning, borderColor: 'rgba(248,81,73,0.3)', color: '#f85149', background: 'rgba(248,81,73,0.08)' }}><strong>Error:</strong> {error}</div>}

      {/* Run Overview */}
      {runDetail && !loading && (
        <div style={S.card}>
          <div style={S.sectionTitle}>Run Overview</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: 16 }}>
            <div><div style={S.label}>Status</div><div style={{ fontSize: 15, fontWeight: 700, color: '#3fb950', marginTop: 4 }}>{runDetail.status}</div></div>
            <div><div style={S.label}>Engine</div><div style={{ fontSize: 15, fontWeight: 700, color: '#e6edf3', marginTop: 4 }}>{runDetail.engine || '--'}</div></div>
            <div><div style={S.label}>Genome Build</div><div style={{ fontSize: 15, fontWeight: 700, color: '#e6edf3', marginTop: 4 }}>{runDetail.genome_build || '--'}</div></div>
            <div><div style={S.label}>Duration</div><div style={{ fontSize: 15, fontWeight: 700, color: '#e6edf3', marginTop: 4 }}>{runDetail.duration_sec != null ? `${runDetail.duration_sec.toFixed(1)}s` : '--'}</div></div>
            <div><div style={S.label}>Population</div><div style={{ fontSize: 15, fontWeight: 700, color: '#e6edf3', marginTop: 4 }}>{refPop}</div></div>
            <div><div style={S.label}>PGS Scores</div><div style={{ fontSize: 15, fontWeight: 700, color: '#58a6ff', marginTop: 4 }}>{runDetail.pgs_ids?.join(', ') || '--'}</div></div>
          </div>
        </div>
      )}

      {/* Per-PGS Detail */}
      {allResults.length > 0 && !loading && (
        <div style={S.card}>
          <div style={S.sectionTitle}>Per-PGS Detail</div>

          {/* PGS selector */}
          {pgsIds.length > 5 ? (
            <select style={{ ...S.select, marginBottom: 16 }} value={localSelectedPgs}
              onChange={e => dispatch({ type: 'SET_SELECTED_PGS_ID', payload: e.target.value })}>
              {pgsIds.map(id => {
                const r = allResults.find(x => x.pgs_id === id);
                return <option key={id} value={id}>{id} — {r?.trait || 'Unknown'}</option>;
              })}
            </select>
          ) : (
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 16 }}>
              {pgsIds.map(id => {
                const isActive = id === localSelectedPgs;
                const r = allResults.find(x => x.pgs_id === id);
                return (
                  <button key={id} onClick={() => dispatch({ type: 'SET_SELECTED_PGS_ID', payload: id })} style={{
                    ...S.button, background: isActive ? 'rgba(88,166,255,0.15)' : '#21262d',
                    borderColor: isActive ? '#58a6ff' : '#30363d', color: isActive ? '#58a6ff' : '#c9d1d9',
                  }}>
                    {id} {r?.trait ? `(${r.trait})` : ''}
                  </button>
                );
              })}
            </div>
          )}

          {/* Results for selected PGS */}
          {selectedPgsResults.map((pgsResult, srcIdx) => {
            const scores = pgsResult.scores_json || [];
            const isSingleSample = scores.length === 1;
            const sfType = pgsResult.source_file_type || '';
            const sfPath = pgsResult.source_file_path || '';

            return (
              <div key={`${pgsResult.pgs_id}-${sfPath}-${srcIdx}`} style={{ ...S.cardInner, marginBottom: 16 }}>
                {sfPath && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14, paddingBottom: 10, borderBottom: '1px solid #21262d' }}>
                    <FileTypeBadge type={sfType} />
                    <span style={{ fontSize: 13, color: '#c9d1d9', fontFamily: 'monospace' }}>{shortFilename(sfPath)}</span>
                  </div>
                )}

                {/* Summary cards */}
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(150px, 1fr))', gap: 10, marginBottom: 16 }}>
                  <div style={{ ...S.cardInner, textAlign: 'center', padding: 14, marginBottom: 0 }}>
                    <div style={{ fontSize: 20, fontWeight: 700, color: '#58a6ff' }}>{pgsResult.pgs_id}</div>
                    <div style={{ fontSize: 12, color: '#8b949e', marginTop: 4 }}>{pgsResult.trait || '--'}</div>
                  </div>
                  <div style={{ ...S.cardInner, textAlign: 'center', padding: 14, marginBottom: 0 }}>
                    <div style={{ fontSize: 20, fontWeight: 700, color: matchRateColor(pgsResult.match_rate) }}>
                      {pgsResult.match_rate != null ? `${(pgsResult.match_rate * 100).toFixed(1)}%` : '--'}
                    </div>
                    <div style={{ fontSize: 12, color: '#8b949e', marginTop: 4 }}>Match Rate</div>
                  </div>
                  <div style={{ ...S.cardInner, textAlign: 'center', padding: 14, marginBottom: 0 }}>
                    <div style={{ fontSize: 20, fontWeight: 700, color: '#c9d1d9' }}>
                      {pgsResult.variants_matched?.toLocaleString() || '?'} / {pgsResult.variants_total?.toLocaleString() || '?'}
                    </div>
                    <div style={{ fontSize: 12, color: '#8b949e', marginTop: 4 }}>Variants Matched</div>
                  </div>
                </div>

                {/* Low match warning */}
                {pgsResult.match_rate != null && pgsResult.match_rate < 0.5 && (
                  <div style={S.warning}>
                    <strong>Low match rate ({(pgsResult.match_rate * 100).toFixed(1)}%):</strong>{' '}
                    Only {pgsResult.variants_matched} of {pgsResult.variants_total} variants found.
                  </div>
                )}

                {/* Sample scores */}
                {scores.map((s, si) => {
                  const popZ = s.pop_z_score;
                  const famZ = s.z_score;
                  const pct = s.percentile;
                  const displayZ = popZ ?? famZ ?? 0;
                  const risk = riskLabel(displayZ);
                  const pc = pctColor(pct);

                  return (
                    <div key={s.sample || si} style={{ background: '#161b22', border: '1px solid #30363d', borderRadius: 8, padding: 16, marginBottom: 10 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                        <span style={{ fontSize: 16, fontWeight: 700, color: '#e6edf3' }}>{s.sample || `Sample ${si + 1}`}</span>
                        {pct != null && (
                          <span style={{ padding: '3px 10px', borderRadius: 12, fontSize: 12, fontWeight: 600, background: risk.color + '22', color: risk.color, border: `1px solid ${risk.color}44` }}>
                            {risk.text}
                          </span>
                        )}
                      </div>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: 12 }}>
                        <div><div style={S.label}>Percentile ({refPop})</div><div style={{ ...S.bigNum, color: pc }}>{pct != null ? `${pct}%` : '--'}</div></div>
                        <div><div style={S.label}>Pop. Z-Score</div><div style={{ ...S.medNum, color: popZ != null ? zscoreColor(popZ) : '#484f58' }}>{popZ != null ? `${popZ >= 0 ? '+' : ''}${popZ.toFixed(3)}` : '--'}</div></div>
                        <div><div style={S.label}>Raw Score</div><div style={{ ...S.medNum, color: '#e6edf3' }}>{s.raw_score != null ? s.raw_score.toFixed(6) : '--'}</div></div>
                        {!isSingleSample && <div><div style={S.label}>Family Z-Score</div><div style={{ ...S.medNum, color: famZ != null ? zscoreColor(famZ) : '#484f58' }}>{famZ != null ? `${famZ >= 0 ? '+' : ''}${famZ.toFixed(3)}` : '--'}</div></div>}
                      </div>
                      {pct != null && <PercentileBar pct={pct} color={pc} />}
                    </div>
                  );
                })}

                <VariantDetailLog runId={selectedRunId} pgsId={pgsResult.pgs_id} sourceType={sfType} sourcePath={sfPath} />
              </div>
            );
          })}
        </div>
      )}

      {/* Raw files */}
      {rawFiles.length > 0 && !loading && (
        <div style={S.card}>
          <div style={S.sectionTitle}>Output Files</div>
          {rawFiles.map((file, i) => {
            const name = typeof file === 'string' ? file : (file.name || file.filename);
            const size = typeof file === 'object' ? file.size : null;
            return (
              <div key={name || i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 12px', borderRadius: 6, background: i % 2 === 0 ? '#0d1117' : 'transparent', fontSize: 13 }}>
                <a href={getRawFileUrl(selectedRunId, name)} target="_blank" rel="noopener noreferrer" style={{ color: '#58a6ff', textDecoration: 'none', fontFamily: 'monospace' }}>{name}</a>
                <span style={{ color: '#8b949e', fontSize: 12 }}>{fmtBytes(size)}</span>
              </div>
            );
          })}
        </div>
      )}

      {/* Start new run */}
      <div style={{ textAlign: 'center', marginTop: 24 }}>
        <button className="btn" onClick={() => dispatch({ type: 'RESET' })}>
          Start New Scoring Run
        </button>
      </div>
    </div>
  );
}
