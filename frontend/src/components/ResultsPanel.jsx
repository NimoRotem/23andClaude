import { useState, useEffect, useCallback } from 'react';
import { getRawFileUrl, runApi, ancestryApi } from '../api.js';
import AncestryBadge from './AncestryBadge.jsx';
import { useAppState } from '../context.jsx';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Ancestry data cache (module-level to persist across re-renders)
let _ancestryCache = null;
let _ancestryLoading = false;

async function loadAncestryData() {
  if (_ancestryCache) return _ancestryCache;
  if (_ancestryLoading) return null;
  _ancestryLoading = true;
  try {
    const [samples, scores] = await Promise.all([
      ancestryApi.all(),
      ancestryApi.scores(),
    ]);
    _ancestryCache = { samples, scores };
    return _ancestryCache;
  } catch {
    return null;
  } finally {
    _ancestryLoading = false;
  }
}

function getAncestryBadgeForScore(sampleId, pgsId, ancestryData) {
  if (!ancestryData || !ancestryData.scores) return null;
  // Find best ancestry score for this sample+pgs combo
  const match = ancestryData.scores.find(
    s => s.sample_id === sampleId && s.pgs_id?.includes(pgsId?.replace('PGS', ''))
  );
  if (!match) {
    // Check if sample has ancestry data at all
    const sample = ancestryData.samples?.find(s => s.sample_id === sampleId);
    if (sample) {
      // Has ancestry but no PRS-CSx score — check if PGS training pop matches
      return { confidence: sample.primary_ancestry === 'EUR' ? 'high' : 'low' };
    }
    return null;
  }
  return match;
}

function zscoreColor(z) {
  const az = Math.abs(z);
  if (az >= 2) return '#f85149';
  if (az >= 1) return '#d29922';
  return '#3fb950';
}

function fmtSize(bytes) {
  if (!bytes) return '--';
  if (bytes > 1e9) return (bytes / 1e9).toFixed(1) + ' GB';
  if (bytes > 1e6) return (bytes / 1e6).toFixed(1) + ' MB';
  return bytes + ' B';
}

function riskLabel(z) {
  if (z >= 2) return { text: 'High Risk', color: '#f85149' };
  if (z >= 1) return { text: 'Above Average', color: '#d29922' };
  if (z > -1) return { text: 'Average', color: '#3fb950' };
  if (z > -2) return { text: 'Below Average', color: '#d29922' };
  return { text: 'Low Risk', color: '#58a6ff' };
}

function matchRateColor(rate) {
  if (rate > 0.5) return '#3fb950';
  if (rate > 0.1) return '#d29922';
  return '#f85149';
}

function pctColor(pct) {
  if (pct == null) return '#484f58';
  if (pct >= 90) return '#f85149';
  if (pct >= 75) return '#d29922';
  if (pct >= 25) return '#3fb950';
  if (pct >= 10) return '#d29922';
  return '#58a6ff';
}

function shortFilename(path) {
  if (!path) return '--';
  const parts = path.split('/');
  return parts[parts.length - 1];
}

function fileTypeBadge(type) {
  const colors = {
    vcf: { bg: 'rgba(63,185,80,0.15)', fg: '#3fb950', label: 'VCF' },
    bam: { bg: 'rgba(88,166,255,0.15)', fg: '#58a6ff', label: 'BAM' },
  };
  const c = colors[(type || '').toLowerCase()] || { bg: 'rgba(139,148,158,0.15)', fg: '#8b949e', label: type || '?' };
  return (
    <span style={{
      display: 'inline-block', padding: '2px 8px', borderRadius: 4,
      fontSize: 11, fontWeight: 700, letterSpacing: 0.5,
      background: c.bg, color: c.fg, textTransform: 'uppercase',
    }}>
      {c.label}
    </span>
  );
}

/** Determine cell color for multi-source comparison agreement */
function agreementColor(values) {
  if (values.length < 2) return 'transparent';
  const nums = values.filter(v => v != null);
  if (nums.length < 2) return 'transparent';
  const max = Math.max(...nums);
  const min = Math.min(...nums);
  const diff = max - min;
  if (diff <= 10) return 'rgba(63,185,80,0.12)';   // green — agree
  if (diff <= 20) return 'rgba(210,153,34,0.12)';   // yellow — moderate
  return 'rgba(248,81,73,0.12)';                     // red — disagree
}

// ---------------------------------------------------------------------------
// Shared inline styles
// ---------------------------------------------------------------------------

const S = {
  card: {
    background: '#161b22', border: '1px solid #30363d', borderRadius: 8,
    padding: 20, marginBottom: 16,
  },
  cardInner: {
    background: '#0d1117', border: '1px solid #30363d', borderRadius: 8,
    padding: 16, marginBottom: 10,
  },
  label: {
    fontSize: 11, color: '#8b949e', textTransform: 'uppercase', letterSpacing: 0.5,
  },
  bigNum: {
    fontSize: 28, fontWeight: 800, fontFamily: 'monospace', marginTop: 2,
  },
  medNum: {
    fontSize: 20, fontWeight: 700, fontFamily: 'monospace', marginTop: 2,
  },
  sectionTitle: {
    fontSize: 16, fontWeight: 700, color: '#e6edf3', marginBottom: 12,
  },
  th: {
    textAlign: 'left', padding: '10px 12px', color: '#8b949e', fontSize: 12,
    fontWeight: 600, textTransform: 'uppercase', letterSpacing: 0.5,
    borderBottom: '2px solid #30363d', position: 'sticky', top: 0,
    background: '#161b22', zIndex: 1,
  },
  td: {
    padding: '10px 12px', fontSize: 13, color: '#c9d1d9',
    borderBottom: '1px solid #21262d',
  },
  warning: {
    background: 'rgba(210,153,34,0.1)', border: '1px solid rgba(210,153,34,0.3)',
    borderRadius: 8, padding: 12, marginBottom: 16, fontSize: 13, color: '#d29922',
  },
  select: {
    width: '100%', padding: '10px 12px', background: '#0d1117', color: '#c9d1d9',
    border: '1px solid #30363d', borderRadius: 6, fontSize: 14, outline: 'none',
  },
  button: {
    padding: '8px 16px', borderRadius: 6, border: '1px solid #30363d',
    background: '#21262d', color: '#c9d1d9', fontSize: 13, cursor: 'pointer',
    fontWeight: 600, transition: 'background 0.15s',
  },
  accentButton: {
    padding: '8px 16px', borderRadius: 6, border: '1px solid #58a6ff',
    background: 'rgba(88,166,255,0.1)', color: '#58a6ff', fontSize: 13,
    cursor: 'pointer', fontWeight: 600, transition: 'background 0.15s',
  },
};

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

/** Section 5: Variant Detail Log */
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
      .then(r => {
        if (r.status === 404) throw new Error('not_found');
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(d => { setData(d); setOpen(true); })
      .catch(e => {
        if (e.message === 'not_found') {
          setError('No variant detail available for this PGS score.');
        } else {
          setError(e.message);
        }
      })
      .finally(() => setLoading(false));
  }, [runId, pgsId, data]);

  // Handle both single detail and multi-source details
  const sources = data?.sources || (data?.detail ? [data.detail] : []);
  // Filter to the matching source — match by path first (most specific), then type
  const srcFilename = sourcePath ? sourcePath.split('/').pop() : '';
  const matchingSource = sourcePath
    ? (sources.find(s => s.source_file_path === sourcePath)
       || sources.find(s => s.source_file_path?.endsWith(srcFilename))
       || sources.find(s => s.source_file_type === sourceType))
    : sources[0];
  const allVariants = matchingSource?.variants || [];
  const isTruncated = matchingSource?.variants_truncated || false;
  const variantsInLog = matchingSource?.variants_in_log || allVariants.length;
  const variantsTotal = matchingSource?.variants_total || allVariants.length;

  const found = allVariants.filter(v => v.status === 'found').length;
  const missing = allVariants.filter(v => v.status === 'missing').length;
  const imputed = allVariants.filter(v => v.status === 'imputed').length;
  const total = allVariants.length;

  const statusColor = (s) => {
    if (s === 'found') return { bg: 'rgba(63,185,80,0.1)', fg: '#3fb950' };
    if (s === 'missing') return { bg: 'rgba(248,81,73,0.1)', fg: '#f85149' };
    if (s === 'imputed') return { bg: 'rgba(88,166,255,0.1)', fg: '#58a6ff' };
    return { bg: 'transparent', fg: '#8b949e' };
  };

  // Get sample names from first found variant
  const sampleNames = [];
  for (const v of allVariants) {
    if (v.samples) {
      for (const s of Object.keys(v.samples)) sampleNames.push(s);
      break;
    }
  }

  return (
    <div style={{ marginTop: 8 }}>
      <button onClick={fetchDetail} disabled={loading} style={S.accentButton}>
        {loading ? 'Loading...' : open ? 'Hide Variant Details' : 'View Variant Details'}
      </button>

      {error && <p style={{ color: '#8b949e', fontSize: 13, marginTop: 8, fontStyle: 'italic' }}>{error}</p>}

      {open && allVariants.length > 0 && (
        <div style={{ marginTop: 12 }}>
          {/* Source info */}
          {matchingSource?.source_file_path && (
            <div style={{ fontSize: 12, color: '#8b949e', marginBottom: 8 }}>
              Source: <strong style={{ color: '#58a6ff' }}>{matchingSource.source_file_path.split('/').pop()}</strong>
              {' '}({matchingSource.source_file_type || 'unknown'})
            </div>
          )}

          {/* Summary */}
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 8, fontSize: 13 }}>
            <span style={{ color: '#e6edf3', fontWeight: 600 }}>{total} total variants</span>
            <span style={{ color: '#3fb950', fontWeight: 600 }}>{found} found in source</span>
            {imputed > 0 && <span style={{ color: '#58a6ff', fontWeight: 600 }}>{imputed} imputed</span>}
            <span style={{ color: '#f85149', fontWeight: 600 }}>{missing} missing</span>
            <span style={{ color: '#8b949e' }}>Match: {total > 0 ? ((found / total) * 100).toFixed(1) : 0}%</span>
          </div>
          {isTruncated && (
            <div style={{ fontSize: 12, color: '#d29922', marginBottom: 12, padding: '6px 10px', background: 'rgba(210,169,34,0.08)', borderRadius: 6, border: '1px solid rgba(210,169,34,0.2)' }}>
              Showing first {variantsInLog.toLocaleString()} of {variantsTotal.toLocaleString()} variants. Full scoring used all variants.
            </div>
          )}

          {/* Variant table */}
          <div style={{ maxHeight: 500, overflow: 'auto', borderRadius: 8, border: '1px solid #30363d' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12, fontFamily: 'monospace' }}>
              <thead>
                <tr style={{ position: 'sticky', top: 0, background: '#161b22', zIndex: 1 }}>
                  {['rsID', 'Chr:Pos', 'Effect', 'Other', 'Weight', 'Status',
                    ...sampleNames.map(s => `${s} (GT/Dosage)`)
                  ].map(h => (
                    <th key={h} style={{ padding: '8px 10px', textAlign: 'left', color: '#8b949e', fontSize: 11, borderBottom: '1px solid #30363d', whiteSpace: 'nowrap' }}>
                      {h}
                    </th>
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
                      <td style={{ padding: '5px 10px', color: '#8b949e' }}>{v.other_allele || v.ref || '--'}</td>
                      <td style={{ padding: '5px 10px', color: '#8b949e' }}>{Number(v.weight).toFixed(4)}</td>
                      <td style={{ padding: '5px 10px' }}>
                        <span style={{ padding: '2px 6px', borderRadius: 4, fontSize: 10, fontWeight: 600, background: sc.bg, color: sc.fg }}>
                          {v.status}
                        </span>
                      </td>
                      {sampleNames.map(s => {
                        const sd = v.samples?.[s];
                        if (!sd) return <td key={s} style={{ padding: '5px 10px', color: '#484f58' }}>--</td>;
                        const gt = sd.gt || './.';
                        const dos = sd.dosage != null ? sd.dosage : '--';
                        return (
                          <td key={s} style={{ padding: '5px 10px', color: v.status === 'found' ? '#c9d1d9' : '#484f58' }}>
                            {gt}({dos})
                          </td>
                        );
                      })}
                      {sampleNames.length === 0 && v.status === 'imputed' && (
                        <td style={{ padding: '5px 10px', color: '#58a6ff' }}>imputed ({v.imputed_dosage?.toFixed(2)})</td>
                      )}
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

/** Percentile bar visualisation */
function PercentileBar({ pct, color }) {
  if (pct == null) return null;
  return (
    <div style={{ marginTop: 14 }}>
      <div style={{ fontSize: 11, color: '#8b949e', marginBottom: 4 }}>
        Population distribution (estimated from effect allele frequencies)
      </div>
      <div style={{ position: 'relative', height: 24, background: '#21262d', borderRadius: 6, overflow: 'hidden' }}>
        <div style={{
          position: 'absolute', top: 0, left: 0, right: 0, bottom: 0,
          background: 'linear-gradient(to right, #58a6ff, #3fb950 25%, #3fb950 50%, #d29922 75%, #f85149)',
          opacity: 0.15,
        }} />
        <div style={{
          position: 'absolute', left: `${pct}%`, top: 0, bottom: 0,
          width: 3, background: color, borderRadius: 2,
          transform: 'translateX(-1px)',
          boxShadow: `0 0 6px ${color}`,
        }} />
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: '#484f58', marginTop: 2 }}>
        <span>0%</span><span>25%</span><span>50%</span><span>75%</span><span>100%</span>
      </div>
    </div>
  );
}


// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export default function ResultsPanel() {
  const { activeRunId } = useAppState();

  // --- State ---
  const [runs, setRuns] = useState([]);
  const [selectedRunId, setSelectedRunId] = useState(activeRunId || '');
  const [runDetail, setRunDetail] = useState(null);
  const [resultsData, setResultsData] = useState(null);
  const [rawFiles, setRawFiles] = useState([]);
  const [selectedPgsId, setSelectedPgsId] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Derived
  const sourceFiles = resultsData?.source_files || [];
  const allResults = resultsData?.results || [];
  const resultsBySource = resultsData?.results_by_source || {};
  const hasMultipleSources = sourceFiles.length > 1;
  const pgsIds = [...new Set(allResults.map(r => r.pgs_id))];
  const refPop = runDetail?.config_snapshot?.ref_population || runDetail?.ref_population || 'EUR';

  // Results for selected PGS
  const selectedPgsResults = allResults.filter(r => r.pgs_id === selectedPgsId);

  // ---------------------------------------------------------------------------
  // Effects
  // ---------------------------------------------------------------------------

  // Fetch run list
  useEffect(() => {
    runApi.list()
      .then(data => {
        const all = Array.isArray(data) ? data : [];
        const done = all.filter(r => r.status === 'complete' || r.status === 'completed');
        setRuns(done);
        if (!selectedRunId && done.length > 0) setSelectedRunId(String(done[0].id));
      })
      .catch(() => {});
  }, []);

  // Sync with activeRunId from context
  useEffect(() => {
    if (activeRunId) setSelectedRunId(String(activeRunId));
  }, [activeRunId]);

  // Fetch run data when selectedRunId changes
  useEffect(() => {
    if (!selectedRunId) return;
    setLoading(true);
    setError(null);
    setRunDetail(null);
    setResultsData(null);
    setRawFiles([]);
    setSelectedPgsId('');

    Promise.all([
      runApi.get(selectedRunId),
      runApi.results(selectedRunId),
      runApi.rawFiles(selectedRunId).catch(() => []),
    ])
      .then(([detail, resData, raw]) => {
        setRunDetail(detail);

        // Normalise results payload
        const normalised = {
          source_files: resData?.source_files || [],
          results: resData?.results || (Array.isArray(resData) ? resData : []),
          results_by_source: resData?.results_by_source || {},
        };
        setResultsData(normalised);

        // Default selected PGS to first available
        const ids = [...new Set((normalised.results || []).map(r => r.pgs_id))];
        if (ids.length > 0) setSelectedPgsId(ids[0]);

        setRawFiles(Array.isArray(raw) ? raw : (raw?.files || []));
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [selectedRunId]);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      {/* Page Header */}
      <div className="section-header">
        <h2 className="section-title">Results</h2>
      </div>

      {/* ================================================================= */}
      {/* SECTION 1: Run Selector                                           */}
      {/* ================================================================= */}
      <div style={{ ...S.card, marginBottom: 20 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
          <div style={S.sectionTitle}>Select Run</div>
          {selectedRunId && (
            <button
              style={{ background: 'transparent', border: '1px solid #f85149', color: '#f85149', padding: '4px 12px', borderRadius: 4, fontSize: 12, cursor: 'pointer' }}
              onClick={async () => {
                if (!window.confirm(`Delete run ${selectedRunId}?\n\nThis will permanently remove all results, detail files, and reports for this run.`)) return;
                try {
                  await fetch(`/genomics/api/runs/${selectedRunId}`, { method: 'DELETE' });
                  setRuns(prev => prev.filter(r => r.id !== selectedRunId));
                  setSelectedRunId('');
                  setRunDetail(null);
                  setResultsData(null);
                } catch {}
              }}
            >
              Delete Run
            </button>
          )}
        </div>
        <select
          style={S.select}
          value={selectedRunId}
          onChange={e => setSelectedRunId(e.target.value)}
        >
          <option value="">Select a completed run...</option>
          {runs.map(r => {
            const pgsCount = r.pgs_ids?.length || '?';
            const srcCount = r.config_snapshot?.source_files?.length || r.source_files?.length || 1;
            const ts = r.started_at ? new Date(r.started_at).toLocaleString() : 'N/A';
            return (
              <option key={r.id} value={r.id}>
                Run {r.id} — {pgsCount} PGS, {srcCount} source file{srcCount !== 1 ? 's' : ''} — {ts}
              </option>
            );
          })}
        </select>
      </div>

      {/* Empty state */}
      {!selectedRunId && (
        <div style={{ textAlign: 'center', padding: 60, color: '#8b949e' }}>
          <div style={{ fontSize: 48, marginBottom: 16, opacity: 0.3 }}>&#x1F4CA;</div>
          <p style={{ fontSize: 15 }}>No results to display. Complete a scoring run first.</p>
        </div>
      )}

      {loading && (
        <div style={{ textAlign: 'center', padding: 40, color: '#8b949e' }}>
          <p style={{ fontSize: 14 }}>Loading results...</p>
        </div>
      )}
      {error && (
        <div style={{ ...S.warning, borderColor: 'rgba(248,81,73,0.3)', color: '#f85149', background: 'rgba(248,81,73,0.08)' }}>
          <strong>Error:</strong> {error}
        </div>
      )}

      {/* ================================================================= */}
      {/* SECTION 2: Run Overview                                           */}
      {/* ================================================================= */}
      {runDetail && !loading && (
        <div style={S.card}>
          <div style={S.sectionTitle}>Run Overview</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 16 }}>
            {/* Status */}
            <div>
              <div style={S.label}>Status</div>
              <div style={{ fontSize: 15, fontWeight: 700, color: '#3fb950', marginTop: 4 }}>
                {runDetail.status}
              </div>
            </div>
            {/* Engine */}
            <div>
              <div style={S.label}>Engine</div>
              <div style={{ fontSize: 15, fontWeight: 700, color: '#e6edf3', marginTop: 4 }}>
                {runDetail.engine || '--'}
              </div>
            </div>
            {/* Genome Build */}
            <div>
              <div style={S.label}>Genome Build</div>
              <div style={{ fontSize: 15, fontWeight: 700, color: '#e6edf3', marginTop: 4 }}>
                {runDetail.genome_build || '--'}
              </div>
            </div>
            {/* Duration */}
            <div>
              <div style={S.label}>Duration</div>
              <div style={{ fontSize: 15, fontWeight: 700, color: '#e6edf3', marginTop: 4 }}>
                {runDetail.duration_sec != null ? `${runDetail.duration_sec.toFixed(1)}s` : '--'}
              </div>
            </div>
            {/* Ref Population */}
            <div>
              <div style={S.label}>Ref Population</div>
              <div style={{ fontSize: 15, fontWeight: 700, color: '#e6edf3', marginTop: 4 }}>
                {refPop}
              </div>
            </div>
            {/* PGS IDs */}
            <div>
              <div style={S.label}>PGS Scores</div>
              <div style={{ fontSize: 15, fontWeight: 700, color: '#58a6ff', marginTop: 4 }}>
                {runDetail.pgs_ids?.join(', ') || '--'}
              </div>
            </div>
          </div>

          {/* Source Files List */}
          {sourceFiles.length > 0 && (
            <div style={{ marginTop: 16 }}>
              <div style={{ ...S.label, marginBottom: 8 }}>Source Files ({sourceFiles.length})</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {sourceFiles.map((sf, i) => (
                  <div key={i} style={{
                    display: 'flex', alignItems: 'center', gap: 10,
                    padding: '8px 12px', background: '#0d1117', borderRadius: 6,
                    border: '1px solid #21262d',
                  }}>
                    {fileTypeBadge(sf.type)}
                    <span style={{ fontSize: 13, color: '#c9d1d9', fontFamily: 'monospace', wordBreak: 'break-all' }}>
                      {sf.filename || shortFilename(sf.path)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ================================================================= */}
      {/* SECTION 3: Multi-Source Comparison                                 */}
      {/* ================================================================= */}
      {hasMultipleSources && !loading && allResults.length > 0 && (
        <div style={S.card}>
          <div style={S.sectionTitle}>Multi-Source Comparison</div>
          <p style={{ fontSize: 13, color: '#8b949e', marginBottom: 16, lineHeight: 1.6 }}>
            Scores from multiple source files (e.g. VCF vs BAM) are shown side-by-side.
            Cells are color-coded: <span style={{ color: '#3fb950' }}>green</span> = results agree (within 10 pp),{' '}
            <span style={{ color: '#d29922' }}>yellow</span> = moderate difference,{' '}
            <span style={{ color: '#f85149' }}>red</span> = results disagree.
          </p>

          <div style={{ overflow: 'auto', borderRadius: 8, border: '1px solid #30363d' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr>
                  <th style={{ ...S.th, minWidth: 130 }}>PGS ID</th>
                  <th style={{ ...S.th, minWidth: 120 }}>Trait</th>
                  {sourceFiles.map((sf, i) => (
                    <th key={i} style={{ ...S.th, textAlign: 'center', minWidth: 180 }}>
                      <div>{fileTypeBadge(sf.type)}</div>
                      <div style={{ fontSize: 11, color: '#c9d1d9', marginTop: 4, fontWeight: 400, textTransform: 'none', letterSpacing: 0 }}>
                        {sf.filename || shortFilename(sf.path)}
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pgsIds.map((pgsId, rowIdx) => {
                  // Gather results for each source file for this PGS
                  const rowResults = sourceFiles.map(sf => {
                    return allResults.find(r =>
                      r.pgs_id === pgsId && (r.source_file_path === sf.path || r.source_file_path === sf.filename)
                    ) || null;
                  });

                  // For agreement colouring, collect percentiles
                  const percentiles = rowResults.map(r => {
                    if (!r) return null;
                    const scores = r.scores_json || [];
                    return scores.length > 0 ? scores[0].percentile : null;
                  });
                  const agColor = agreementColor(percentiles);

                  return (
                    <tr key={pgsId} style={{
                      background: rowIdx % 2 === 0 ? '#0d1117' : '#161b22',
                      borderBottom: '1px solid #21262d',
                    }}>
                      <td style={{
                        ...S.td, color: '#58a6ff', fontWeight: 600, cursor: 'pointer',
                      }} onClick={() => setSelectedPgsId(pgsId)}>
                        {pgsId}
                      </td>
                      <td style={S.td}>
                        {(rowResults.find(r => r)?.trait) || '--'}
                      </td>
                      {rowResults.map((r, colIdx) => {
                        if (!r) {
                          return (
                            <td key={colIdx} style={{ ...S.td, textAlign: 'center', color: '#484f58' }}>
                              --
                            </td>
                          );
                        }
                        const scores = r.scores_json || [];
                        const s = scores[0] || {};
                        const pct = s.percentile;
                        const rawScore = s.raw_score;
                        const mr = r.match_rate;

                        return (
                          <td key={colIdx} style={{
                            ...S.td, textAlign: 'center', background: agColor,
                          }}>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 4, alignItems: 'center' }}>
                              {/* Percentile */}
                              <span style={{
                                fontSize: 18, fontWeight: 800, fontFamily: 'monospace',
                                color: pctColor(pct),
                              }}>
                                {pct != null ? `${pct}%` : '--'}
                              </span>
                              {/* Raw score */}
                              <span style={{ fontSize: 11, color: '#8b949e' }}>
                                raw: {rawScore != null ? rawScore.toFixed(6) : '--'}
                              </span>
                              {/* Match rate */}
                              <span style={{
                                fontSize: 11, fontWeight: 600,
                                color: matchRateColor(mr),
                              }}>
                                match: {mr != null ? `${(mr * 100).toFixed(1)}%` : '--'}
                              </span>
                            </div>
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ================================================================= */}
      {/* SECTION 4: Per-PGS Detail                                         */}
      {/* ================================================================= */}
      {allResults.length > 0 && !loading && (
        <div style={S.card}>
          <div style={S.sectionTitle}>Per-PGS Detail</div>

          {/* PGS Selector */}
          {pgsIds.length > 5 ? (
            <div style={{ marginBottom: 16 }}>
              <select
                style={S.select}
                value={selectedPgsId}
                onChange={e => setSelectedPgsId(e.target.value)}
              >
                {pgsIds.map(id => {
                  const r = allResults.find(x => x.pgs_id === id);
                  return (
                    <option key={id} value={id}>
                      {id} — {r?.trait || 'Unknown'}
                    </option>
                  );
                })}
              </select>
            </div>
          ) : (
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 16 }}>
              {pgsIds.map(id => {
                const isActive = id === selectedPgsId;
                const r = allResults.find(x => x.pgs_id === id);
                return (
                  <button
                    key={id}
                    onClick={() => setSelectedPgsId(id)}
                    style={{
                      ...S.button,
                      background: isActive ? 'rgba(88,166,255,0.15)' : '#21262d',
                      borderColor: isActive ? '#58a6ff' : '#30363d',
                      color: isActive ? '#58a6ff' : '#c9d1d9',
                    }}
                  >
                    {id} {r?.trait ? `(${r.trait})` : ''}
                  </button>
                );
              })}
            </div>
          )}

          {/* Per-source results for selected PGS */}
          {selectedPgsResults.length === 0 && selectedPgsId && (
            <p style={{ color: '#8b949e', fontSize: 13 }}>No results for {selectedPgsId}.</p>
          )}

          {selectedPgsResults.map((pgsResult, srcIdx) => {
            const scores = pgsResult.scores_json || [];
            const isSingleSample = scores.length === 1;
            const sfType = pgsResult.source_file_type || '';
            const sfPath = pgsResult.source_file_path || '';

            return (
              <div key={`${pgsResult.pgs_id}-${sfPath}-${srcIdx}`} style={{
                ...S.cardInner, marginBottom: 16,
              }}>
                {/* Source file header */}
                {sfPath && (
                  <div style={{
                    display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14,
                    paddingBottom: 10, borderBottom: '1px solid #21262d',
                  }}>
                    {sfType && fileTypeBadge(sfType)}
                    <span style={{ fontSize: 13, color: '#c9d1d9', fontFamily: 'monospace' }}>
                      {shortFilename(sfPath)}
                    </span>
                  </div>
                )}

                {/* Summary Cards Row */}
                <div style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fill, minmax(150px, 1fr))',
                  gap: 10, marginBottom: 16,
                }}>
                  {/* PGS ID + Trait */}
                  <div style={{ ...S.cardInner, textAlign: 'center', padding: 14, marginBottom: 0 }}>
                    <div style={{ fontSize: 20, fontWeight: 700, color: '#58a6ff' }}>{pgsResult.pgs_id}</div>
                    <div style={{ fontSize: 12, color: '#8b949e', marginTop: 4 }}>{pgsResult.trait || '--'}</div>
                  </div>
                  {/* Match Rate */}
                  <div style={{ ...S.cardInner, textAlign: 'center', padding: 14, marginBottom: 0 }}>
                    <div style={{
                      fontSize: 20, fontWeight: 700,
                      color: matchRateColor(pgsResult.match_rate),
                    }}>
                      {pgsResult.match_rate != null ? `${(pgsResult.match_rate * 100).toFixed(1)}%` : '--'}
                    </div>
                    <div style={{ fontSize: 12, color: '#8b949e', marginTop: 4 }}>Match Rate</div>
                  </div>
                  {/* Variants Matched */}
                  <div style={{ ...S.cardInner, textAlign: 'center', padding: 14, marginBottom: 0 }}>
                    <div style={{ fontSize: 20, fontWeight: 700, color: '#c9d1d9' }}>
                      {pgsResult.variants_matched?.toLocaleString() || '?'} / {pgsResult.variants_total?.toLocaleString() || '?'}
                    </div>
                    <div style={{ fontSize: 12, color: '#8b949e', marginTop: 4 }}>Variants Matched</div>
                  </div>
                  {/* Confidence */}
                  {pgsResult.confidence && (
                    <div style={{ ...S.cardInner, textAlign: 'center', padding: 14, marginBottom: 0 }}>
                      <div style={{
                        fontSize: 15, fontWeight: 700,
                        color: pgsResult.confidence.level === 'high' ? '#3fb950'
                          : pgsResult.confidence.level === 'moderate' ? '#58a6ff' : '#d29922',
                      }}>
                        {pgsResult.confidence.label}
                      </div>
                      <div style={{ fontSize: 12, color: '#8b949e', marginTop: 4 }}>Confidence</div>
                    </div>
                  )}
                </div>

                {/* Low match rate warning */}
                {pgsResult.match_rate != null && pgsResult.match_rate < 0.5 && (
                  <div style={S.warning}>
                    <strong>Low match rate ({(pgsResult.match_rate * 100).toFixed(1)}%):</strong>{' '}
                    Only {pgsResult.variants_matched} of {pgsResult.variants_total} PGS variants were found.
                    Common causes: genome build mismatch, variants-only VCF missing hom-ref sites, or rare variants not called.
                    {pgsResult.match_rate < 0.1 && (
                      <span style={{ color: '#f85149' }}>
                        {' '}Match rate below 10% — results are unreliable. Verify genome build matches PGS scoring file.
                      </span>
                    )}
                  </div>
                )}

                {/* Sample Scores */}
                {scores.length > 0 && scores.map((s, si) => {
                  const famZ = s.z_score;
                  const popZ = s.pop_z_score;
                  const pct = s.percentile;
                  const displayZ = popZ ?? famZ ?? 0;
                  const hasPercentile = pct != null;
                  const risk = riskLabel(displayZ);
                  const pc = pctColor(pct);

                  return (
                    <div key={s.sample || si} style={{
                      background: '#161b22', border: '1px solid #30363d', borderRadius: 8,
                      padding: 16, marginBottom: 10,
                    }}>
                      {/* Sample header */}
                      <div style={{
                        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                        marginBottom: 12,
                      }}>
                        <div>
                          <span style={{ fontSize: 16, fontWeight: 700, color: '#e6edf3' }}>
                            {s.sample || `Sample ${si + 1}`}
                          </span>
                          {!isSingleSample && (
                            <span style={{ marginLeft: 8, fontSize: 12, color: '#8b949e' }}>
                              Rank #{s.rank || si + 1}
                            </span>
                          )}
                        </div>
                        {hasPercentile && (
                          <span style={{
                            padding: '3px 10px', borderRadius: 12, fontSize: 12, fontWeight: 600,
                            background: risk.color + '22', color: risk.color,
                            border: `1px solid ${risk.color}44`,
                          }}>
                            {risk.text}
                          </span>
                        )}
                      </div>

                      {/* Metrics grid */}
                      <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
                        gap: 12,
                      }}>
                        {/* Percentile */}
                        <div>
                          <div style={S.label}>
                            Percentile ({refPop})
                            {s.confidence_level === 'low' && (
                              <span style={{ color: '#d29922', marginLeft: 4 }}>~approx</span>
                            )}
                          </div>
                          <div style={{ ...S.bigNum, color: pc }}>
                            {hasPercentile
                              ? (s.confidence_level === 'low' ? `~${pct}%` : `${pct}%`)
                              : '--'}
                          </div>
                        </div>
                        {/* Z-score */}
                        <div>
                          <div style={S.label}>Pop. Z-Score</div>
                          <div style={{
                            ...S.medNum,
                            color: popZ != null ? zscoreColor(popZ) : '#484f58',
                          }}>
                            {popZ != null ? `${popZ >= 0 ? '+' : ''}${popZ.toFixed(3)}` : '--'}
                          </div>
                        </div>
                        {/* Raw score */}
                        <div>
                          <div style={S.label}>Raw Score</div>
                          <div style={{ ...S.medNum, color: '#e6edf3' }}>
                            {s.raw_score != null ? s.raw_score.toFixed(6) : '--'}
                          </div>
                        </div>
                        {/* Family Z-score — multi-sample only */}
                        {!isSingleSample && (
                          <div>
                            <div style={S.label}>Family Z-Score</div>
                            <div style={{
                              ...S.medNum,
                              color: famZ != null ? zscoreColor(famZ) : '#484f58',
                            }}>
                              {famZ != null ? `${famZ >= 0 ? '+' : ''}${famZ.toFixed(3)}` : '--'}
                            </div>
                          </div>
                        )}
                        {/* Variants */}
                        <div>
                          <div style={S.label}>Variants Used</div>
                          <div style={{ ...S.medNum, color: '#e6edf3' }}>
                            {s.variants_used || pgsResult.variants_matched || '--'}
                          </div>
                        </div>
                      </div>

                      {/* Percentile bar */}
                      {hasPercentile && <PercentileBar pct={pct} color={pc} />}
                    </div>
                  );
                })}

                {scores.length === 0 && (
                  <p style={{ color: '#8b949e', fontSize: 13, textAlign: 'center', padding: 16 }}>
                    No sample scores available for this source file.
                  </p>
                )}

                {/* Section 5: Variant Detail Log */}
                <VariantDetailLog runId={selectedRunId} pgsId={pgsResult.pgs_id} sourceType={sfType} sourcePath={sfPath} />
              </div>
            );
          })}
        </div>
      )}

      {/* ================================================================= */}
      {/* SECTION 6: Raw Files                                              */}
      {/* ================================================================= */}
      {rawFiles.length > 0 && !loading && (
        <div style={S.card}>
          <div style={S.sectionTitle}>Output Files</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {rawFiles.map((file, i) => {
              const name = typeof file === 'string' ? file : (file.name || file.filename);
              const size = typeof file === 'object' ? file.size : null;
              return (
                <div key={name || i} style={{
                  display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                  padding: '10px 12px', borderRadius: 6,
                  background: i % 2 === 0 ? '#0d1117' : 'transparent',
                  fontSize: 13,
                }}>
                  <a
                    href={getRawFileUrl(selectedRunId, name)}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ color: '#58a6ff', textDecoration: 'none', fontFamily: 'monospace' }}
                  >
                    {name}
                  </a>
                  <span style={{ color: '#8b949e', fontSize: 12 }}>{fmtSize(size)}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* All PGS Overview Table (when multiple PGS) */}
      {pgsIds.length > 1 && !loading && !hasMultipleSources && (
        <div style={S.card}>
          <div style={S.sectionTitle}>All PGS Results</div>
          <div style={{ overflow: 'auto', borderRadius: 8, border: '1px solid #30363d' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr>
                  <th style={S.th}>PGS ID</th>
                  <th style={S.th}>Trait</th>
                  <th style={{ ...S.th, textAlign: 'right' }}>Match Rate</th>
                  <th style={{ ...S.th, textAlign: 'right' }}>Variants</th>
                  <th style={{ ...S.th, textAlign: 'right' }}>Samples</th>
                </tr>
              </thead>
              <tbody>
                {allResults.map((r, i) => (
                  <tr
                    key={`${r.pgs_id}-${i}`}
                    style={{
                      background: i % 2 === 0 ? '#0d1117' : '#161b22',
                      borderBottom: '1px solid #21262d',
                      cursor: 'pointer',
                    }}
                    onClick={() => setSelectedPgsId(r.pgs_id)}
                  >
                    <td style={{
                      ...S.td, color: '#58a6ff', fontWeight: 600,
                    }}>{r.pgs_id}</td>
                    <td style={S.td}>{r.trait || '--'}</td>
                    <td style={{
                      ...S.td, textAlign: 'right',
                      color: matchRateColor(r.match_rate),
                    }}>
                      {r.match_rate != null ? `${(r.match_rate * 100).toFixed(1)}%` : '--'}
                    </td>
                    <td style={{ ...S.td, textAlign: 'right', color: '#8b949e' }}>
                      {r.variants_matched || '?'}/{r.variants_total || '?'}
                    </td>
                    <td style={{ ...S.td, textAlign: 'right', color: '#8b949e' }}>
                      {r.scores_json?.length || 0}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
