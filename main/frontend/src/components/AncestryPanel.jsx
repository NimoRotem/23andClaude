import { useState, useEffect, useCallback, useRef } from 'react';

/* ── Constants ────────────────────────────────────────────────── */

const POP_COLORS = {
  EUR: '#58a6ff',
  EAS: '#3fb950',
  AFR: '#f0883e',
  SAS: '#bc8cff',
  AMR: '#f85149',
};
const POP_LABELS = {
  EUR: 'European',
  EAS: 'East Asian',
  AFR: 'African',
  SAS: 'South Asian',
  AMR: 'Admixed American',
};
const POPS = ['EUR', 'EAS', 'AFR', 'SAS', 'AMR'];
const BASE = '/genomics/api';

/* ── PCA Scatter Plot (SVG) ───────────────────────────────────── */

function PCAPlot({ points, width = 700, height = 440 }) {
  const [axes, setAxes] = useState({ x: 'pc1', y: 'pc2' });
  const [hoveredSample, setHoveredSample] = useState(null);
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });
  const svgRef = useRef(null);

  if (!points || points.length === 0) {
    return <div style={{ color: '#8b949e', padding: 20, textAlign: 'center' }}>No PCA data available. Run ancestry inference first.</div>;
  }

  const pad = { top: 30, right: 30, bottom: 50, left: 60 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;

  const xVals = points.map(p => p[axes.x]);
  const yVals = points.map(p => p[axes.y]);
  const xMin = Math.min(...xVals), xMax = Math.max(...xVals);
  const yMin = Math.min(...yVals), yMax = Math.max(...yVals);
  const xRange = xMax - xMin || 1;
  const yRange = yMax - yMin || 1;
  const xPad = xRange * 0.05, yPad = yRange * 0.05;

  const sx = (v) => pad.left + ((v - xMin + xPad) / (xRange + 2 * xPad)) * plotW;
  const sy = (v) => pad.top + plotH - ((v - yMin + yPad) / (yRange + 2 * yPad)) * plotH;

  // Separate ref and our points
  const refPoints = points.filter(p => p.is_reference);
  const ourPoints = points.filter(p => !p.is_reference);

  const axisOptions = [
    { value: 'pc1_pc2', label: 'PC1 vs PC2' },
    { value: 'pc1_pc3', label: 'PC1 vs PC3' },
    { value: 'pc2_pc3', label: 'PC2 vs PC3' },
  ];

  return (
    <div>
      <div style={{ display: 'flex', gap: 8, marginBottom: 8, alignItems: 'center' }}>
        <span style={{ fontSize: 12, color: '#8b949e' }}>Axes:</span>
        {axisOptions.map(opt => {
          const [ax, ay] = opt.value.split('_');
          const active = axes.x === ax && axes.y === ay;
          return (
            <button key={opt.value}
              onClick={() => setAxes({ x: ax, y: ay })}
              style={{
                padding: '3px 10px', borderRadius: 4, fontSize: 11,
                border: '1px solid ' + (active ? '#58a6ff' : '#30363d'),
                background: active ? 'rgba(88,166,255,0.15)' : 'transparent',
                color: active ? '#58a6ff' : '#8b949e', cursor: 'pointer',
              }}>
              {opt.label}
            </button>
          );
        })}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 12 }}>
          {POPS.map(pop => (
            <span key={pop} style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 11, color: '#8b949e' }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: POP_COLORS[pop], display: 'inline-block' }} />
              {pop}
            </span>
          ))}
        </div>
      </div>

      <svg ref={svgRef} width={width} height={height} style={{ background: '#0d1117', borderRadius: 8, border: '1px solid #21262d' }}>
        {/* Grid lines */}
        {[0.25, 0.5, 0.75].map(frac => (
          <g key={frac}>
            <line x1={pad.left} y1={pad.top + plotH * frac} x2={pad.left + plotW} y2={pad.top + plotH * frac}
              stroke="#161b22" strokeWidth={1} />
            <line x1={pad.left + plotW * frac} y1={pad.top} x2={pad.left + plotW * frac} y2={pad.top + plotH}
              stroke="#161b22" strokeWidth={1} />
          </g>
        ))}

        {/* Axes */}
        <line x1={pad.left} y1={pad.top + plotH} x2={pad.left + plotW} y2={pad.top + plotH} stroke="#30363d" />
        <line x1={pad.left} y1={pad.top} x2={pad.left} y2={pad.top + plotH} stroke="#30363d" />
        <text x={pad.left + plotW / 2} y={height - 10} fill="#8b949e" fontSize={12} textAnchor="middle">
          {axes.x.toUpperCase()}
        </text>
        <text x={15} y={pad.top + plotH / 2} fill="#8b949e" fontSize={12} textAnchor="middle"
          transform={`rotate(-90, 15, ${pad.top + plotH / 2})`}>
          {axes.y.toUpperCase()}
        </text>

        {/* Reference points (small, semi-transparent) */}
        {refPoints.map((p, i) => (
          <circle key={`ref-${i}`}
            cx={sx(p[axes.x])} cy={sy(p[axes.y])} r={2}
            fill={POP_COLORS[p.population] || '#484f58'}
            opacity={0.3}
          />
        ))}

        {/* Our samples (large, bright, with labels) */}
        {ourPoints.map((p, i) => {
          const cx = sx(p[axes.x]);
          const cy = sy(p[axes.y]);
          return (
            <g key={`our-${i}`}
              onMouseEnter={(e) => {
                setHoveredSample(p);
                const rect = svgRef.current.getBoundingClientRect();
                setTooltipPos({ x: e.clientX - rect.left, y: e.clientY - rect.top });
              }}
              onMouseLeave={() => setHoveredSample(null)}
              style={{ cursor: 'pointer' }}>
              {/* Diamond marker */}
              <polygon
                points={`${cx},${cy - 8} ${cx + 6},${cy} ${cx},${cy + 8} ${cx - 6},${cy}`}
                fill={POP_COLORS[p.population] || '#e6edf3'}
                stroke="#0d1117" strokeWidth={1.5}
              />
              {/* Label */}
              <text x={cx + 10} y={cy + 4} fill="#e6edf3" fontSize={11} fontWeight={600}>
                {p.sample_id}
              </text>
            </g>
          );
        })}
      </svg>

      {/* Tooltip */}
      {hoveredSample && (
        <div style={{
          position: 'absolute',
          left: tooltipPos.x + 16, top: tooltipPos.y - 10,
          background: '#1c2128', border: '1px solid #30363d', borderRadius: 8,
          padding: '10px 14px', zIndex: 100, fontSize: 12, color: '#c9d1d9',
          pointerEvents: 'none', minWidth: 160,
        }}>
          <div style={{ fontWeight: 700, color: '#e6edf3', marginBottom: 6 }}>{hoveredSample.sample_id}</div>
          <div>Population: <span style={{ color: POP_COLORS[hoveredSample.population] }}>{hoveredSample.population}</span></div>
          <div>{axes.x.toUpperCase()}: {hoveredSample[axes.x]?.toFixed(4)}</div>
          <div>{axes.y.toUpperCase()}: {hoveredSample[axes.y]?.toFixed(4)}</div>
        </div>
      )}
    </div>
  );
}

/* ── Admixture Bar Chart ──────────────────────────────────────── */

function AdmixtureBarChart({ samples }) {
  if (!samples || samples.length === 0) return null;

  const barH = 36;
  const labelW = 100;
  const barW = 500;
  const gap = 4;
  const totalH = samples.length * (barH + gap) + 40;

  return (
    <svg width={labelW + barW + 40} height={totalH}>
      {/* Legend */}
      <g transform={`translate(${labelW}, 10)`}>
        {POPS.map((pop, i) => (
          <g key={pop} transform={`translate(${i * 90}, 0)`}>
            <rect x={0} y={0} width={12} height={12} rx={2} fill={POP_COLORS[pop]} />
            <text x={16} y={10} fill="#8b949e" fontSize={11}>{POP_LABELS[pop]}</text>
          </g>
        ))}
      </g>

      {/* Bars */}
      {samples.map((s, si) => {
        const y = 30 + si * (barH + gap);
        let xOffset = 0;
        const props = s.proportions;

        return (
          <g key={s.sample_id}>
            <text x={labelW - 8} y={y + barH / 2 + 4} fill="#e6edf3" fontSize={13} fontWeight={600} textAnchor="end">
              {s.sample_id}
            </text>
            {POPS.map(pop => {
              const frac = props[pop] || 0;
              const w = frac * barW;
              const x = labelW + xOffset;
              xOffset += w;
              if (w < 1) return null;
              return (
                <g key={pop}>
                  <rect x={x} y={y} width={w} height={barH} fill={POP_COLORS[pop]} rx={si === 0 && pop === POPS[0] ? 4 : 0} />
                  {w > 30 && (
                    <text x={x + w / 2} y={y + barH / 2 + 4} fill="#0d1117" fontSize={10} fontWeight={700} textAnchor="middle">
                      {(frac * 100).toFixed(0)}%
                    </text>
                  )}
                </g>
              );
            })}
            {/* Border */}
            <rect x={labelW} y={y} width={barW} height={barH} fill="none" stroke="#30363d" rx={4} />
          </g>
        );
      })}
    </svg>
  );
}

/* ── Ancestry Summary Table ───────────────────────────────────── */

function AncestryTable({ samples }) {
  if (!samples || samples.length === 0) return null;

  return (
    <div style={{ overflowX: 'auto', margin: '12px 0' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
        <thead>
          <tr>
            <th style={thStyle}>Sample</th>
            <th style={thStyle}>Primary</th>
            {POPS.map(p => <th key={p} style={{ ...thStyle, color: POP_COLORS[p] }}>{p}</th>)}
            <th style={thStyle}>Status</th>
            <th style={thStyle}>Method</th>
          </tr>
        </thead>
        <tbody>
          {samples.map(s => (
            <tr key={s.sample_id} style={{ borderBottom: '1px solid #161b22' }}
              onMouseEnter={e => e.currentTarget.style.background = '#1c2128'}
              onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
              <td style={{ ...tdStyle, fontWeight: 700, color: '#e6edf3' }}>{s.sample_id}</td>
              <td style={tdStyle}>
                <span style={{
                  display: 'inline-flex', alignItems: 'center', gap: 4,
                  padding: '2px 8px', borderRadius: 10,
                  background: POP_COLORS[s.primary_ancestry] + '20',
                  color: POP_COLORS[s.primary_ancestry], fontSize: 12, fontWeight: 600,
                }}>
                  <span style={{ width: 6, height: 6, borderRadius: '50%', background: POP_COLORS[s.primary_ancestry] }} />
                  {s.primary_ancestry}
                </span>
              </td>
              {POPS.map(p => (
                <td key={p} style={{ ...tdStyle, fontFamily: 'monospace', fontSize: 12, color: (s.proportions[p] || 0) > 0.1 ? '#e6edf3' : '#484f58' }}>
                  {((s.proportions[p] || 0) * 100).toFixed(1)}%
                </td>
              ))}
              <td style={tdStyle}>
                <span style={{
                  padding: '2px 8px', borderRadius: 10, fontSize: 11,
                  background: s.is_admixed ? 'rgba(210,153,34,0.15)' : 'rgba(63,185,80,0.15)',
                  color: s.is_admixed ? '#d29922' : '#3fb950',
                }}>
                  {s.is_admixed ? s.admixture_description : 'Single-ancestry'}
                </span>
              </td>
              <td style={{ ...tdStyle, fontSize: 11, color: '#6e7681' }}>{s.inference_method || '--'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const thStyle = {
  padding: '8px 10px', textAlign: 'left', color: '#8b949e', fontWeight: 600,
  borderBottom: '2px solid #21262d', whiteSpace: 'nowrap', fontSize: 12,
};
const tdStyle = {
  padding: '8px 10px', color: '#c9d1d9',
};

/* ── Pipeline Status Panel ────────────────────────────────────── */

function PipelineStatus({ status, onRunInference }) {
  const statusColor = {
    idle: '#8b949e', running: '#d29922', complete: '#3fb950', error: '#f85149',
  };

  return (
    <div style={{
      background: '#161b22', border: '1px solid #30363d', borderRadius: 8,
      padding: 16, marginBottom: 16, display: 'flex', alignItems: 'center', gap: 16,
    }}>
      <div style={{ flex: 1 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
          <span style={{
            width: 8, height: 8, borderRadius: '50%',
            background: statusColor[status.status] || '#8b949e',
            ...(status.status === 'running' ? { animation: 'pulse 1.5s infinite' } : {}),
          }} />
          <span style={{ color: '#e6edf3', fontSize: 14, fontWeight: 600 }}>
            Ancestry Pipeline: {status.status.charAt(0).toUpperCase() + status.status.slice(1)}
          </span>
        </div>
        <div style={{ display: 'flex', gap: 20, fontSize: 12, color: '#8b949e' }}>
          <span>Reference Panel: {status.reference_panel_ready ? '\u2705' : '\u274C'}</span>
          <span>PCA Computed: {status.pca_computed ? '\u2705' : '\u274C'}</span>
          <span>Classifier: {status.classifier_ready ? '\u2705' : '\u274C'}</span>
          <span>Samples: {status.samples_inferred}</span>
        </div>
        {status.message && (
          <div style={{ marginTop: 6, fontSize: 12, color: statusColor[status.status] || '#8b949e' }}>
            {status.message}
          </div>
        )}
      </div>
      <button
        onClick={onRunInference}
        disabled={status.status === 'running'}
        style={{
          padding: '8px 16px', borderRadius: 6, fontSize: 13,
          border: '1px solid #238636', background: '#238636', color: '#fff',
          cursor: status.status === 'running' ? 'not-allowed' : 'pointer',
          opacity: status.status === 'running' ? 0.5 : 1,
        }}>
        {status.status === 'running' ? 'Running...' : 'Run Inference'}
      </button>
    </div>
  );
}

/* ── GWAS Availability Panel ──────────────────────────────────── */

function GWASAvailability({ availability }) {
  if (!availability || Object.keys(availability).length === 0) {
    return (
      <div style={{ color: '#8b949e', fontSize: 13, padding: 16, background: '#161b22', borderRadius: 8, border: '1px solid #30363d' }}>
        No GWAS summary statistics found. Download summary statistics to <code style={{ background: '#21262d', padding: '1px 5px', borderRadius: 3 }}>/data/gwas_sumstats/&#123;trait&#125;/&#123;POP&#125;.txt.gz</code>
      </div>
    );
  }

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
        <thead>
          <tr>
            <th style={thStyle}>Trait</th>
            {POPS.map(p => <th key={p} style={{ ...thStyle, textAlign: 'center', color: POP_COLORS[p] }}>{p}</th>)}
            <th style={{ ...thStyle, textAlign: 'center' }}>PRS-CSx Ready</th>
          </tr>
        </thead>
        <tbody>
          {Object.entries(availability).sort().map(([trait, info]) => (
            <tr key={trait} style={{ borderBottom: '1px solid #161b22' }}>
              <td style={{ ...tdStyle, fontWeight: 600 }}>{trait}</td>
              {POPS.map(p => (
                <td key={p} style={{ ...tdStyle, textAlign: 'center' }}>
                  {info.populations.includes(p) ? '\u2705' : '\u274C'}
                </td>
              ))}
              <td style={{ ...tdStyle, textAlign: 'center' }}>
                <span style={{
                  padding: '2px 8px', borderRadius: 10, fontSize: 11,
                  background: info.prscsx_ready ? 'rgba(63,185,80,0.15)' : 'rgba(139,148,158,0.1)',
                  color: info.prscsx_ready ? '#3fb950' : '#484f58',
                }}>
                  {info.prscsx_ready ? 'Ready' : 'Need 2+ pops'}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ── Main Ancestry Panel ──────────────────────────────────────── */

export default function AncestryPanel() {
  const [samples, setSamples] = useState([]);
  const [pcaPoints, setPcaPoints] = useState([]);
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [gwasAvailability, setGwasAvailability] = useState(null);
  const [loading, setLoading] = useState(true);
  const [activeSection, setActiveSection] = useState('overview');

  // Source file selection for per-sample ancestry runs
  const [sourceFiles, setSourceFiles] = useState([]);
  const [selectedFile, setSelectedFile] = useState('');
  const [sampleRunStatus, setSampleRunStatus] = useState(null);

  const token = localStorage.getItem('auth_token');
  const headers = { Authorization: `Bearer ${token}` };

  const fetchData = useCallback(async () => {
    try {
      const [samplesRes, pcaRes, statusRes, gwasRes, filesRes] = await Promise.all([
        fetch(`${BASE}/ancestry/all`, { headers }).then(r => r.json()),
        fetch(`${BASE}/ancestry/pca`, { headers }).then(r => r.json()),
        fetch(`${BASE}/ancestry/status`, { headers }).then(r => r.json()),
        fetch(`${BASE}/ancestry/gwas-availability`, { headers }).then(r => r.json()),
        fetch(`${BASE}/ancestry/source-files`, { headers }).then(r => r.ok ? r.json() : []),
      ]);
      setSamples(samplesRes);
      setPcaPoints(pcaRes);
      setPipelineStatus(statusRes);
      setGwasAvailability(gwasRes);
      if (filesRes && Array.isArray(filesRes)) setSourceFiles(filesRes);
    } catch (e) {
      console.error('Failed to load ancestry data:', e);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleRunInference = async () => {
    try {
      await fetch(`${BASE}/ancestry/run-inference`, { method: 'POST', headers });
      // Poll status
      const poll = setInterval(async () => {
        const res = await fetch(`${BASE}/ancestry/status`, { headers });
        const data = await res.json();
        setPipelineStatus(data);
        if (data.status !== 'running') {
          clearInterval(poll);
          fetchData();
        }
      }, 3000);
    } catch (e) {
      console.error('Failed to start inference:', e);
    }
  };

  const handleRunSample = async () => {
    if (!selectedFile) { alert('Please select a file from the dropdown first.'); return; }
    const file = sourceFiles.find(f => f.path === selectedFile);
    if (!file) return;
    try {
      setSampleRunStatus({ status: 'running', message: `Starting ancestry for ${file.name}...` });
      const res = await fetch(`${BASE}/ancestry/run-sample`, {
        method: 'POST', headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ sample_name: file.name, file_path: file.path, file_type: file.type }),
      });
      if (!res.ok) {
        const err = await res.json();
        setSampleRunStatus({ status: 'error', message: err.detail || 'Failed to start' });
        return;
      }
      // Poll sample run status
      const poll = setInterval(async () => {
        try {
          const statusRes = await fetch(`${BASE}/ancestry/run-sample-status/${file.name}`, { headers });
          const data = await statusRes.json();
          setSampleRunStatus(data);
          if (data.status !== 'running') {
            clearInterval(poll);
            fetchData();
          }
        } catch {}
      }, 3000);
    } catch (e) {
      console.error('Failed to start sample ancestry:', e);
      setSampleRunStatus({ status: 'error', message: String(e) });
    }
  };

  if (loading) {
    return <div style={{ padding: 40, textAlign: 'center', color: '#8b949e' }}>Loading ancestry data...</div>;
  }

  const sections = [
    { id: 'overview', label: 'Overview' },
    { id: 'pca', label: 'PCA Plot' },
    { id: 'admixture', label: 'Admixture' },
    { id: 'gwas', label: 'GWAS Summary Stats' },
  ];

  return (
    <div>
      {/* Section tabs */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 16, borderBottom: '1px solid #21262d', paddingBottom: 8 }}>
        {sections.map(s => (
          <button key={s.id}
            onClick={() => setActiveSection(s.id)}
            style={{
              padding: '6px 14px', borderRadius: 6, fontSize: 13, cursor: 'pointer',
              border: activeSection === s.id ? '1px solid #58a6ff' : '1px solid transparent',
              background: activeSection === s.id ? 'rgba(88,166,255,0.1)' : 'transparent',
              color: activeSection === s.id ? '#58a6ff' : '#8b949e',
            }}>
            {s.label}
          </button>
        ))}
        <button onClick={fetchData} style={{
          marginLeft: 'auto', padding: '5px 12px', borderRadius: 6, fontSize: 12,
          border: '1px solid #30363d', background: '#21262d', color: '#c9d1d9', cursor: 'pointer',
        }}>
          Refresh
        </button>
      </div>

      {/* Sample file selector + run button */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 12, padding: '12px 16px', marginBottom: 12,
        background: '#161b22', border: '1px solid #30363d', borderRadius: 8,
      }}>
        <span style={{ fontSize: 14, fontWeight: 600, color: '#e6edf3', whiteSpace: 'nowrap' }}>Source File:</span>
        <select value={selectedFile} onChange={e => setSelectedFile(e.target.value)}
          style={{
            flex: 1, maxWidth: 450, padding: '8px 12px', fontSize: 14,
            background: '#0d1117', border: '1px solid #58a6ff', color: '#e6edf3',
            borderRadius: 6, cursor: 'pointer',
          }}>
          <option value="">Select a BAM / VCF / gVCF file...</option>
          {sourceFiles.map((s, i) => (
            <option key={`${s.path}-${i}`} value={s.path}>
              {s.name} [{s.type.toUpperCase()}] — {s.path.split('/').pop()}
            </option>
          ))}
        </select>
        <button onClick={handleRunSample}
          disabled={!selectedFile || (sampleRunStatus && sampleRunStatus.status === 'running')}
          style={{
            padding: '8px 16px', borderRadius: 6, fontSize: 13, fontWeight: 600,
            border: '1px solid #238636', cursor: selectedFile ? 'pointer' : 'not-allowed',
            background: selectedFile && !(sampleRunStatus && sampleRunStatus.status === 'running') ? '#238636' : '#21262d',
            color: selectedFile && !(sampleRunStatus && sampleRunStatus.status === 'running') ? '#fff' : '#484f58',
          }}>
          {sampleRunStatus && sampleRunStatus.status === 'running' ? 'Running...' : 'Run Ancestry'}
        </button>
        {sampleRunStatus && sampleRunStatus.status === 'complete' && (
          <span style={{ fontSize: 12, color: '#3fb950' }}>✓ Done</span>
        )}
        {sampleRunStatus && sampleRunStatus.status === 'error' && (
          <span style={{ fontSize: 12, color: '#f85149' }} title={sampleRunStatus.message}>✗ Error</span>
        )}
      </div>

      {/* Pipeline status */}
      {pipelineStatus && (
        <PipelineStatus status={pipelineStatus} onRunInference={handleRunInference} />
      )}

      {/* Overview */}
      {activeSection === 'overview' && (
        <div>
          <h3 style={{ color: '#e6edf3', fontSize: 16, marginBottom: 12 }}>Ancestry Summary</h3>
          {samples.length > 0 ? (
            <AncestryTable samples={samples} />
          ) : (
            <div style={{ color: '#8b949e', fontSize: 13, padding: 20, textAlign: 'center',
              background: '#161b22', borderRadius: 8, border: '1px solid #30363d' }}>
              No ancestry data yet. Run the ancestry inference pipeline to get started.
            </div>
          )}
        </div>
      )}

      {/* PCA */}
      {activeSection === 'pca' && (
        <div>
          <h3 style={{ color: '#e6edf3', fontSize: 16, marginBottom: 12 }}>
            PCA — Principal Component Analysis
          </h3>
          <div style={{ background: '#161b22', border: '1px solid #30363d', borderRadius: 8, padding: 16, position: 'relative' }}>
            <PCAPlot points={pcaPoints} />
          </div>
          <p style={{ color: '#6e7681', fontSize: 12, marginTop: 8 }}>
            Small dots: 1000 Genomes reference samples (2,504 individuals, 5 superpopulations).
            Large diamonds: your samples. Hover for details.
          </p>
        </div>
      )}

      {/* Admixture */}
      {activeSection === 'admixture' && (
        <div>
          <h3 style={{ color: '#e6edf3', fontSize: 16, marginBottom: 12 }}>
            Admixture Proportions
          </h3>
          {samples.length > 0 ? (
            <div style={{ background: '#161b22', border: '1px solid #30363d', borderRadius: 8, padding: 16 }}>
              <AdmixtureBarChart samples={samples} />
            </div>
          ) : (
            <div style={{ color: '#8b949e', fontSize: 13, padding: 20, textAlign: 'center' }}>
              No ancestry data available.
            </div>
          )}
        </div>
      )}

      {/* GWAS */}
      {activeSection === 'gwas' && (
        <div>
          <h3 style={{ color: '#e6edf3', fontSize: 16, marginBottom: 12 }}>
            GWAS Summary Statistics Availability
          </h3>
          <p style={{ color: '#8b949e', fontSize: 13, marginBottom: 12 }}>
            PRS-CSx requires GWAS summary statistics from at least 2 populations per trait.
            Download from <a href="https://www.pgscatalog.org" target="_blank" rel="noopener" style={{ color: '#58a6ff' }}>PGS Catalog</a>,
            {' '}<a href="https://pheweb.jp" target="_blank" rel="noopener" style={{ color: '#58a6ff' }}>Biobank Japan</a>,
            {' '}<a href="https://www.ukbiobank.ac.uk" target="_blank" rel="noopener" style={{ color: '#58a6ff' }}>UK Biobank</a>.
          </p>
          <GWASAvailability availability={gwasAvailability} />
        </div>
      )}
    </div>
  );
}
