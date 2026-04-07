import { useState, useEffect, useRef, useCallback } from 'react';
import { useAppDispatch } from '../context.jsx';
import AnalysisMasterList from './AnalysisMasterList.jsx';

/* ══════════════════════════════════════════════════════════════
   PIPELINE JOB COMPONENTS (merged from PipelinePanel)
   ══════════════════════════════════════════════════════════════ */

const STEP_LABELS = {
  'dv-calling': 'DeepVariant Calling',
  'dv-call_variants': 'call_variants (GPU)',
  'dv-postprocess': 'postprocess_variants',
  'dv-filter': 'Filter & QC',
  'calling': 'Variant Calling',
  'concatenating': 'Concatenating',
  'normalizing': 'Normalizing',
  'filtering': 'Filtering',
  'qc': 'Quality Control',
  'joint-genotyping': 'Joint Genotyping',
  'cleanup': 'Cleanup',
  'done': 'Complete',
};

const DV_STAGES = [
  { key: 'make_examples', label: 'make_examples', desc: 'CPU' },
  { key: 'call_variants', label: 'call_variants', desc: 'GPU' },
  { key: 'postprocess', label: 'postprocess', desc: 'CPU' },
];

function pipFormatElapsed(secs) {
  if (!secs || secs < 0) return '0s';
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = Math.floor(secs % 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function pipFormatEta(secs) {
  if (!secs || secs <= 0) return 'calculating...';
  const m = secs / 60;
  if (m < 1) return '<1 min';
  if (m < 120) return `~${Math.round(m)} min`;
  return `~${(m / 60).toFixed(1)} hrs`;
}

function inferDvStage(logs) {
  if (!logs || logs.length === 0) return 0;
  const joined = logs.join('\n').toLowerCase();
  if (joined.includes('postprocess')) return 2;
  if (joined.includes('call_variants')) return 1;
  return 0;
}

function PipelineProgressBar({ pct, color = '#58a6ff' }) {
  return (
    <div style={{ background: '#21262d', borderRadius: 6, height: 8, overflow: 'hidden', marginTop: 4 }}>
      <div style={{
        width: `${Math.min(Math.max(pct, 0), 100)}%`,
        height: '100%',
        background: color,
        borderRadius: 6,
        transition: 'width 0.5s ease',
      }} />
    </div>
  );
}

function ActiveJobCard({ job }) {
  const pct = (job.progress || 0) * 100;
  const isDv = job.mode === 'deepvariant';
  const dvStage = isDv ? inferDvStage(job.logs) : -1;
  const isGpu = dvStage === 1;
  const stepLabel = STEP_LABELS[job.step] || job.step || 'Starting...';
  const logs = (job.logs || []).slice(-6);

  return (
    <div style={{
      background: '#161b22', border: `1px solid ${job.status === 'running' ? '#388bfd44' : job.status === 'failed' ? '#f8514944' : '#238636'}`,
      borderRadius: 8, padding: 14, marginBottom: 8,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontFamily: 'monospace', fontSize: 13, color: '#8b949e' }}>{job.job_id?.slice(0, 8)}</span>
          {job.sample_name && <span style={{ fontWeight: 600, color: '#e6edf3', fontSize: 14 }}>{job.sample_name}</span>}
          <span style={{
            fontSize: 11, padding: '2px 6px', borderRadius: 4,
            background: isDv ? '#6e40c922' : '#21262d',
            color: isDv ? '#d2a8ff' : '#8b949e',
            border: `1px solid ${isDv ? '#6e40c944' : '#30363d'}`,
          }}>{isDv ? 'DeepVariant' : 'bcftools'}</span>
          {isDv && isGpu && job.status === 'running' && (
            <span style={{
              fontSize: 11, padding: '2px 6px', borderRadius: 4,
              background: '#3fb95022', color: '#3fb950', border: '1px solid #3fb95044',
              animation: 'pulse 2s infinite',
            }}>GPU Active</span>
          )}
        </div>
        <span style={{
          fontSize: 12, fontWeight: 600,
          color: job.status === 'running' ? '#58a6ff' : job.status === 'completed' ? '#3fb950' : '#f85149',
        }}>{job.status === 'running' ? 'Running' : job.status === 'completed' ? 'Completed' : 'Failed'}</span>
      </div>

      {/* DV stage indicators */}
      {isDv && job.status === 'running' && (
        <div style={{ display: 'flex', gap: 4, marginBottom: 8 }}>
          {DV_STAGES.map((stage, i) => (
            <div key={stage.key} style={{
              flex: 1, padding: '4px 8px', borderRadius: 4, fontSize: 11, textAlign: 'center',
              background: i < dvStage ? '#23863622' : i === dvStage ? '#388bfd22' : '#21262d',
              color: i < dvStage ? '#3fb950' : i === dvStage ? '#58a6ff' : '#484f58',
              border: `1px solid ${i === dvStage ? '#388bfd44' : 'transparent'}`,
            }}>
              {stage.label} <span style={{ opacity: 0.7 }}>({stage.desc})</span>
            </div>
          ))}
        </div>
      )}

      {/* Progress */}
      {job.status === 'running' && (
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: '#8b949e', marginBottom: 2 }}>
            <span>{stepLabel}</span>
            <span>{pct.toFixed(1)}%</span>
          </div>
          <PipelineProgressBar pct={pct} color={isGpu ? '#d2a8ff' : '#58a6ff'} />
          <div style={{ display: 'flex', gap: 16, fontSize: 12, color: '#8b949e', marginTop: 6 }}>
            <span>Elapsed: <strong style={{ color: '#c9d1d9' }}>{pipFormatElapsed(job.elapsed)}</strong></span>
            <span>ETA: <strong style={{ color: '#c9d1d9' }}>{pipFormatEta(job.eta)}</strong></span>
          </div>
        </div>
      )}

      {/* Completed */}
      {job.status === 'completed' && (
        <div style={{ fontSize: 13, color: '#8b949e' }}>
          <span>Finished in {pipFormatElapsed(job.elapsed)}</span>
          {job.qc_result && (
            <span style={{ marginLeft: 12 }}>
              Variants: {job.qc_result.variant_count?.toLocaleString()} | Ti/Tv: {job.qc_result.titv_ratio?.toFixed(2)}
            </span>
          )}
        </div>
      )}

      {/* Error */}
      {job.status === 'failed' && job.error && (
        <div style={{ fontSize: 12, color: '#f85149', background: '#f8514911', borderRadius: 4, padding: '6px 8px', marginTop: 4 }}>
          {job.error}
        </div>
      )}

      {/* Logs */}
      {job.status === 'running' && logs.length > 0 && (
        <div style={{ marginTop: 8, fontSize: 11, fontFamily: 'monospace', color: '#6e7681', maxHeight: 100, overflow: 'auto', lineHeight: 1.5 }}>
          {logs.map((line, i) => <div key={i}>{line}</div>)}
        </div>
      )}
    </div>
  );
}

function PipelineSection({ showToast }) {
  const [jobs, setJobs] = useState([]);
  const [activeJob, setActiveJob] = useState(null);
  const [showNimog, setShowNimog] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [gpuAvailable, setGpuAvailable] = useState(null);
  const sseRef = useRef(null);
  const pollRef = useRef(null);

  useEffect(() => {
    fetch('/genomics/api/system/capabilities')
      .then(r => r.json())
      .then(data => setGpuAvailable(data.gpu_available))
      .catch(() => {});
  }, []);

  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch('/nimog/api/jobs');
      if (res.ok) {
        const data = await res.json();
        setJobs(Array.isArray(data) ? data : []);
      }
    } catch {}
  }, []);

  useEffect(() => {
    fetchJobs();
    pollRef.current = setInterval(fetchJobs, 5000);
    return () => clearInterval(pollRef.current);
  }, [fetchJobs]);

  // SSE for running jobs
  const jobDetailRef = useRef(null);
  useEffect(() => {
    const runningJob = jobs.find(j => j.status === 'running');
    if (!runningJob) {
      if (sseRef.current) { sseRef.current.close(); sseRef.current = null; }
      setActiveJob(prev => prev?.status === 'running' ? null : prev);
      return;
    }
    if (sseRef.current && activeJob?.job_id === runningJob.job_id) return;
    if (sseRef.current) sseRef.current.close();

    const connect = async () => {
      let detail = {};
      try {
        const res = await fetch(`/nimog/api/jobs/${runningJob.job_id}`);
        if (res.ok) detail = await res.json();
      } catch {}
      jobDetailRef.current = detail;
      const sampleName = detail.sample_names?.[0] || detail.bam_path?.split('/').pop()?.replace(/\.(bam|cram)$/, '') || '';

      const es = new EventSource(`/nimog/api/jobs/${runningJob.job_id}/stream`);
      sseRef.current = es;
      es.onmessage = (e) => {
        try {
          const data = JSON.parse(e.data);
          setActiveJob({ ...data, job_id: runningJob.job_id, sample_name: sampleName, use_gpu: detail.use_gpu !== false });
          if (data.status === 'completed' || data.status === 'failed') fetchJobs();
        } catch {}
      };
      es.onerror = () => { es.close(); sseRef.current = null; setTimeout(fetchJobs, 2000); };
    };
    connect();
    return () => { if (sseRef.current) { sseRef.current.close(); sseRef.current = null; } };
  }, [jobs, activeJob?.job_id, fetchJobs]);

  const handleSync = async () => {
    setSyncing(true);
    try {
      const res = await fetch('/genomics/api/vcfs/sync-nimog', { method: 'POST' });
      const data = await res.json();
      showToast(data.synced ? `Synced ${data.synced} VCF(s)` : 'No new VCFs to sync');
    } catch { showToast('Sync failed', 'error'); }
    setSyncing(false);
  };

  const completedJobs = jobs.filter(j => j.status === 'completed');
  const failedJobs = jobs.filter(j => j.status === 'failed');
  const historyJobs = [...completedJobs, ...failedJobs].sort((a, b) => (b.completed_at || 0) - (a.completed_at || 0));

  return (
    <div>
      {/* Active / recent job */}
      {activeJob && activeJob.status === 'running' ? (
        <ActiveJobCard job={activeJob} />
      ) : completedJobs.length > 0 ? (
        <ActiveJobCard job={{
          ...completedJobs[0],
          job_id: completedJobs[0].job_id,
          sample_name: completedJobs[0].sample_names?.[0] || '',
          elapsed: completedJobs[0].completed_at && completedJobs[0].started_at
            ? completedJobs[0].completed_at - completedJobs[0].started_at : null,
        }} />
      ) : (
        <div style={{ padding: '20px 0', textAlign: 'center', color: '#6e7681', fontSize: 13 }}>
          No conversion jobs yet. Click "New Conversion" below to convert alignment files to VCF.
        </div>
      )}

      {/* Actions */}
      <div style={{ display: 'flex', gap: 8, marginTop: 8, flexWrap: 'wrap', alignItems: 'center' }}>
        <button style={BTN_BASE} onClick={() => setShowNimog(!showNimog)}>
          <ConvertIcon />
          {showNimog ? 'Hide Converter' : 'New Conversion'}
        </button>
        <button style={BTN_BASE} onClick={handleSync} disabled={syncing}>
          {syncing ? 'Syncing...' : 'Sync VCFs'}
        </button>
        {historyJobs.length > 0 && (
          <button style={BTN_BASE} onClick={() => setShowHistory(!showHistory)}>
            {showHistory ? 'Hide' : 'Show'} History ({historyJobs.length})
          </button>
        )}
        {gpuAvailable !== null && (
          <span style={{
            marginLeft: 'auto', fontSize: 11, padding: '3px 8px', borderRadius: 4,
            background: gpuAvailable ? '#3fb95015' : '#21262d',
            color: gpuAvailable ? '#3fb950' : '#6e7681',
            border: `1px solid ${gpuAvailable ? '#3fb95033' : '#30363d'}`,
          }}>
            {gpuAvailable ? '\u26A1 GPU available — DeepVariant accelerated' : '\uD83D\uDCBB No GPU — bcftools or DeepVariant CPU only'}
          </span>
        )}
      </div>

      {/* Nimog iframe */}
      {showNimog && (
        <div style={{ marginTop: 10, border: '1px solid #30363d', borderRadius: 8, overflow: 'hidden' }}>
          <iframe
            src="/nimog/"
            title="BAM/CRAM to VCF Converter"
            style={{ width: '100%', height: 700, border: 'none', background: '#0d1117' }}
          />
        </div>
      )}

      {/* Job history */}
      {showHistory && historyJobs.length > 0 && (
        <div style={{ marginTop: 10, maxHeight: 300, overflow: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ color: '#8b949e', borderBottom: '1px solid #21262d', textAlign: 'left' }}>
                <th style={{ padding: '6px 8px' }}>ID</th>
                <th style={{ padding: '6px 8px' }}>Sample</th>
                <th style={{ padding: '6px 8px' }}>Mode</th>
                <th style={{ padding: '6px 8px' }}>Status</th>
                <th style={{ padding: '6px 8px' }}>Duration</th>
              </tr>
            </thead>
            <tbody>
              {historyJobs.map(j => (
                <tr key={j.job_id} style={{ borderBottom: '1px solid #161b22', color: '#c9d1d9', cursor: 'pointer' }}
                  onClick={() => setActiveJob({
                    ...j, sample_name: j.sample_names?.[0] || '',
                    elapsed: j.completed_at && j.started_at ? j.completed_at - j.started_at : null,
                  })}>
                  <td style={{ padding: '6px 8px', fontFamily: 'monospace', color: '#8b949e' }}>{j.job_id?.slice(0, 8)}</td>
                  <td style={{ padding: '6px 8px' }}>{j.sample_names?.join(', ') || '—'}</td>
                  <td style={{ padding: '6px 8px' }}>{j.mode === 'deepvariant' ? 'DV' : 'bcftools'}</td>
                  <td style={{ padding: '6px 8px' }}>
                    <span style={{ color: j.status === 'completed' ? '#3fb950' : '#f85149' }}>{j.status}</span>
                  </td>
                  <td style={{ padding: '6px 8px' }}>{j.completed_at && j.started_at ? pipFormatElapsed(j.completed_at - j.started_at) : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

/* ══════════════════════════════════════════════════════════════
   INLINE EDITABLE NAME
   ══════════════════════════════════════════════════════════════ */

function EditableName({ name, path, onRename }) {
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(name);
  const inputRef = useRef(null);

  useEffect(() => { setValue(name); }, [name]);
  useEffect(() => { if (editing && inputRef.current) inputRef.current.focus(); }, [editing]);

  const [displayName, setDisplayName] = useState(name);
  useEffect(() => { setDisplayName(name); }, [name]);

  const save = async () => {
    const trimmed = value.trim();
    if (!trimmed || trimmed === displayName) { setEditing(false); return; }
    try {
      await fetch('/genomics/api/files/rename', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path, new_name: trimmed }),
      });
      setDisplayName(trimmed); // Update immediately
      if (onRename) onRename(trimmed);
    } catch {}
    setEditing(false);
  };

  if (editing) {
    return (
      <input ref={inputRef} value={value} onChange={e => setValue(e.target.value)}
        onBlur={save} onKeyDown={e => { if (e.key === 'Enter') save(); if (e.key === 'Escape') setEditing(false); }}
        style={{ background: '#0d1117', border: '1px solid #58a6ff', borderRadius: 4, color: '#e6edf3', padding: '2px 6px', fontSize: 14, fontWeight: 600, width: '100%', maxWidth: 300 }}
      />
    );
  }

  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
      <span style={{ fontWeight: 600, fontSize: 14, color: '#e6edf3' }}>{displayName}</span>
      <span onClick={() => setEditing(true)} title="Edit name"
        style={{ cursor: 'pointer', color: '#484f58', fontSize: 12, padding: '0 4px' }}>&#9998;</span>
    </span>
  );
}

/* ══════════════════════════════════════════════════════════════
   HELPERS
   ══════════════════════════════════════════════════════════════ */

function hexAlpha(hex, alpha) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function fmtBytes(b) {
  if (b == null || b === 0) return '--';
  if (b >= 1e9) return (b / 1e9).toFixed(2) + ' GB';
  if (b >= 1e6) return (b / 1e6).toFixed(1) + ' MB';
  if (b >= 1e3) return (b / 1e3).toFixed(1) + ' KB';
  return b + ' B';
}

function fmtStorageSize(bytes) {
  if (bytes == null) return '--';
  if (bytes >= 1e12) return (bytes / 1e12).toFixed(1) + ' TB';
  if (bytes >= 1e9) return (bytes / 1e9).toFixed(1) + ' GB';
  return (bytes / 1e6).toFixed(0) + ' MB';
}

function fmtNumber(n) {
  if (n == null) return '--';
  return Number(n).toLocaleString();
}

function fmtDate(ts) {
  if (!ts) return '--';
  try {
    const d = new Date(typeof ts === 'number' ? ts * 1000 : ts);
    return d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
  } catch { return '--'; }
}

function progressColor(pct) {
  if (pct >= 90) return '#f85149';
  if (pct >= 70) return '#d29922';
  return '#3fb950';
}

function badgeStyle(color) {
  return {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 4,
    padding: '2px 8px',
    borderRadius: 12,
    fontSize: 12,
    fontWeight: 500,
    lineHeight: 1.6,
    background: hexAlpha(color, 0.15),
    color: color,
    border: `1px solid ${hexAlpha(color, 0.3)}`,
    whiteSpace: 'nowrap',
  };
}

const CARD_STYLE = {
  background: '#161b22',
  border: '1px solid #30363d',
  borderRadius: 8,
  padding: 14,
  transition: 'border-color 0.2s',
};

const BTN_BASE = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: 6,
  padding: '5px 12px',
  borderRadius: 6,
  border: '1px solid #30363d',
  background: '#21262d',
  color: '#c9d1d9',
  fontSize: 12,
  fontWeight: 500,
  cursor: 'pointer',
  transition: 'background 0.15s, border-color 0.15s',
};

const BTN_PRIMARY = {
  ...BTN_BASE,
  background: '#238636',
  borderColor: '#2ea043',
  color: '#fff',
};

const LABEL_STYLE = {
  fontSize: 13,
  fontWeight: 500,
  color: '#c9d1d9',
  marginBottom: 4,
  display: 'block',
};

const INPUT_STYLE = {
  width: '100%',
  padding: '6px 10px',
  borderRadius: 6,
  border: '1px solid #30363d',
  background: '#0d1117',
  color: '#c9d1d9',
  fontSize: 13,
  outline: 'none',
  boxSizing: 'border-box',
};

const SELECT_STYLE = {
  ...INPUT_STYLE,
  appearance: 'auto',
};


/* ══════════════════════════════════════════════════════════════
   SVG ICONS (inline, no deps)
   ══════════════════════════════════════════════════════════════ */

function ChevronIcon({ open }) {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
      style={{ transition: 'transform 0.2s', transform: open ? 'rotate(90deg)' : 'rotate(0deg)', flexShrink: 0 }}>
      <path d="M9 18l6-6-6-6" />
    </svg>
  );
}

function BamIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#d29922" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
      <polyline points="14 2 14 8 20 8" />
      <line x1="9" y1="13" x2="15" y2="13" />
      <line x1="9" y1="17" x2="15" y2="17" />
    </svg>
  );
}

function FastqIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#bc8cff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
      <polyline points="14 2 14 8 20 8" />
      <path d="M8 13h2m4 0h2M8 17h2m4 0h2" />
    </svg>
  );
}

function VcfIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#3fb950" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
      <polyline points="14 2 14 8 20 8" />
      <path d="M9 15l2 2 4-4" />
    </svg>
  );
}

function GvcfIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
      <polyline points="14 2 14 8 20 8" />
      <circle cx="12" cy="15" r="2" />
      <path d="M12 13v-2m0 6v2" />
    </svg>
  );
}

function UploadIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#8b949e" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4" />
      <polyline points="17 8 12 3 7 8" />
      <line x1="12" y1="3" x2="12" y2="15" />
    </svg>
  );
}

function ConvertIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#d29922" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="16 3 21 3 21 8" />
      <line x1="4" y1="20" x2="21" y2="3" />
      <polyline points="21 16 21 21 16 21" />
      <line x1="15" y1="15" x2="21" y2="21" />
      <line x1="4" y1="4" x2="9" y2="9" />
    </svg>
  );
}

function AlignIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#bc8cff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="4" y1="6" x2="20" y2="6" />
      <line x1="4" y1="12" x2="14" y2="12" />
      <line x1="4" y1="18" x2="18" y2="18" />
    </svg>
  );
}

function StorageIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#8b949e" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
      <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
    </svg>
  );
}

function RefreshIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="23 4 23 10 17 10" />
      <path d="M20.49 15a9 9 0 11-2.12-9.36L23 10" />
    </svg>
  );
}

function InfoIcon() {
  return (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ opacity: 0.6 }}>
      <circle cx="12" cy="12" r="10" />
      <line x1="12" y1="16" x2="12" y2="12" />
      <line x1="12" y1="8" x2="12.01" y2="8" />
    </svg>
  );
}


/* ══════════════════════════════════════════════════════════════
   COLLAPSIBLE SECTION (reusable)
   ══════════════════════════════════════════════════════════════ */

function CollapsibleSection({ title, count, tooltip, icon, color, defaultOpen = false, children }) {
  const [open, setOpen] = useState(defaultOpen);
  const [showTooltip, setShowTooltip] = useState(false);
  const tooltipTimer = useRef(null);
  const tipRef = useRef(null);

  function handleTipEnter() {
    tooltipTimer.current = setTimeout(() => setShowTooltip(true), 200);
  }
  function handleTipLeave() {
    clearTimeout(tooltipTimer.current);
    setShowTooltip(false);
  }

  return (
    <div style={{ marginBottom: 16 }}>
      {/* Header */}
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          width: '100%',
          textAlign: 'left',
          background: '#161b22',
          border: '1px solid #30363d',
          borderRadius: open ? '8px 8px 0 0' : 8,
          padding: '10px 14px',
          cursor: 'pointer',
          color: '#e6edf3',
          fontSize: 15,
          fontWeight: 600,
          transition: 'background 0.15s, border-radius 0.2s',
        }}
        onMouseEnter={e => { e.currentTarget.style.background = '#1c2128'; }}
        onMouseLeave={e => { e.currentTarget.style.background = '#161b22'; }}
      >
        <ChevronIcon open={open} />
        {icon}
        <span style={{ color }}>{title}</span>
        {count != null && (
          <span style={{
            ...badgeStyle(color),
            fontSize: 11,
            padding: '1px 7px',
            marginLeft: 2,
          }}>
            {count}
          </span>
        )}

        {/* Tooltip trigger */}
        {tooltip && (
          <span
            ref={tipRef}
            onMouseEnter={handleTipEnter}
            onMouseLeave={handleTipLeave}
            onClick={e => e.stopPropagation()}
            style={{ position: 'relative', display: 'inline-flex', alignItems: 'center', marginLeft: 4 }}
          >
            <InfoIcon />
            {showTooltip && (
              <div style={{
                position: 'absolute',
                left: 20,
                top: -8,
                zIndex: 1000,
                background: '#1c2a3a',
                border: '1px solid #58a6ff',
                borderRadius: 8,
                padding: '10px 14px',
                maxWidth: 320,
                fontSize: 12,
                lineHeight: 1.5,
                color: '#c9d1d9',
                boxShadow: '0 4px 16px rgba(0,0,0,0.4)',
                whiteSpace: 'normal',
                fontWeight: 400,
              }}>
                {tooltip}
              </div>
            )}
          </span>
        )}
      </button>

      {/* Body */}
      {open && (
        <div style={{
          border: '1px solid #30363d',
          borderTop: 'none',
          borderRadius: '0 0 8px 8px',
          padding: 12,
          background: '#0d1117',
        }}>
          {children}
        </div>
      )}
    </div>
  );
}


/* ══════════════════════════════════════════════════════════════
   MODAL (reusable)
   ══════════════════════════════════════════════════════════════ */

function Modal({ open, onClose, title, children }) {
  if (!open) return null;
  return (
    <div
      onClick={e => { if (e.target === e.currentTarget) onClose(); }}
      style={{
        position: 'fixed', inset: 0, zIndex: 9999,
        background: 'rgba(0,0,0,0.6)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        padding: 20,
      }}
    >
      <div style={{
        background: '#161b22', border: '1px solid #30363d', borderRadius: 12,
        maxWidth: 700, width: '100%', maxHeight: '80vh',
        display: 'flex', flexDirection: 'column',
      }}>
        <div style={{
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
          padding: '14px 18px', borderBottom: '1px solid #30363d',
        }}>
          <h3 style={{ margin: 0, fontSize: 16, color: '#e6edf3' }}>{title}</h3>
          <button onClick={onClose} style={{
            background: 'none', border: 'none', color: '#8b949e', cursor: 'pointer',
            fontSize: 20, lineHeight: 1, padding: '2px 6px',
          }}>&times;</button>
        </div>
        <div style={{ padding: 18, overflow: 'auto', flex: 1 }}>
          {children}
        </div>
      </div>
    </div>
  );
}


/* ══════════════════════════════════════════════════════════════
   TOAST
   ══════════════════════════════════════════════════════════════ */

function Toast({ toast }) {
  if (!toast) return null;
  const isError = toast.type === 'error';
  return (
    <div style={{
      position: 'fixed',
      bottom: 24,
      right: 24,
      zIndex: 10000,
      background: isError ? '#3d1518' : '#0f2e16',
      border: `1px solid ${isError ? '#f8514980' : '#3fb95080'}`,
      color: isError ? '#f85149' : '#3fb950',
      padding: '10px 18px',
      borderRadius: 8,
      fontSize: 14,
      fontWeight: 500,
      boxShadow: '0 4px 16px rgba(0,0,0,0.4)',
      maxWidth: 400,
    }}>
      {toast.msg}
    </div>
  );
}


/* ══════════════════════════════════════════════════════════════
   CARD GRID WRAPPER
   ══════════════════════════════════════════════════════════════ */

function CardGrid({ children }) {
  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
      gap: 10,
    }}>
      {children}
    </div>
  );
}


/* ══════════════════════════════════════════════════════════════
   BAM FILE CARD
   ══════════════════════════════════════════════════════════════ */

// Shared file action handlers
function useFileActions(file, showToast, loadFiles) {
  const [deleting, setDeleting] = useState(false);
  const [duplicating, setDuplicating] = useState(false);

  const isOnFast = file.path?.startsWith('/scratch');
  const isOnPersistent = file.path?.startsWith('/data');

  async function handleDelete() {
    if (!window.confirm(`Delete "${file.sample_name || file.path.split('/').pop()}"?\n\nThis will permanently remove the file from disk. This cannot be undone.`)) return;
    setDeleting(true);
    try {
      await fetch('/genomics/api/files/delete', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: file.path }),
      });
      showToast('File deleted', 'success');
      if (loadFiles) loadFiles();
    } catch (err) { showToast('Delete failed: ' + err.message, 'error'); }
    finally { setDeleting(false); }
  }

  async function handleDuplicate(target) {
    const label = target === 'persistent' ? 'persistent disk (/data)' : 'fast SSD (/scratch)';
    setDuplicating(true);
    try {
      const res = await fetch('/genomics/api/files/duplicate', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: file.path, target }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || 'Failed');
      showToast(`Copied to ${label}: ${data.destination?.split('/').pop()}`, 'success');
      if (loadFiles) loadFiles();
    } catch (err) { showToast('Copy failed: ' + err.message, 'error'); }
    finally { setDuplicating(false); }
  }

  return { deleting, duplicating, isOnFast, isOnPersistent, handleDelete, handleDuplicate };
}

function FileActionButtons({ actions, style }) {
  const { deleting, duplicating, isOnFast, isOnPersistent, handleDelete, handleDuplicate } = actions;
  return (
    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', ...style }}>
      {isOnFast && (
        <button style={{ ...BTN_BASE, color: '#58a6ff', fontSize: 11 }} onClick={() => handleDuplicate('persistent')} disabled={duplicating}>
          {duplicating ? '...' : 'Copy to /data'}
        </button>
      )}
      {isOnPersistent && (
        <button style={{ ...BTN_BASE, color: '#58a6ff', fontSize: 11 }} onClick={() => handleDuplicate('fast')} disabled={duplicating}>
          {duplicating ? '...' : 'Copy to /scratch'}
        </button>
      )}
      <button style={{ ...BTN_BASE, color: '#f85149', fontSize: 11 }} onClick={handleDelete} disabled={deleting}>
        {deleting ? '...' : 'Delete'}
      </button>
    </div>
  );
}

function BamCard({ file, showToast, loadFiles }) {
  const actions = useFileActions(file, showToast, loadFiles);
  const [inspecting, setInspecting] = useState(false);
  const [inspectData, setInspectData] = useState(null);
  const [validating, setValidating] = useState(false);

  async function handleInspect() {
    setInspecting(true);
    try {
      const res = await fetch('/genomics/api/files/inspect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: file.path }),
      });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      setInspectData(data);
    } catch (err) {
      showToast('Inspect failed: ' + err.message, 'error');
    } finally {
      setInspecting(false);
    }
  }

  async function handleValidate() {
    setValidating(true);
    try {
      const res = await fetch('/genomics/api/files/validate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: file.path }),
      });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      if (data.valid) {
        showToast('Valid \u2713');
      } else {
        showToast('Error: ' + (data.error || 'Validation failed'), 'error');
      }
    } catch (err) {
      showToast('Validate failed: ' + err.message, 'error');
    } finally {
      setValidating(false);
    }
  }

  const filename = file.path ? file.path.split('/').pop() : 'Unknown';

  return (
    <>
      <div
        style={CARD_STYLE}
        onMouseEnter={e => { e.currentTarget.style.borderColor = '#484f58'; }}
        onMouseLeave={e => { e.currentTarget.style.borderColor = '#30363d'; }}
      >
        {/* Filename — editable */}
        <div style={{ marginBottom: 6 }}>
          <EditableName name={file.sample_name || filename} path={file.path} onRename={() => loadFiles()} />
        </div>

        {/* Badges row */}
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5, marginBottom: 8 }}>
          {file.sample_name && <span style={badgeStyle('#d29922')}>{file.sample_name}</span>}
          <span style={{ fontSize: 12, color: '#8b949e' }}>{fmtBytes(file.file_size_bytes)}</span>
          {file.mtime && <span style={{ fontSize: 12, color: '#6e7681' }}>{fmtDate(file.mtime)}</span>}
        </div>

        {/* Stats row */}
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5, marginBottom: 8 }}>
          {file.format_details?.read_count != null && (
            <span style={{ fontSize: 12, color: '#8b949e' }}>Reads: {fmtNumber(file.format_details.read_count)}</span>
          )}
          {file.format_details?.mapped_pct != null && (
            <span style={{ fontSize: 12, color: '#8b949e' }}>Mapped: {file.format_details.mapped_pct}%</span>
          )}
          <span style={badgeStyle(file.has_index ? '#3fb950' : '#d29922')}>
            {file.has_index ? `Indexed (${file.file_type === 'cram' ? '.crai' : '.bai'})` : 'No index'}
          </span>
        </div>

        {/* Path */}
        <div style={{ fontSize: 11, fontFamily: 'monospace', color: '#484f58', wordBreak: 'break-all', lineHeight: 1.4, marginBottom: 10 }}>
          {file.path}
        </div>

        {/* Buttons */}
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          <button style={BTN_BASE} onClick={handleInspect} disabled={inspecting}>
            {inspecting ? '...' : 'Inspect'}
          </button>
          <button style={BTN_BASE} onClick={handleValidate} disabled={validating}>
            {validating ? '...' : 'Validate'}
          </button>
          <FileActionButtons actions={actions} />
        </div>
      </div>

      {/* Inspect modal */}
      <Modal open={!!inspectData} onClose={() => setInspectData(null)} title={`Inspect: ${filename}`}>
        <pre style={{
          background: '#0d1117', border: '1px solid #30363d', borderRadius: 8,
          padding: 14, fontSize: 12, color: '#c9d1d9', fontFamily: 'monospace',
          whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: '60vh', overflow: 'auto',
          margin: 0,
        }}>
          {typeof inspectData === 'string' ? inspectData : JSON.stringify(inspectData, null, 2)}
        </pre>
      </Modal>
    </>
  );
}


/* ══════════════════════════════════════════════════════════════
   FASTQ FILE CARD
   ══════════════════════════════════════════════════════════════ */

function FastqCard({ file, showToast, loadFiles }) {
  const actions = useFileActions(file, showToast, loadFiles);
  const filename = file.path ? file.path.split('/').pop() : 'Unknown';
  const hasPair = !!file.format_details?.paired_path;
  const pairedFile = hasPair ? file.format_details.paired_path.split('/').pop() : null;

  // Combine sizes
  let combinedSize = file.file_size_bytes || 0;
  if (file.format_details?.paired_size) {
    combinedSize += file.format_details.paired_size;
  }

  const [showAlign, setShowAlign] = useState(false);
  const [alignRef, setAlignRef] = useState('/data/reference/GRCh38.fa');
  const [aligner, setAligner] = useState('bwa');
  const [threads, setThreads] = useState(20);
  const [aligning, setAligning] = useState(false);

  async function handleAlign() {
    setAligning(true);
    try {
      const res = await fetch('/genomics/api/files/convert/fastq-to-bam', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          fastq_r1: file.path,
          fastq_r2: file.format_details?.paired_path || null,
          reference: alignRef,
          aligner: aligner,
          threads: threads,
        }),
      });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      showToast('Alignment started! Job ID: ' + (data.job_id || 'unknown'));
      setShowAlign(false);
    } catch (err) {
      showToast('Alignment failed: ' + err.message, 'error');
    } finally {
      setAligning(false);
    }
  }

  return (
    <div
      style={CARD_STYLE}
      onMouseEnter={e => { e.currentTarget.style.borderColor = '#484f58'; }}
      onMouseLeave={e => { e.currentTarget.style.borderColor = '#30363d'; }}
    >
      {/* Sample name — editable */}
      <div style={{ marginBottom: 6 }}>
        <EditableName name={file.sample_name || filename} path={file.path} onRename={() => loadFiles()} />
      </div>

      {/* Badges */}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5, marginBottom: 8 }}>
        {hasPair ? (
          <span style={badgeStyle('#3fb950')}>Paired (R1 + R2)</span>
        ) : (
          <span style={badgeStyle('#d29922')}>Single-end</span>
        )}
        <span style={{ fontSize: 12, color: '#8b949e' }}>{fmtBytes(combinedSize)}</span>
        {file.format_details?.read_count != null && (
          <span style={{ fontSize: 12, color: '#8b949e' }}>~{fmtNumber(file.format_details.read_count)} reads</span>
        )}
      </div>

      {/* File names */}
      <div style={{ fontSize: 11, fontFamily: 'monospace', color: '#484f58', wordBreak: 'break-all', lineHeight: 1.5, marginBottom: 8 }}>
        <div>R1: {filename}</div>
        {pairedFile && <div>R2: {pairedFile}</div>}
      </div>

      {/* Action buttons */}
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
        <button
          style={{ ...BTN_BASE, color: '#bc8cff', borderColor: hexAlpha('#bc8cff', 0.4) }}
          onClick={() => setShowAlign(o => !o)}
        >
          {showAlign ? 'Close' : 'Align to BAM \u2192'}
        </button>
        <FileActionButtons actions={actions} />
      </div>

      {/* Inline align form */}
      {showAlign && (
        <div style={{ marginTop: 10, padding: 10, background: '#0d1117', borderRadius: 8, border: '1px solid #30363d' }}>
          <div style={{ marginBottom: 8 }}>
            <label style={LABEL_STYLE}>Reference genome</label>
            <input style={INPUT_STYLE} value={alignRef} onChange={e => setAlignRef(e.target.value)} />
          </div>
          <div style={{ marginBottom: 8 }}>
            <label style={LABEL_STYLE}>Aligner</label>
            <select style={SELECT_STYLE} value={aligner} onChange={e => setAligner(e.target.value)}>
              <option value="bwa">bwa mem (recommended)</option>
              <option value="minimap2">minimap2</option>
            </select>
          </div>
          <div style={{ marginBottom: 10 }}>
            <label style={LABEL_STYLE}>Threads: {threads}</label>
            <input type="range" min={1} max={44} value={threads}
              onChange={e => setThreads(Number(e.target.value))}
              style={{ width: '100%', accentColor: '#bc8cff' }}
            />
          </div>
          <button style={BTN_PRIMARY} onClick={handleAlign} disabled={aligning}>
            {aligning ? 'Starting...' : 'Start Alignment'}
          </button>
        </div>
      )}
    </div>
  );
}


/* ══════════════════════════════════════════════════════════════
   VCF / gVCF FILE CARD
   ══════════════════════════════════════════════════════════════ */

function VcfCard({ file, color, typeLabel, showToast, loadFiles }) {
  const actions = useFileActions(file, showToast, loadFiles);
  const [inspecting, setInspecting] = useState(false);
  const [inspectData, setInspectData] = useState(null);
  const [validating, setValidating] = useState(false);

  const filename = file.path ? file.path.split('/').pop() : 'Unknown';

  const qcColors = {
    passed: '#3fb950',
    issues: '#d29922',
    failed: '#f85149',
    unknown: '#8b949e',
  };

  async function handleInspect() {
    setInspecting(true);
    try {
      const res = await fetch('/genomics/api/files/inspect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: file.path }),
      });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      setInspectData(data);
    } catch (err) {
      showToast('Inspect failed: ' + err.message, 'error');
    } finally {
      setInspecting(false);
    }
  }

  async function handleValidate() {
    setValidating(true);
    try {
      const res = await fetch('/genomics/api/files/validate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: file.path }),
      });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      if (data.valid) {
        showToast('Valid \u2713');
      } else {
        showToast('Error: ' + (data.error || 'Validation failed'), 'error');
      }
    } catch (err) {
      showToast('Validate failed: ' + err.message, 'error');
    } finally {
      setValidating(false);
    }
  }

  return (
    <>
      <div
        style={CARD_STYLE}
        onMouseEnter={e => { e.currentTarget.style.borderColor = '#484f58'; }}
        onMouseLeave={e => { e.currentTarget.style.borderColor = '#30363d'; }}
      >
        {/* Filename — editable */}
        <div style={{ marginBottom: 6 }}>
          <EditableName name={file.sample_name || filename} path={file.path} onRename={() => loadFiles()} />
        </div>

        {/* Badges row 1 */}
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5, marginBottom: 6 }}>
          {file.sample_name && <span style={badgeStyle(color)}>{file.sample_name}</span>}
          {file.genome_build && <span style={badgeStyle('#bc8cff')}>{file.genome_build}</span>}
          {file.qc_status && (
            <span style={badgeStyle(qcColors[file.qc_status] || '#8b949e')}>QC: {file.qc_status}</span>
          )}
          {file.has_index != null && (
            <span style={badgeStyle(file.has_index ? '#3fb950' : '#d29922')}>
              {file.has_index ? 'Indexed (.tbi)' : 'No index'}
            </span>
          )}
        </div>

        {/* Stats row */}
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 6, fontSize: 12, color: '#8b949e' }}>
          {file.format_details?.variant_count != null && (
            <span>Variants: {fmtNumber(file.format_details.variant_count)}</span>
          )}
          {file.format_details?.snp_count != null && (
            <span>SNPs: {fmtNumber(file.format_details.snp_count)}</span>
          )}
          {file.format_details?.indel_count != null && (
            <span>Indels: {fmtNumber(file.format_details.indel_count)}</span>
          )}
          {file.format_details?.ti_tv_ratio != null && (
            <span>Ti/Tv: {file.format_details.ti_tv_ratio}</span>
          )}
          {file.format_details?.caller && (
            <span>Caller: {file.format_details.caller}</span>
          )}
          <span>{fmtBytes(file.file_size_bytes)}</span>
        </div>

        {/* Path */}
        <div style={{ fontSize: 11, fontFamily: 'monospace', color: '#484f58', wordBreak: 'break-all', lineHeight: 1.4, marginBottom: 10 }}>
          {file.path}
        </div>

        {/* Buttons */}
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          <button style={BTN_BASE} onClick={handleInspect} disabled={inspecting}>
            {inspecting ? '...' : 'Inspect'}
          </button>
          <button style={BTN_BASE} onClick={handleValidate} disabled={validating}>
            {validating ? '...' : 'Validate'}
          </button>
          <FileActionButtons actions={actions} />
        </div>
      </div>

      {/* Inspect modal */}
      <Modal open={!!inspectData} onClose={() => setInspectData(null)} title={`Inspect: ${filename}`}>
        <pre style={{
          background: '#0d1117', border: '1px solid #30363d', borderRadius: 8,
          padding: 14, fontSize: 12, color: '#c9d1d9', fontFamily: 'monospace',
          whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: '60vh', overflow: 'auto',
          margin: 0,
        }}>
          {typeof inspectData === 'string' ? inspectData : JSON.stringify(inspectData, null, 2)}
        </pre>
      </Modal>
    </>
  );
}


/* ══════════════════════════════════════════════════════════════
   UPLOAD SECTION
   ══════════════════════════════════════════════════════════════ */

function UploadSection({ onUploadComplete, showToast }) {
  const [dragOver, setDragOver] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadResult, setUploadResult] = useState(null);
  const [urlValue, setUrlValue] = useState('');
  const [downloading, setDownloading] = useState(false);
  const fileInputRef = useRef(null);

  function handleDragOver(e) {
    e.preventDefault();
    e.stopPropagation();
    setDragOver(true);
  }

  function handleDragLeave(e) {
    e.preventDefault();
    e.stopPropagation();
    setDragOver(false);
  }

  function handleDrop(e) {
    e.preventDefault();
    e.stopPropagation();
    setDragOver(false);
    const files = e.dataTransfer.files;
    if (files.length > 0) {
      uploadFile(files[0]);
    }
  }

  function handleFileSelect(e) {
    const files = e.target.files;
    if (files.length > 0) {
      uploadFile(files[0]);
    }
  }

  function uploadFile(file) {
    setUploading(true);
    setUploadProgress(0);
    setUploadResult(null);

    const formData = new FormData();
    formData.append('file', file);

    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/genomics/api/files/upload');

    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) {
        setUploadProgress(Math.round((e.loaded / e.total) * 100));
      }
    };

    xhr.onload = () => {
      setUploading(false);
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          const data = JSON.parse(xhr.responseText);
          setUploadResult(data);
          showToast('Upload complete: ' + (data.filename || file.name));
          if (onUploadComplete) onUploadComplete();
        } catch {
          showToast('Upload complete');
          if (onUploadComplete) onUploadComplete();
        }
      } else {
        showToast('Upload failed: HTTP ' + xhr.status, 'error');
      }
    };

    xhr.onerror = () => {
      setUploading(false);
      showToast('Upload failed: network error', 'error');
    };

    xhr.send(formData);
  }

  async function handleUrlDownload() {
    if (!urlValue.trim()) return;
    setDownloading(true);
    setUploadResult(null);
    try {
      const formData = new FormData();
      formData.append('url', urlValue.trim());
      const res = await fetch('/genomics/api/files/upload', {
        method: 'POST',
        body: formData,
      });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      setUploadResult(data);
      showToast('Download complete: ' + (data.filename || urlValue.trim().split('/').pop()));
      setUrlValue('');
      if (onUploadComplete) onUploadComplete();
    } catch (err) {
      showToast('Download failed: ' + err.message, 'error');
    } finally {
      setDownloading(false);
    }
  }

  const TYPE_LABELS = {
    bam: 'BAM Files',
    fastq: 'FASTQ Files',
    vcf: 'VCF Files',
    gvcf: 'gVCF Files',
  };

  return (
    <div>
      {/* Drop zone */}
      <div
        onClick={() => fileInputRef.current?.click()}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        style={{
          border: `2px dashed ${dragOver ? '#58a6ff' : '#30363d'}`,
          borderRadius: 8,
          padding: '32px 20px',
          textAlign: 'center',
          cursor: 'pointer',
          transition: 'border-color 0.2s, background 0.2s',
          background: dragOver ? hexAlpha('#58a6ff', 0.05) : 'transparent',
          marginBottom: 12,
        }}
      >
        <UploadIcon />
        <div style={{ fontSize: 14, color: '#c9d1d9', marginTop: 8, fontWeight: 500 }}>
          Drop files here or click to browse
        </div>
        <div style={{ fontSize: 12, color: '#6e7681', marginTop: 4 }}>
          Supported: BAM, FASTQ (.fastq.gz, .fq.gz), VCF (.vcf.gz), gVCF (.g.vcf.gz)
        </div>
      </div>
      <input
        ref={fileInputRef}
        type="file"
        style={{ display: 'none' }}
        onChange={handleFileSelect}
        accept=".bam,.cram,.fastq.gz,.fq.gz,.vcf,.vcf.gz,.g.vcf.gz"
      />

      {/* Upload progress */}
      {uploading && (
        <div style={{ marginBottom: 12 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
            <span style={{ fontSize: 13, color: '#c9d1d9' }}>Uploading...</span>
            <span style={{ fontSize: 13, color: '#8b949e' }}>{uploadProgress}%</span>
          </div>
          <div style={{ height: 6, background: '#21262d', borderRadius: 3, overflow: 'hidden' }}>
            <div style={{
              width: `${uploadProgress}%`,
              height: '100%',
              background: '#58a6ff',
              borderRadius: 3,
              transition: 'width 0.2s',
            }} />
          </div>
        </div>
      )}

      {/* Upload result */}
      {uploadResult && (
        <div style={{
          background: '#0f2e16', border: '1px solid #3fb95040', borderRadius: 8,
          padding: '10px 14px', marginBottom: 12, fontSize: 13, color: '#3fb950',
        }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Upload complete</div>
          <div style={{ color: '#c9d1d9' }}>
            File: <span style={{ fontFamily: 'monospace' }}>{uploadResult.filename}</span>
          </div>
          <div style={{ color: '#8b949e', fontSize: 12 }}>
            Size: {fmtBytes(uploadResult.file_size_bytes)}
            {uploadResult.file_type && (
              <span> &middot; Detected as: {TYPE_LABELS[uploadResult.file_type] || uploadResult.file_type}</span>
            )}
          </div>
        </div>
      )}

      {/* URL download */}
      <div style={{ display: 'flex', gap: 8, alignItems: 'flex-end' }}>
        <div style={{ flex: 1 }}>
          <label style={LABEL_STYLE}>Download from URL</label>
          <input
            style={INPUT_STYLE}
            type="text"
            placeholder="https://example.com/sample.vcf.gz"
            value={urlValue}
            onChange={e => setUrlValue(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter') handleUrlDownload(); }}
          />
        </div>
        <button style={BTN_BASE} onClick={handleUrlDownload} disabled={downloading || !urlValue.trim()}>
          {downloading ? 'Downloading...' : 'Download'}
        </button>
      </div>
    </div>
  );
}


/* ══════════════════════════════════════════════════════════════
   CONVERT BAM -> VCF SECTION
   ══════════════════════════════════════════════════════════════ */

function ConvertFastqToBamSection({ fastqFiles, showToast }) {
  const [selectedFastq, setSelectedFastq] = useState('');
  const [reference, setReference] = useState('/data/reference/GRCh38.fa');
  const [aligner, setAligner] = useState('bwa');
  const [threads, setThreads] = useState(20);
  const [running, setRunning] = useState(false);
  const [jobId, setJobId] = useState(null);
  const [jobStatus, setJobStatus] = useState(null);
  const [elapsed, setElapsed] = useState(0);
  const [logOutput, setLogOutput] = useState('');
  const pollRef = useRef(null);
  const timerRef = useRef(null);
  const startTimeRef = useRef(null);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      clearInterval(pollRef.current);
      clearInterval(timerRef.current);
    };
  }, []);

  function startPolling(jid) {
    startTimeRef.current = Date.now();
    timerRef.current = setInterval(() => {
      setElapsed(Math.floor((Date.now() - startTimeRef.current) / 1000));
    }, 1000);

    pollRef.current = setInterval(async () => {
      try {
        const res = await fetch(`/genomics/api/files/convert/status/${jid}`);
        if (!res.ok) return;
        const data = await res.json();
        setJobStatus(data);
        if (data.log) setLogOutput(data.log);
        if (data.status === 'completed' || data.status === 'failed' || data.status === 'error') {
          clearInterval(pollRef.current);
          clearInterval(timerRef.current);
          setRunning(false);
          if (data.status === 'completed') {
            showToast('FASTQ alignment completed!');
          } else {
            showToast('Alignment failed: ' + (data.error || 'unknown'), 'error');
          }
        }
      } catch { /* silent */ }
    }, 2000);
  }

  async function handleStart() {
    if (!selectedFastq) {
      showToast('Select a FASTQ file pair first', 'error');
      return;
    }

    const sel = fastqFiles.find(f => f.path === selectedFastq);
    if (!sel) {
      showToast('FASTQ file not found', 'error');
      return;
    }

    setRunning(true);
    setJobStatus(null);
    setLogOutput('');
    setElapsed(0);

    try {
      const res = await fetch('/genomics/api/files/convert/fastq-to-bam', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          fastq_r1: sel.path,
          fastq_r2: sel.format_details?.paired_path || null,
          reference: reference,
          aligner: aligner,
          threads: threads,
        }),
      });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      const jid = data.job_id;
      setJobId(jid);
      showToast('Alignment started! Job: ' + jid);
      startPolling(jid);
    } catch (err) {
      setRunning(false);
      showToast('Failed to start alignment: ' + err.message, 'error');
    }
  }

  function fmtElapsed(sec) {
    const m = Math.floor(sec / 60);
    const s = sec % 60;
    return `${m}:${String(s).padStart(2, '0')}`;
  }

  return (
    <div>
      {/* Inline form */}
      <div style={{ display: 'grid', gap: 12, gridTemplateColumns: '1fr 1fr', marginBottom: 12 }}>
        <div style={{ gridColumn: '1 / -1' }}>
          <label style={LABEL_STYLE}>FASTQ pair</label>
          <select style={SELECT_STYLE} value={selectedFastq} onChange={e => setSelectedFastq(e.target.value)}>
            <option value="">-- Select FASTQ pair --</option>
            {fastqFiles.map(f => {
              const name = f.sample_name || f.path.split('/').pop();
              const paired = f.format_details?.paired_path ? ' (paired)' : ' (single-end)';
              return <option key={f.path} value={f.path}>{name}{paired}</option>;
            })}
          </select>
        </div>
        <div>
          <label style={LABEL_STYLE}>Reference genome path</label>
          <input style={INPUT_STYLE} value={reference} onChange={e => setReference(e.target.value)} />
        </div>
        <div>
          <label style={LABEL_STYLE}>Aligner</label>
          <select style={SELECT_STYLE} value={aligner} onChange={e => setAligner(e.target.value)}>
            <option value="bwa">bwa mem (recommended)</option>
            <option value="minimap2">minimap2</option>
          </select>
        </div>
        <div style={{ gridColumn: '1 / -1' }}>
          <label style={LABEL_STYLE}>Threads: {threads}</label>
          <input type="range" min={1} max={44} value={threads}
            onChange={e => setThreads(Number(e.target.value))}
            style={{ width: '100%', accentColor: '#bc8cff' }}
          />
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: '#6e7681' }}>
            <span>1</span>
            <span>44</span>
          </div>
        </div>
      </div>

      <button style={BTN_PRIMARY} onClick={handleStart} disabled={running || !selectedFastq}>
        {running ? 'Running...' : 'Start Alignment'}
      </button>

      {/* Progress section */}
      {(running || jobStatus) && (
        <div style={{
          marginTop: 14, padding: 12, background: '#161b22', border: '1px solid #30363d', borderRadius: 8,
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
            <span style={{ fontSize: 13, color: '#c9d1d9', fontWeight: 500 }}>
              {running ? 'Alignment in progress...' : (jobStatus?.status === 'completed' ? 'Completed' : 'Failed')}
            </span>
            <span style={{ fontSize: 12, color: '#8b949e' }}>Elapsed: {fmtElapsed(elapsed)}</span>
          </div>

          {/* Progress bar */}
          {running && (
            <div style={{ height: 6, background: '#21262d', borderRadius: 3, overflow: 'hidden', marginBottom: 10 }}>
              <div style={{
                width: jobStatus?.progress != null ? `${jobStatus.progress}%` : '100%',
                height: '100%',
                background: '#bc8cff',
                borderRadius: 3,
                transition: 'width 0.5s',
                animation: jobStatus?.progress == null ? 'pulse 1.5s ease-in-out infinite' : 'none',
              }} />
            </div>
          )}

          {/* Log output */}
          {logOutput && (
            <pre style={{
              background: '#0d1117', border: '1px solid #21262d', borderRadius: 6,
              padding: 10, fontSize: 11, color: '#8b949e', fontFamily: 'monospace',
              whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: 200, overflow: 'auto',
              margin: 0,
            }}>
              {logOutput}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}


/* ══════════════════════════════════════════════════════════════
   STORAGE DASHBOARD
   ══════════════════════════════════════════════════════════════ */

function StorageDashboard({ storage }) {
  if (!storage) {
    return (
      <div style={{
        background: '#161b22', border: '1px solid #30363d', borderRadius: 8,
        padding: 20, textAlign: 'center', color: '#8b949e', fontSize: 14,
      }}>
        Loading storage info...
      </div>
    );
  }

  function StorageBar({ label, total, used, pct }) {
    const color = progressColor(pct ?? 0);
    return (
      <div style={{ marginBottom: 12 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
          <span style={{ fontSize: 13, fontWeight: 500, color: '#c9d1d9' }}>{label}</span>
          <span style={{ fontSize: 12, color: '#8b949e' }}>
            {fmtStorageSize(used)} / {fmtStorageSize(total)} ({pct != null ? pct.toFixed(1) : '?'}%)
          </span>
        </div>
        <div style={{ height: 8, background: '#21262d', borderRadius: 4, overflow: 'hidden' }}>
          <div style={{
            width: `${Math.min(pct ?? 0, 100)}%`,
            height: '100%',
            background: color,
            borderRadius: 4,
            transition: 'width 0.3s',
          }} />
        </div>
      </div>
    );
  }

  return (
    <div>
      {storage.persistent && (
        <StorageBar
          label="/data (Persistent)"
          total={storage.persistent.total}
          used={storage.persistent.used}
          pct={storage.persistent.pct}
        />
      )}
      {storage.fast && (
        <StorageBar
          label="/scratch (Fast SSD)"
          total={storage.fast.total}
          used={storage.fast.used}
          pct={storage.fast.pct}
        />
      )}
    </div>
  );
}


/* ══════════════════════════════════════════════════════════════
   MAIN COMPONENT
   ══════════════════════════════════════════════════════════════ */

export default function RawDataPanel() {
  const dispatch = useAppDispatch();

  /* state */
  const [files, setFiles] = useState([]);
  const [storage, setStorage] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [toast, setToast] = useState(null);
  const [refreshing, setRefreshing] = useState(false);

  /* toast helper */
  const showToast = useCallback((msg, type = 'success') => {
    setToast({ msg, type });
    setTimeout(() => setToast(null), 4000);
  }, []);

  /* ── data fetching ──────────────────────────────────── */
  const loadFiles = useCallback(async () => {
    try {
      const res = await fetch('/genomics/api/files/scan');
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      setFiles(Array.isArray(data) ? data : []);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  const loadStorage = useCallback(async () => {
    try {
      const res = await fetch('/genomics/api/storage/status');
      if (!res.ok) return;
      const data = await res.json();
      setStorage(data);
    } catch {
      /* silent */
    }
  }, []);

  useEffect(() => {
    loadFiles();
    loadStorage();
  }, [loadFiles, loadStorage]);

  /* ── actions ─────────────────────────────────────────── */
  async function handleRefresh() {
    setRefreshing(true);
    await Promise.all([loadFiles(), loadStorage()]);
    setRefreshing(false);
    showToast('Files refreshed');
  }

  /* ── group files by type ─────────────────────────────── */
  const TYPE_ORDER = ['BAM', 'FASTQ', 'VCF', 'gVCF'];
  const grouped = {};
  for (const type of TYPE_ORDER) grouped[type] = [];

  for (const f of files) {
    const t = (f.file_type || '').toUpperCase();
    if (t === 'BAM' || t === 'CRAM') grouped['BAM'].push(f);
    else if (t === 'FASTQ') grouped['FASTQ'].push(f);
    else if (t === 'GVCF') grouped['gVCF'].push(f);
    else if (t === 'VCF') grouped['VCF'].push(f);
    else {
      const ft = (f.file_type || '').toLowerCase();
      if (ft.includes('bam') || ft.includes('cram')) grouped['BAM'].push(f);
      else if (ft.includes('fastq') || ft.includes('fq')) grouped['FASTQ'].push(f);
      else if (ft.includes('gvcf') || ft.includes('g.vcf')) grouped['gVCF'].push(f);
      else if (ft.includes('vcf')) grouped['VCF'].push(f);
    }
  }

  const SECTION_ICONS = {
    BAM: <BamIcon />,
    FASTQ: <FastqIcon />,
    VCF: <VcfIcon />,
    gVCF: <GvcfIcon />,
  };

  const SECTION_COLORS = {
    BAM: '#d29922',
    FASTQ: '#bc8cff',
    VCF: '#3fb950',
    gVCF: '#58a6ff',
  };

  const SECTION_TOOLTIPS = {
    BAM: 'Alignment files (BAM/CRAM) contain sequencing reads aligned to a reference genome. BAM is uncompressed; CRAM is a compressed alternative. Both are produced by aligning FASTQ reads using BWA or minimap2, and are required for variant calling.',
    FASTQ: 'FASTQ files contain raw sequencing reads with quality scores. They come in pairs (R1 forward reads, R2 reverse reads). Must be aligned to a reference genome to create BAM files before variant calling.',
    VCF: 'Variant Call Format (VCF) files contain genetic variants (SNPs, insertions, deletions) identified by comparing BAM reads to a reference genome. Used directly for PGS scoring.',
    gVCF: 'Genomic VCF (gVCF) files are an enhanced VCF format that includes BOTH variant positions AND reference-homozygous positions. This provides complete genotype information at every position, giving the best match rates for PGS scoring (typically 95-100% vs ~50% for regular VCF).',
  };

  /* ── render ──────────────────────────────────────────── */
  return (
    <div>
      {/* ── Header ──────────────────────────────────────── */}
      <div className="section-header">
        <div>
          <h2 className="section-title">Raw Data Files</h2>
          <p style={{ color: '#8b949e', fontSize: 14, marginTop: 4, margin: 0 }}>
            All genomic data files discovered on this server.
            {files.length > 0 && <span style={{ color: '#c9d1d9', marginLeft: 8 }}>{files.length} files total</span>}
          </p>
        </div>
        <button className="btn" onClick={handleRefresh} disabled={refreshing} style={{ gap: 6 }}>
          <RefreshIcon />
          {refreshing ? 'Scanning...' : 'Refresh'}
        </button>
      </div>

      {/* ── Loading / Error ─────────────────────────────── */}
      {loading && (
        <div>
          <div className="spinner" />
          <p className="loading-text">Scanning files...</p>
        </div>
      )}

      {error && (
        <div style={{
          background: 'rgba(248, 81, 73, 0.1)', border: '1px solid rgba(248, 81, 73, 0.3)',
          borderRadius: 8, padding: '12px 16px', marginBottom: 16, color: '#f85149', fontSize: 14,
        }}>
          Error loading files: {error}
        </div>
      )}

      {!loading && files.length === 0 && !error && (
        <div style={{
          background: '#161b22', border: '1px solid #30363d', borderRadius: 8,
          padding: '40px 20px', textAlign: 'center', marginBottom: 16,
        }}>
          <BamIcon />
          <h3 style={{ color: '#c9d1d9', fontSize: 16, marginTop: 12 }}>No genomic files found</h3>
          <p style={{ color: '#8b949e', fontSize: 13 }}>
            Use the Upload section below to add files, or run the FASTQ/BAM conversion pipeline.
          </p>
        </div>
      )}

      {/* ══════════════════════════════════════════════════
         Section 1: BAM Files
         ══════════════════════════════════════════════════ */}
      {!loading && (
        <CollapsibleSection
          title="Alignment Files"
          count={grouped['BAM'].length}
          tooltip={SECTION_TOOLTIPS.BAM}
          icon={SECTION_ICONS.BAM}
          color={SECTION_COLORS.BAM}
        >
          {grouped['BAM'].length === 0 ? (
            <div style={{ padding: 16, textAlign: 'center', color: '#6e7681', fontSize: 13 }}>
              No alignment files found. Align FASTQ files to create BAM/CRAM files.
            </div>
          ) : (
            <CardGrid>
              {grouped['BAM'].map(file => (
                <BamCard key={file.id || file.path} file={file} showToast={showToast} loadFiles={loadFiles} />
              ))}
            </CardGrid>
          )}
        </CollapsibleSection>
      )}

      {/* ══════════════════════════════════════════════════
         Section 2: BAM/CRAM → VCF Conversion Pipeline
         ══════════════════════════════════════════════════ */}
      {!loading && (
        <CollapsibleSection
          title="Convert to VCF"
          tooltip="Convert BAM or CRAM alignment files to VCF/gVCF using bcftools (quick) or DeepVariant (full, GPU-accelerated). This is required before PGS scoring."
          icon={<ConvertIcon />}
          color="#58a6ff"
          defaultOpen={false}
        >
          <PipelineSection showToast={showToast} />
        </CollapsibleSection>
      )}

      {/* ══════════════════════════════════════════════════
         Section 3: FASTQ Files
         ══════════════════════════════════════════════════ */}
      {!loading && (
        <CollapsibleSection
          title="FASTQ Files"
          count={grouped['FASTQ'].length}
          tooltip={SECTION_TOOLTIPS.FASTQ}
          icon={SECTION_ICONS.FASTQ}
          color={SECTION_COLORS.FASTQ}
        >
          {grouped['FASTQ'].length === 0 ? (
            <div style={{ padding: 16, textAlign: 'center', color: '#6e7681', fontSize: 13 }}>
              No FASTQ files found. Upload sequencing data to get started.
            </div>
          ) : (
            <CardGrid>
              {grouped['FASTQ'].map(file => (
                <FastqCard key={file.id || file.path} file={file} showToast={showToast} loadFiles={loadFiles} />
              ))}
            </CardGrid>
          )}
        </CollapsibleSection>
      )}

      {/* ══════════════════════════════════════════════════
         Section 3: VCF Files
         ══════════════════════════════════════════════════ */}
      {!loading && (
        <CollapsibleSection
          title="VCF Files"
          count={grouped['VCF'].length}
          tooltip={SECTION_TOOLTIPS.VCF}
          icon={SECTION_ICONS.VCF}
          color={SECTION_COLORS.VCF}
        >
          {grouped['VCF'].length === 0 ? (
            <div style={{ padding: 16, textAlign: 'center', color: '#6e7681', fontSize: 13 }}>
              No VCF files found. Use the "Convert to VCF" section above to convert alignment files.
            </div>
          ) : (
            <CardGrid>
              {grouped['VCF'].map(file => (
                <VcfCard key={file.id || file.path} file={file} color="#3fb950" typeLabel="VCF" showToast={showToast} loadFiles={loadFiles} />
              ))}
            </CardGrid>
          )}
        </CollapsibleSection>
      )}

      {/* ══════════════════════════════════════════════════
         Section 4: gVCF Files
         ══════════════════════════════════════════════════ */}
      {!loading && (
        <CollapsibleSection
          title="gVCF Files"
          count={grouped['gVCF'].length}
          tooltip={SECTION_TOOLTIPS.gVCF}
          icon={SECTION_ICONS.gVCF}
          color={SECTION_COLORS.gVCF}
        >
          {grouped['gVCF'].length === 0 ? (
            <div style={{ padding: 16, textAlign: 'center', color: '#6e7681', fontSize: 13 }}>
              No gVCF files found. Use DeepVariant via the BAM to VCF conversion tool to generate gVCF files.
            </div>
          ) : (
            <CardGrid>
              {grouped['gVCF'].map(file => (
                <VcfCard key={file.id || file.path} file={file} color="#58a6ff" typeLabel="gVCF" showToast={showToast} loadFiles={loadFiles} />
              ))}
            </CardGrid>
          )}
        </CollapsibleSection>
      )}

      {/* ══════════════════════════════════════════════════
         Section 5: Upload Files
         ══════════════════════════════════════════════════ */}
      <CollapsibleSection
        title="Upload Files"
        tooltip="Upload genomic files from your computer or download from a URL. Supported formats: BAM, FASTQ (.fastq.gz, .fq.gz), VCF (.vcf.gz), gVCF (.g.vcf.gz)"
        icon={<UploadIcon />}
        color="#8b949e"
      >
        <UploadSection
          onUploadComplete={() => { loadFiles(); loadStorage(); }}
          showToast={showToast}
        />
      </CollapsibleSection>



{/* ══════════════════════════════════════════════════
         Section 7: Convert FASTQ -> BAM
         ══════════════════════════════════════════════════ */}
      <CollapsibleSection
        title="Convert FASTQ \u2192 BAM"
        tooltip="Align raw FASTQ reads to a reference genome to create BAM files. This is the first step in the genomics pipeline: FASTQ \u2192 BAM \u2192 VCF \u2192 PGS scoring."
        icon={<AlignIcon />}
        color="#bc8cff"
      >
        <ConvertFastqToBamSection
          fastqFiles={grouped['FASTQ'] || []}
          showToast={showToast}
        />
      </CollapsibleSection>

      {/* ══════════════════════════════════════════════════
         Section 8: Storage Dashboard
         ══════════════════════════════════════════════════ */}
      <CollapsibleSection
        title="Storage Dashboard"
        tooltip="Disk space usage across storage tiers. Persistent (/data) survives reboots. Fast SSD (/scratch) is ephemeral but 10x faster for computation."
        icon={<StorageIcon />}
        color="#8b949e"
      >
        <StorageDashboard storage={storage} />
      </CollapsibleSection>

      {/* ── Toast ──────────────────────────────────────── */}
      <Toast toast={toast} />
    </div>
  );
}
