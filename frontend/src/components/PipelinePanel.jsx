import { useState, useEffect, useCallback, useRef } from 'react';

const STEP_LABELS = {
  'dv-calling': 'DeepVariant Calling',
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
  { key: 'make_examples', label: 'make_examples', desc: 'CPU — extracting candidate variants' },
  { key: 'call_variants', label: 'call_variants', desc: 'GPU — neural network inference' },
  { key: 'postprocess', label: 'postprocess', desc: 'CPU — generating final VCF' },
];

function formatElapsed(secs) {
  if (!secs || secs < 0) return '0s';
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = Math.floor(secs % 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function formatEta(secs) {
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

function ProgressBar({ pct, color = '#58a6ff' }) {
  return (
    <div style={{ background: '#21262d', borderRadius: 6, height: 10, overflow: 'hidden', marginTop: 4 }}>
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
  const logs = (job.logs || []).slice(-8);

  return (
    <div className="pip-job-card">
      <div className="pip-job-header">
        <div>
          <span className="pip-job-id">Job {job.job_id?.slice(0, 8)}</span>
          {job.sample_name && <span className="pip-job-sample">{job.sample_name}</span>}
          <span className="pip-job-mode">{isDv ? 'DeepVariant' : 'bcftools'}</span>
          {isDv && job.use_gpu && (
            <span className="pip-gpu-badge">{isGpu ? 'GPU Active' : 'GPU Ready'}</span>
          )}
        </div>
        <span className={`pip-status pip-status-${job.status}`}>
          {job.status === 'running' ? 'Running' : job.status === 'completed' ? 'Completed' : job.status === 'failed' ? 'Failed' : job.status}
        </span>
      </div>

      {/* DeepVariant stage indicators */}
      {isDv && job.status === 'running' && (
        <div className="pip-dv-stages">
          {DV_STAGES.map((stage, i) => (
            <div key={stage.key} className={`pip-dv-stage ${i < dvStage ? 'done' : i === dvStage ? 'active' : 'pending'}`}>
              <div className="pip-dv-stage-num">{i + 1}</div>
              <div>
                <div className="pip-dv-stage-label">{stage.label}</div>
                <div className="pip-dv-stage-desc">{stage.desc}</div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Progress */}
      {job.status === 'running' && (
        <div className="pip-progress-section">
          <div className="pip-progress-label">
            <span>{stepLabel}</span>
            <span>{pct.toFixed(1)}%</span>
          </div>
          <ProgressBar pct={pct} color={isGpu ? '#d2a8ff' : '#58a6ff'} />

          <div className="pip-timing">
            <span>Elapsed: <strong>{formatElapsed(job.elapsed)}</strong></span>
            <span>ETA: <strong>{formatEta(job.eta)}</strong></span>
            {isDv && job.samples_done !== undefined && (
              <span>Samples: <strong>{job.samples_done}/{job.total_samples}</strong></span>
            )}
          </div>
        </div>
      )}

      {/* Completed info */}
      {job.status === 'completed' && (
        <div className="pip-completed-info">
          <span>Finished in {formatElapsed(job.elapsed)}</span>
          {job.output_vcf && <span className="pip-output-path">{job.output_vcf}</span>}
          {job.qc_result && (
            <div className="pip-qc-summary">
              {job.qc_result.variant_count != null && <span>Variants: {job.qc_result.variant_count.toLocaleString()}</span>}
              {job.qc_result.titv_ratio != null && <span>Ti/Tv: {job.qc_result.titv_ratio.toFixed(2)}</span>}
            </div>
          )}
        </div>
      )}

      {/* Error */}
      {job.status === 'failed' && job.error && (
        <div className="pip-error">{job.error}</div>
      )}

      {/* Logs */}
      {logs.length > 0 && (
        <div className="pip-logs">
          <div className="pip-logs-title">Recent Activity</div>
          <div className="pip-logs-content">
            {logs.map((line, i) => <div key={i} className="pip-log-line">{line}</div>)}
          </div>
        </div>
      )}
    </div>
  );
}

function HistoryRow({ job, onView }) {
  return (
    <tr onClick={() => onView(job)} style={{ cursor: 'pointer' }}>
      <td className="pip-hist-id">{job.job_id?.slice(0, 8)}</td>
      <td>{job.sample_names?.join(', ') || job.bam_path?.split('/').pop()?.replace(/\.(bam|cram)$/, '') || '—'}</td>
      <td>{job.mode === 'deepvariant' ? 'DV' : 'bcftools'}</td>
      <td>
        <span className={`pip-status-dot pip-status-${job.status}`} />
        {job.status}
      </td>
      <td>{job.elapsed ? formatElapsed(job.elapsed) : '—'}</td>
      <td>{job.completed_at ? new Date(job.completed_at * 1000).toLocaleString() : '—'}</td>
    </tr>
  );
}

export default function PipelinePanel() {
  const [jobs, setJobs] = useState([]);
  const [activeJob, setActiveJob] = useState(null);
  const [loading, setLoading] = useState(true);
  const [showNimog, setShowNimog] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [syncMsg, setSyncMsg] = useState('');
  const sseRef = useRef(null);
  const pollRef = useRef(null);

  // Fetch all jobs
  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch('/nimog/api/jobs');
      if (res.ok) {
        const data = await res.json();
        setJobs(Array.isArray(data) ? data : []);
      }
    } catch (e) {
      console.error('Failed to fetch jobs:', e);
    } finally {
      setLoading(false);
    }
  }, []);

  // Poll for new jobs every 5 seconds
  useEffect(() => {
    fetchJobs();
    pollRef.current = setInterval(fetchJobs, 5000);
    return () => clearInterval(pollRef.current);
  }, [fetchJobs]);

  // Find running job, fetch its full details, and connect SSE
  const jobDetailRef = useRef(null);

  useEffect(() => {
    const runningJob = jobs.find(j => j.status === 'running');
    if (!runningJob) {
      if (sseRef.current) {
        sseRef.current.close();
        sseRef.current = null;
      }
      jobDetailRef.current = null;
      setActiveJob(prev => prev?.status === 'running' ? null : prev);
      return;
    }

    // Already connected to this job
    if (sseRef.current && activeJob?.job_id === runningJob.job_id) return;

    // Close old SSE
    if (sseRef.current) sseRef.current.close();

    // Fetch full job details first (for sample_names, use_gpu, etc.)
    const fetchDetailAndConnect = async () => {
      let detail = { sample_names: [], use_gpu: true };
      try {
        const res = await fetch(`/nimog/api/jobs/${runningJob.job_id}`);
        if (res.ok) detail = await res.json();
      } catch {}
      jobDetailRef.current = detail;

      const sampleName = detail.sample_names?.[0] || detail.bam_path?.split('/').pop()?.replace(/\.(bam|cram)$/, '') || runningJob.bam_path?.replace(/\.(bam|cram)$/, '');

      const es = new EventSource(`/nimog/api/jobs/${runningJob.job_id}/stream`);
      sseRef.current = es;

      es.onmessage = (e) => {
        try {
          const data = JSON.parse(e.data);
          setActiveJob({
            ...data,
            job_id: runningJob.job_id,
            sample_name: sampleName,
            use_gpu: detail.use_gpu !== false,
          });
          if (data.status === 'completed' || data.status === 'failed') {
            fetchJobs();
          }
        } catch (err) {
          console.error('SSE parse error:', err);
        }
      };

      es.onerror = () => {
        es.close();
        sseRef.current = null;
        setTimeout(fetchJobs, 2000);
      };
    };

    fetchDetailAndConnect();

    return () => {
      if (sseRef.current) {
        sseRef.current.close();
        sseRef.current = null;
      }
    };
  }, [jobs, activeJob?.job_id, fetchJobs]);

  // Sync nimog VCFs
  const handleSync = async () => {
    setSyncing(true);
    setSyncMsg('');
    try {
      const res = await fetch('/api/vcfs/sync-nimog', { method: 'POST' });
      const data = await res.json();
      setSyncMsg(data.synced ? `Synced ${data.synced} VCF(s)` : 'No new VCFs to sync');
    } catch {
      setSyncMsg('Sync failed');
    } finally {
      setSyncing(false);
      setTimeout(() => setSyncMsg(''), 4000);
    }
  };

  const completedJobs = jobs.filter(j => j.status === 'completed');
  const failedJobs = jobs.filter(j => j.status === 'failed');
  const historyJobs = [...completedJobs, ...failedJobs].sort((a, b) => (b.completed_at || 0) - (a.completed_at || 0));

  return (
    <div className="pip-panel">
      <div className="pip-header">
        <h2>Pipeline</h2>
        <div className="pip-header-actions">
          <button className="pip-sync-btn" onClick={handleSync} disabled={syncing}>
            {syncing ? 'Syncing...' : 'Sync VCFs to Dashboard'}
          </button>
          {syncMsg && <span className="pip-sync-msg">{syncMsg}</span>}
        </div>
      </div>

      {/* Active Job */}
      {activeJob && activeJob.status === 'running' ? (
        <ActiveJobCard job={activeJob} />
      ) : loading ? (
        <div className="pip-empty">Loading pipeline status...</div>
      ) : (
        <div className="pip-empty">
          <div className="pip-empty-icon">
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#30363d" strokeWidth="1.5">
              <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" />
            </svg>
          </div>
          <div>No active pipeline jobs</div>
          <div className="pip-empty-sub">Use the converter below to start a BAM → gVCF conversion</div>
        </div>
      )}

      {/* Recently completed job */}
      {(!activeJob || activeJob.status !== 'running') && completedJobs.length > 0 && (
        <ActiveJobCard job={{
          ...completedJobs[0],
          job_id: completedJobs[0].job_id,
          sample_name: completedJobs[0].sample_names?.[0] || '',
          elapsed: completedJobs[0].completed_at && completedJobs[0].started_at
            ? completedJobs[0].completed_at - completedJobs[0].started_at
            : null,
        }} />
      )}

      {/* Start New Conversion */}
      <div className="pip-section">
        <button className="pip-section-toggle" onClick={() => setShowNimog(!showNimog)}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ transform: showNimog ? 'rotate(90deg)' : 'none', transition: 'transform 0.2s' }}>
            <path d="M9 18l6-6-6-6" />
          </svg>
          <span>Start New Conversion</span>
        </button>
        {showNimog && (
          <div className="pip-nimog-wrap">
            <iframe
              src="/nimog/"
              title="nimog BAM to VCF Converter"
              className="pip-nimog-iframe"
            />
          </div>
        )}
      </div>

      {/* Job History */}
      {historyJobs.length > 0 && (
        <div className="pip-section">
          <div className="pip-history-title">Job History ({historyJobs.length})</div>
          <div className="pip-history-scroll">
            <table className="pip-history-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Sample</th>
                  <th>Mode</th>
                  <th>Status</th>
                  <th>Duration</th>
                  <th>Completed</th>
                </tr>
              </thead>
              <tbody>
                {historyJobs.map(j => (
                  <HistoryRow key={j.job_id} job={j} onView={(job) => setActiveJob({
                    ...job,
                    sample_name: job.sample_names?.[0] || '',
                    elapsed: job.completed_at && job.started_at ? job.completed_at - job.started_at : null,
                  })} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
