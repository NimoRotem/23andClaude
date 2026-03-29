import { useState, useEffect } from 'react';
import { useAppDispatch } from '../context.jsx';

export default function VCFPanel() {
  const dispatch = useAppDispatch();
  const [vcfs, setVcfs] = useState([]);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [registerPath, setRegisterPath] = useState('');
  const [registerBuild, setRegisterBuild] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [toast, setToast] = useState(null);

  async function loadVcfs() {
    try {
      const res = await fetch('/genomics/api/vcfs/');
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      setVcfs(Array.isArray(data) ? data : []);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadVcfs(); }, []);

  async function handleRegister() {
    if (!registerPath.trim()) return;
    setSubmitting(true);
    try {
      const res = await fetch('/genomics/api/vcfs/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          path: registerPath.trim(),
          ...(registerBuild ? { genome_build: registerBuild } : {}),
        }),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text);
      }
      setToast({ msg: 'VCF registered!', type: 'success' });
      setDialogOpen(false);
      setRegisterPath('');
      setRegisterBuild('');
      loadVcfs();
    } catch (err) {
      setToast({ msg: 'Failed: ' + err.message, type: 'error' });
    } finally {
      setSubmitting(false);
      setTimeout(() => setToast(null), 4000);
    }
  }

  function selectForScoring(vcf) {
    dispatch({ type: 'SELECT_VCF', payload: vcf.id });
    dispatch({ type: 'SET_TAB', payload: 3 });
  }

  async function deleteVcf(vcf) {
    if (!window.confirm('Delete ' + vcf.filename + '?')) return;
    await fetch('/genomics/api/vcfs/' + vcf.id, { method: 'DELETE' });
    loadVcfs();
  }

  function fmt(n) { return n != null ? Number(n).toLocaleString() : '--'; }
  function fmtBytes(b) {
    if (!b) return '--';
    if (b > 1e9) return (b / 1e9).toFixed(1) + ' GB';
    return (b / 1e6).toFixed(1) + ' MB';
  }

  return (
    <div>
      <div className="section-header">
        <h2 className="section-title">VCF Files ({vcfs.length})</h2>
        <button className="btn btn-primary" onClick={() => setDialogOpen(true)}>+ Register VCF</button>
      </div>

      {loading && <p style={{ color: '#8b949e', padding: 20 }}>Loading...</p>}
      {error && <p style={{ color: '#f85149', padding: 20 }}>Error: {error}</p>}

      {!loading && vcfs.length === 0 && !error && (
        <div style={{ textAlign: 'center', padding: 40, color: '#8b949e' }}>
          <p>No VCF files registered yet.</p>
          <p style={{ fontSize: 13, marginTop: 8 }}>Use the BAM → VCF tab to create one, or click "Register VCF" to add an existing file.</p>
        </div>
      )}

      {vcfs.length > 0 && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(360px, 1fr))', gap: 12 }}>
          {vcfs.map(vcf => (
            <div key={vcf.id} className="card">
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
                <div>
                  <div style={{ fontWeight: 600, fontSize: 15, color: '#e6edf3' }}>{vcf.filename}</div>
                  <div style={{ fontSize: 12, color: '#8b949e', fontFamily: 'monospace', marginTop: 2 }}>
                    {vcf.path_fast || vcf.path_persistent}
                  </div>
                </div>
                <span className={`badge badge-${vcf.qc_status === 'passed' ? 'green' : vcf.qc_status === 'issues' ? 'yellow' : 'gray'}`}>
                  {vcf.qc_status}
                </span>
              </div>

              <div style={{ display: 'flex', gap: 16, fontSize: 13, color: '#c9d1d9', marginBottom: 8 }}>
                <span>Samples: <strong>{fmt(vcf.sample_count)}</strong></span>
                <span>Variants: <strong>{fmt(vcf.variant_count)}</strong></span>
                <span>Ti/Tv: <strong>{vcf.titv_ratio ? vcf.titv_ratio.toFixed(2) : '--'}</strong></span>
                <span>Size: <strong>{fmtBytes(vcf.file_size_bytes)}</strong></span>
              </div>

              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginBottom: 10 }}>
                {vcf.genome_build && <span className="badge badge-purple">{vcf.genome_build}</span>}
                {vcf.caller && <span className="badge badge-gray">{vcf.caller}</span>}
                {vcf.path_fast && <span className="badge badge-blue">Fast SSD</span>}
                {vcf.path_persistent && <span className="badge badge-gray">Persistent</span>}
                {vcf.samples?.map(s => <span key={s} className="badge badge-gray">{s}</span>)}
              </div>

              <div style={{ display: 'flex', gap: 6 }}>
                <button className="btn btn-accent" style={{ fontSize: 12, padding: '4px 10px' }} onClick={() => selectForScoring(vcf)}>
                  Select for Scoring
                </button>
                <button className="btn" style={{ fontSize: 12, padding: '4px 10px', color: '#f85149' }} onClick={() => deleteVcf(vcf)}>
                  Delete
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Register Dialog */}
      {dialogOpen && (
        <div className="overlay" onClick={e => { if (e.target === e.currentTarget) setDialogOpen(false); }}>
          <div className="dialog">
            <h2>Register VCF File</h2>
            <div className="form-group">
              <label>File Path</label>
              <input className="input" type="text" placeholder="/scratch/nimog_output/7c266d66/final.vcf.gz"
                value={registerPath} onChange={e => setRegisterPath(e.target.value)} autoFocus />
            </div>
            <div className="form-group">
              <label>Genome Build (optional)</label>
              <select className="input" value={registerBuild} onChange={e => setRegisterBuild(e.target.value)}>
                <option value="">Auto-detect</option>
                <option value="GRCh37">GRCh37 (hg19)</option>
                <option value="GRCh38">GRCh38 (hg38)</option>
              </select>
            </div>
            <div className="dialog-actions">
              <button className="btn" onClick={() => setDialogOpen(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleRegister} disabled={!registerPath.trim() || submitting}>
                {submitting ? 'Registering...' : 'Register'}
              </button>
            </div>
          </div>
        </div>
      )}

      {toast && (
        <div className={`toast ${toast.type === 'error' ? 'toast-error' : 'toast-success'}`}>
          {toast.msg}
        </div>
      )}
    </div>
  );
}
