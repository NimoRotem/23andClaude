import { useState, useEffect } from 'react';
import { useAppDispatch } from '../context.jsx';

export default function BAMtoVCFPanel() {
  const dispatch = useAppDispatch();
  const [vcfs, setVcfs] = useState([]);
  const [synced, setSynced] = useState(null);

  async function loadVcfs() {
    try {
      const res = await fetch('/genomics/api/vcfs/');
      const data = await res.json();
      setVcfs(Array.isArray(data) ? data : []);
    } catch {}
  }

  async function syncNimog() {
    try {
      const res = await fetch('/genomics/api/vcfs/sync-nimog', { method: 'POST' });
      const data = await res.json();
      setSynced(data);
      loadVcfs();
    } catch {}
  }

  useEffect(() => {
    loadVcfs();
    syncNimog();
  }, []);

  return (
    <div>
      <div className="section-header">
        <h2 className="section-title">Step 1: Convert BAM to VCF</h2>
        <p style={{ color: '#8b949e', fontSize: 14, marginTop: 4 }}>
          Use nimog below to call variants from BAM files. Completed VCFs automatically appear in the VCF Files tab.
        </p>
      </div>

      {/* VCF status bar */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        background: '#161b22', border: '1px solid #30363d', borderRadius: 8,
        padding: '12px 16px', marginBottom: 16, flexWrap: 'wrap', gap: 10,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ color: '#e6edf3', fontSize: 14 }}>
            <strong>{vcfs.length}</strong> VCF{vcfs.length !== 1 ? 's' : ''} available
          </span>
          {synced && synced.synced > 0 && (
            <span style={{ color: '#3fb950', fontSize: 13, fontWeight: 600 }}>
              +{synced.synced} new from nimog
            </span>
          )}
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button className="btn" style={{ fontSize: 12, padding: '5px 12px' }} onClick={syncNimog}>
            Sync nimog VCFs
          </button>
          <button className="btn btn-accent" onClick={() => dispatch({ type: 'SET_TAB', payload: 1 })}>
            View VCF Files &rarr;
          </button>
        </div>
      </div>

      {/* VCF dropdown for quick view */}
      {vcfs.length > 0 && (
        <div style={{
          background: '#161b22', border: '1px solid #30363d', borderRadius: 8,
          padding: '12px 16px', marginBottom: 16
        }}>
          <div style={{ fontSize: 13, color: '#8b949e', marginBottom: 8 }}>Available VCF files:</div>
          {vcfs.map(v => (
            <div key={v.id} style={{
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
              padding: '6px 0', borderBottom: '1px solid #21262d', fontSize: 13,
            }}>
              <div>
                <span style={{ color: '#58a6ff', fontWeight: 600 }}>{v.filename}</span>
                <span style={{ color: '#8b949e', marginLeft: 8 }}>
                  {Number(v.variant_count).toLocaleString()} variants
                </span>
                <span style={{ color: '#8b949e', marginLeft: 8 }}>
                  {v.samples?.join(', ')}
                </span>
              </div>
              <div style={{ display: 'flex', gap: 6 }}>
                <span className={`badge badge-${v.qc_status === 'passed' ? 'green' : 'yellow'}`}>{v.qc_status}</span>
                <span className="badge badge-purple">{v.genome_build}</span>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Embedded nimog */}
      <div style={{ border: '1px solid #30363d', borderRadius: 8, overflow: 'hidden', marginBottom: 16 }}>
        <iframe
          src="/nimog/"
          title="nimog BAM to VCF Converter"
          style={{ width: '100%', height: '80vh', minHeight: 600, border: 'none', background: '#0d1117' }}
        />
      </div>
    </div>
  );
}
