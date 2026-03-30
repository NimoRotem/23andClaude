import { useState, useEffect, useCallback } from 'react';
import { systemApi } from '../api.js';

function UsageBar({ label, used, total, unit = 'GB', color = '#58a6ff', segments = null }) {
  const pct = total > 0 ? (used / total * 100) : 0;
  return (
    <div className="srv-bar-wrap">
      <div className="srv-bar-label">
        <span>{label}</span>
        <span>{used.toFixed(1)} / {total.toFixed(1)} {unit} ({pct.toFixed(1)}%)</span>
      </div>
      <div className="srv-bar-track">
        {segments ? segments.map((s, i) => (
          <div key={i} className="srv-bar-fill" style={{ width: `${s.pct}%`, background: s.color }} />
        )) : (
          <div className="srv-bar-fill" style={{ width: `${Math.min(pct, 100)}%`, background: pct > 90 ? '#f85149' : pct > 70 ? '#d29922' : color }} />
        )}
      </div>
    </div>
  );
}

function getProcessColor(cmd) {
  const lc = cmd.toLowerCase();
  if (lc.includes('claude') || lc.includes('anthropic')) return '#d2a8ff';
  if (lc.includes('samtools') || lc.includes('bcftools') || lc.includes('plink2') || lc.includes('bwa') || lc.includes('minimap2') || lc.includes('deepvariant')) return '#3fb950';
  if (lc.includes('uvicorn') || lc.includes('python') || lc.includes('node')) return '#58a6ff';
  if (lc.includes('singularity')) return '#d29922';
  return '';
}

function cpuClass(pct) {
  if (pct > 50) return 'srv-cpu-hot';
  if (pct > 20) return 'srv-cpu-warm';
  return '';
}

function formatAge(ts) {
  if (!ts) return '';
  const secs = Math.floor((Date.now() / 1000) - ts);
  if (secs < 5) return 'just now';
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  return `${Math.floor(secs / 3600)}h ago`;
}

function SettingsSection() {
  const [settings, setSettings] = useState({});
  const [aiEnabled, setAiEnabled] = useState(false);
  const [claudeStatus, setClaudeStatus] = useState(null);
  const [editingKey, setEditingKey] = useState(null);
  const [editValue, setEditValue] = useState('');
  const [saving, setSaving] = useState(false);
  const [toast, setToast] = useState('');

  useEffect(() => {
    fetch('/genomics/api/system/settings').then(r => r.json())
      .then(d => { setSettings(d.settings || {}); setAiEnabled(d.ai_enabled); })
      .catch(() => {});
    fetch('/genomics/api/system/claude-status').then(r => r.json())
      .then(setClaudeStatus)
      .catch(() => {});
  }, []);

  const handleSave = async (key) => {
    setSaving(true);
    try {
      const res = await fetch('/genomics/api/system/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key, value: editValue }),
      });
      const data = await res.json();
      if (data.ok) {
        setSettings(prev => ({ ...prev, [key]: editValue ? (key.includes('KEY') || key.includes('SECRET') ? editValue.slice(0, 8) + '...' : editValue) : '' }));
        if (key === 'ANTHROPIC_API_KEY') setAiEnabled(!!editValue);
        setToast(`${key} saved`);
        setTimeout(() => setToast(''), 3000);
      }
    } catch {}
    setSaving(false);
    setEditingKey(null);
  };

  const handleClaudeLogin = async () => {
    const res = await fetch('/genomics/api/system/claude-login', { method: 'POST' });
    const data = await res.json();
    setToast(data.message || data.error || 'Login started');
    setTimeout(() => setToast(''), 5000);
  };

  const keyLabels = {
    ANTHROPIC_API_KEY: { label: 'Anthropic API Key', desc: 'Enables AI-written report narratives. Get from console.anthropic.com', sensitive: true },
    OPENAI_API_KEY: { label: 'OpenAI API Key', desc: 'Optional alternative AI provider', sensitive: true },
    JWT_SECRET: { label: 'JWT Secret', desc: 'Authentication signing key', sensitive: true },
    AI_REPORT_MODEL: { label: 'AI Report Model', desc: 'Claude model for reports (default: claude-sonnet-4-20250514)' },
    REDIS_URL: { label: 'Redis URL', desc: 'Cache and pub/sub (default: redis://localhost:6379/0)' },
    GENOMICS_DATA_DIR: { label: 'Data Directory', desc: 'Persistent storage root (default: /data)' },
    GENOMICS_SCRATCH_DIR: { label: 'Scratch Directory', desc: 'Fast ephemeral storage (default: /scratch)' },
    GENOMICS_PORT: { label: 'Server Port', desc: 'Backend port (default: 8600)' },
  };

  return (
    <div className="srv-section">
      <h3 className="srv-section-title">Settings & Configuration</h3>

      {toast && (
        <div style={{ padding: '6px 12px', background: '#23863622', color: '#3fb950', borderRadius: 6, fontSize: 13, marginBottom: 10 }}>
          {toast}
        </div>
      )}

      {/* Claude Code Status */}
      <div style={{ marginBottom: 16, padding: 12, background: '#0d1117', borderRadius: 8, border: '1px solid #21262d' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <span style={{ fontWeight: 600, color: '#e6edf3', fontSize: 14 }}>Claude Code</span>
            {claudeStatus && (
              <span style={{ marginLeft: 8, fontSize: 12, color: claudeStatus.installed ? '#3fb950' : '#f85149' }}>
                {claudeStatus.installed ? `v${claudeStatus.version || '?'}` : 'Not installed'}
              </span>
            )}
            {claudeStatus?.session_active && (
              <span style={{ marginLeft: 8, fontSize: 11, padding: '1px 6px', borderRadius: 8, background: '#23863622', color: '#3fb950' }}>Session active</span>
            )}
          </div>
          <button onClick={handleClaudeLogin} style={{
            padding: '5px 12px', borderRadius: 6, border: '1px solid #30363d', background: '#21262d',
            color: '#c9d1d9', fontSize: 12, cursor: 'pointer',
          }}>
            {claudeStatus?.session_active ? 'Re-login' : 'Login to Claude'}
          </button>
        </div>
      </div>

      {/* AI Reports Status */}
      <div style={{ marginBottom: 16, padding: 12, background: '#0d1117', borderRadius: 8, border: `1px solid ${aiEnabled ? '#238636' : '#30363d'}` }}>
        <span style={{ fontSize: 13, color: aiEnabled ? '#3fb950' : '#d29922' }}>
          {aiEnabled ? 'AI Reports: Enabled — reports include AI-written narratives' : 'AI Reports: Disabled — set ANTHROPIC_API_KEY to enable'}
        </span>
      </div>

      {/* Environment Variables */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {Object.entries(keyLabels).map(([key, info]) => (
          <div key={key} style={{
            display: 'flex', alignItems: 'center', gap: 8, padding: '8px 12px',
            background: '#0d1117', borderRadius: 6, border: '1px solid #21262d',
          }}>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 13, fontWeight: 500, color: '#e6edf3' }}>{info.label}</div>
              <div style={{ fontSize: 11, color: '#484f58' }}>{info.desc}</div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0 }}>
              {editingKey === key ? (
                <>
                  <input value={editValue} onChange={e => setEditValue(e.target.value)}
                    type={info.sensitive ? 'password' : 'text'}
                    placeholder={info.sensitive ? 'Enter key...' : 'Enter value...'}
                    style={{ width: 200, padding: '4px 8px', fontSize: 12, background: '#161b22', border: '1px solid #30363d', color: '#c9d1d9', borderRadius: 4 }}
                    autoFocus
                    onKeyDown={e => { if (e.key === 'Enter') handleSave(key); if (e.key === 'Escape') setEditingKey(null); }}
                  />
                  <button onClick={() => handleSave(key)} disabled={saving}
                    style={{ padding: '3px 8px', fontSize: 11, borderRadius: 4, border: '1px solid #2ea043', background: '#238636', color: '#fff', cursor: 'pointer' }}>Save</button>
                  <button onClick={() => setEditingKey(null)}
                    style={{ padding: '3px 8px', fontSize: 11, borderRadius: 4, border: '1px solid #30363d', background: '#21262d', color: '#8b949e', cursor: 'pointer' }}>Cancel</button>
                </>
              ) : (
                <>
                  <span style={{ fontSize: 12, fontFamily: 'monospace', color: settings[key] ? '#8b949e' : '#484f58', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {settings[key] || '(not set)'}
                  </span>
                  <button onClick={() => { setEditingKey(key); setEditValue(''); }}
                    style={{ padding: '3px 8px', fontSize: 11, borderRadius: 4, border: '1px solid #30363d', background: '#21262d', color: '#c9d1d9', cursor: 'pointer' }}>
                    {settings[key] ? 'Change' : 'Set'}
                  </button>
                </>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default function ServerPanel() {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(0);
  const [now, setNow] = useState(Date.now());

  const fetchStats = useCallback(async () => {
    try {
      const data = await systemApi.stats();
      setStats(data);
      setLastUpdate(data.timestamp || Date.now() / 1000);
      setError(false);
      setLoading(false);
    } catch (e) {
      console.error('Failed to fetch system stats:', e);
      if (loading) {
        setError(true);
        setLoading(false);
      }
    }
  }, [loading]);

  useEffect(() => {
    fetchStats();
    const iv = setInterval(fetchStats, 3000);
    return () => clearInterval(iv);
  }, [fetchStats]);

  // Update "last updated" display every second
  useEffect(() => {
    const iv = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(iv);
  }, []);

  if (loading) return <div className="srv-loading">Loading system stats...</div>;
  if (error || !stats) return <div className="srv-error">Failed to load system stats</div>;

  const { cpu, memory, swap, disks, gpu, processes, network } = stats;

  // Memory segments
  const memUsedPct = memory.total_gb > 0 ? ((memory.used_gb - (memory.buffers_gb || 0) - (memory.cached_gb || 0)) / memory.total_gb * 100) : 0;
  const memCachedPct = memory.total_gb > 0 ? ((memory.cached_gb || 0) / memory.total_gb * 100) : 0;
  const memBuffersPct = memory.total_gb > 0 ? ((memory.buffers_gb || 0) / memory.total_gb * 100) : 0;

  const memSegments = [
    { pct: Math.max(memUsedPct, 0), color: '#58a6ff' },
    { pct: memBuffersPct, color: '#8957e5' },
    { pct: memCachedPct, color: '#d29922' },
  ];

  const sortedProcs = (processes || [])
    .slice()
    .sort((a, b) => b.cpu_pct - a.cpu_pct)
    .slice(0, 50);

  return (
    <div className="srv-panel">
      {/* Header */}
      <div className="srv-header">
        <h2>Server Monitor</h2>
        <span className="srv-header-meta">Last updated: {formatAge(lastUpdate)}</span>
      </div>

      {/* Row 1: Overview cards */}
      <div className="srv-overview">
        {/* Card 1: System Info */}
        <div className="srv-card">
          <div className="srv-card-title">System</div>
          <div className="srv-card-value">{stats.hostname || 'Unknown'}</div>
          <div className="srv-card-sub">Uptime: {stats.uptime || 'N/A'}</div>
          {stats.load_avg && (
            <div className="srv-card-sub">
              Load: {stats.load_avg[0]?.toFixed(2)} / {stats.load_avg[1]?.toFixed(2)} / {stats.load_avg[2]?.toFixed(2)}
            </div>
          )}
        </div>

        {/* Card 2: CPU */}
        <div className="srv-card">
          <div className="srv-card-title">CPU</div>
          <div className="srv-card-value">{cpu.usage_pct?.toFixed(1)}%</div>
          <div className="srv-card-sub">{cpu.cores} cores / {cpu.threads} threads</div>
          <div className="srv-card-sub" style={{ fontSize: '0.68rem', color: '#6e7681', marginTop: 2 }}>
            {cpu.model || ''}
          </div>
        </div>

        {/* Card 3: GPU */}
        <div className="srv-card" style={gpu && gpu.available ? { borderColor: '#238636' } : {}}>
          <div className="srv-card-title">GPU</div>
          {gpu && gpu.available && gpu.devices && gpu.devices.length > 0 ? (
            gpu.devices.map((dev, i) => (
              <div key={i}>
                <div className="srv-card-value" style={{ fontSize: '1.1rem' }}>{dev.name || `GPU ${i}`}</div>
                <div className="srv-card-sub" style={{ color: '#3fb950' }}>
                  {dev.utilization_pct?.toFixed(0)}% util | {dev.temperature_c != null ? `${dev.temperature_c}°C` : '—'}
                </div>
                <div className="srv-card-sub">
                  {dev.memory_used_mb != null ? `${(dev.memory_used_mb / 1024).toFixed(1)}` : '0'} / {dev.memory_total_mb != null ? `${(dev.memory_total_mb / 1024).toFixed(0)}` : '?'} GB VRAM
                </div>
              </div>
            ))
          ) : (
            <div className="srv-card-value" style={{ color: '#8b949e' }}>No GPU</div>
          )}
        </div>

        {/* Card 4: Network */}
        <div className="srv-card">
          <div className="srv-card-title">Network</div>
          {network && network.interfaces && network.interfaces.length > 0 ? (
            network.interfaces.map((iface, i) => (
              <div key={i}>
                <div className="srv-card-row">
                  <span>{iface.name}</span>
                  <span>{iface.ip}</span>
                </div>
                <div className="srv-card-row">
                  <span>RX: {(iface.rx_mb / 1024).toFixed(1)} GB</span>
                  <span>TX: {(iface.tx_mb / 1024).toFixed(1)} GB</span>
                </div>
              </div>
            ))
          ) : (
            <div className="srv-card-sub">No interfaces</div>
          )}
        </div>
      </div>

      {/* Row 2: Resource Bars */}
      <div className="srv-bars">
        <div className="srv-bars-title">Resources</div>

        {/* CPU bar */}
        <UsageBar
          label="CPU"
          used={cpu.usage_pct || 0}
          total={100}
          unit="%"
          color="#58a6ff"
        />

        {/* Memory bar with segments */}
        <div className="srv-bar-wrap">
          <div className="srv-bar-label">
            <span>Memory (used / buffers / cached)</span>
            <span>{memory.used_gb?.toFixed(1)} / {memory.total_gb?.toFixed(1)} GB ({memory.usage_pct?.toFixed(1)}%)</span>
          </div>
          <div className="srv-bar-track">
            {memSegments.map((s, i) => (
              <div key={i} className="srv-bar-fill" style={{ width: `${s.pct}%`, background: s.color, borderRadius: i === 0 ? '5px 0 0 5px' : i === memSegments.length - 1 ? '0 5px 5px 0' : '0' }} />
            ))}
          </div>
        </div>

        {/* Swap bar */}
        {swap && swap.total_gb > 0 && (
          <UsageBar
            label="Swap"
            used={swap.used_gb || 0}
            total={swap.total_gb}
            unit="GB"
            color="#8957e5"
          />
        )}

        {/* GPU bars (right after CPU/Memory, before disks) */}
        {gpu && gpu.available && gpu.devices && gpu.devices.length > 0 && (
          <>
            {gpu.devices.map((dev, i) => (
              <div key={`gpu-${i}`}>
                <div style={{ fontSize: '0.75rem', color: '#d2a8ff', marginTop: 8, marginBottom: 2, fontWeight: 600 }}>
                  GPU: {dev.name || `GPU ${i}`}
                  {dev.temperature_c != null && (
                    <span style={{ marginLeft: 8, color: dev.temperature_c > 80 ? '#f85149' : dev.temperature_c > 60 ? '#d29922' : '#8b949e', fontWeight: 400 }}>
                      {dev.temperature_c}°C
                    </span>
                  )}
                </div>
                <UsageBar
                  label="GPU Memory"
                  used={dev.memory_used_mb ? dev.memory_used_mb / 1024 : 0}
                  total={dev.memory_total_mb ? dev.memory_total_mb / 1024 : 0}
                  unit="GB"
                  color="#d2a8ff"
                />
                {dev.utilization_pct !== undefined && (
                  <UsageBar
                    label="GPU Utilization"
                    used={dev.utilization_pct}
                    total={100}
                    unit="%"
                    color="#d2a8ff"
                  />
                )}
              </div>
            ))}
          </>
        )}

        {/* Disk bars (filter out tmpfs/devtmpfs noise) */}
        {(disks || []).filter(d => !['tmpfs', 'devtmpfs'].includes(d.filesystem)).map((disk, i) => (
          <UsageBar
            key={i}
            label={`Disk ${disk.mount} (${disk.filesystem || disk.device || ''})`}
            used={disk.used_gb || 0}
            total={disk.total_gb || 0}
            unit="GB"
            color="#3fb950"
          />
        ))}
      </div>

      {/* Settings & Configuration */}
      <SettingsSection />

      {/* Row 4: Process Table */}
      <div className="srv-procs">
        <div className="srv-procs-title">Processes (Top {sortedProcs.length})</div>
        <div className="srv-procs-scroll">
          <table>
            <thead>
              <tr>
                <th>PID</th>
                <th>User</th>
                <th>CPU%</th>
                <th>MEM%</th>
                <th>RSS</th>
                <th>State</th>
                <th>Time</th>
                <th>Command</th>
              </tr>
            </thead>
            <tbody>
              {sortedProcs.map((p) => {
                const cmdColor = getProcessColor(p.command || '');
                return (
                  <tr key={p.pid}>
                    <td style={{ fontFamily: "'SF Mono', monospace", fontSize: '0.7rem' }}>{p.pid}</td>
                    <td>{p.user}</td>
                    <td className={cpuClass(p.cpu_pct)}>{p.cpu_pct?.toFixed(1)}</td>
                    <td>{p.mem_pct?.toFixed(1)}</td>
                    <td style={{ fontFamily: "'SF Mono', monospace", fontSize: '0.7rem' }}>{p.rss_mb?.toFixed(0)} MB</td>
                    <td>{p.state}</td>
                    <td style={{ fontFamily: "'SF Mono', monospace", fontSize: '0.7rem' }}>{p.time}</td>
                    <td className="srv-proc-cmd" style={cmdColor ? { color: cmdColor } : {}} title={p.command}>{p.command}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
