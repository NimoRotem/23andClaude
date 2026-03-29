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
