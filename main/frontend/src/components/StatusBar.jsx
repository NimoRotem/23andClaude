function getMetricColor(pct) {
  if (pct > 90) return '#f85149';
  if (pct > 70) return '#d29922';
  return '#3fb950';
}

function getProcessColor(name) {
  const lc = name.toLowerCase();
  if (lc.includes('claude') || lc.includes('anthropic')) return '#d2a8ff';
  if (['samtools', 'bcftools', 'plink2', 'plink', 'bwa', 'minimap2', 'deepvariant', 'gatk', 'picard', 'fastqc', 'trimmomatic', 'bowtie', 'hisat'].some(t => lc.includes(t))) return '#3fb950';
  if (['python', 'node', 'uvicorn', 'gunicorn', 'npm', 'deno'].some(t => lc.includes(t))) return '#58a6ff';
  if (lc.includes('singularity') || lc.includes('docker')) return '#d29922';
  return '#8b949e';
}

function extractProcessName(command) {
  if (!command) return '';
  let cmd = command;
  // Strip leading env vars like "KEY=val cmd"
  cmd = cmd.replace(/^(\S+=\S+\s+)+/, '');
  // Strip path
  const parts = cmd.split(/\s+/);
  let name = parts[0].split('/').pop();
  // Handle "env" prefix
  if (name === 'env' && parts.length > 1) {
    let i = 1;
    while (i < parts.length && parts[i].includes('=')) i++;
    if (i < parts.length) name = parts[i].split('/').pop();
  }
  // Normalize python3.10 -> python, node18 -> node
  name = name.replace(/^python\d[\d.]*/i, 'python')
    .replace(/^node\d[\d.]*/i, 'node')
    .replace(/^ruby\d[\d.]*/i, 'ruby');
  // Strip .py/.js/.sh extension for script files to show script name
  if (name === 'python' || name === 'node' || name === 'bash' || name === 'sh') {
    // Find the first argument that looks like a script
    for (let i = 1; i < Math.min(parts.length, 5); i++) {
      const arg = parts[i];
      if (arg && !arg.startsWith('-') && (arg.endsWith('.py') || arg.endsWith('.js') || arg.endsWith('.sh'))) {
        name = arg.split('/').pop();
        break;
      }
    }
  }
  return name;
}

function aggregateProcesses(processes) {
  if (!processes || !processes.length) return [];
  const groups = {};
  for (const p of processes) {
    const name = extractProcessName(p.command);
    if (!name || name === 'ps' || name === 'top' || name === 'head') continue;
    if (!groups[name]) groups[name] = { name, count: 0, totalCpu: 0 };
    groups[name].count++;
    groups[name].totalCpu += p.cpu_pct || 0;
  }
  return Object.values(groups)
    .sort((a, b) => b.totalCpu - a.totalCpu)
    .slice(0, 6);
}

export default function StatusBar({ stats, loading }) {
  if (loading || !stats) {
    return (
      <div className="status-bar">
        <div className="status-bar-inner">
          <span className="status-bar-chip" style={{ color: '#8b949e' }}>Loading...</span>
        </div>
      </div>
    );
  }

  const { cpu, memory, gpu, processes } = stats;
  const cpuPct = cpu?.usage_pct ?? 0;
  const cpuThreads = cpu?.threads ?? 0;
  const cpuUsed = cpuThreads > 0 ? Math.round((cpuPct / 100) * cpuThreads) : 0;
  const memUsed = memory?.used_gb ?? 0;
  const memTotal = memory?.total_gb ?? 0;
  const memPct = memTotal > 0 ? (memUsed / memTotal * 100) : 0;

  const gpuAvailable = gpu?.available && gpu?.devices?.length > 0;
  const gpuDev = gpuAvailable ? gpu.devices[0] : null;

  const procGroups = aggregateProcesses(processes);

  return (
    <div className="status-bar">
      <div className="status-bar-inner">
        {/* Metrics */}
        <div className="status-bar-metrics">
          <span className="status-bar-chip">
            CPU <span style={{ color: getMetricColor(cpuPct), fontWeight: 600 }}>{cpuPct.toFixed(1)}%</span>
            {cpuThreads > 0 && (
              <span style={{ color: '#8b949e', marginLeft: 4 }}>{cpuUsed}/{cpuThreads}</span>
            )}
          </span>
          <span className="status-bar-chip">
            MEM <span style={{ color: getMetricColor(memPct), fontWeight: 600 }}>{memUsed.toFixed(0)}/{memTotal.toFixed(0)}G</span>
          </span>
          {gpuDev && (
            <span className="status-bar-chip">
              GPU {gpuDev.name?.replace(/NVIDIA /, '').replace(/GeForce /, '') || 'GPU'}{' '}
              <span style={{ color: getMetricColor(gpuDev.utilization_pct || 0), fontWeight: 600 }}>
                {(gpuDev.utilization_pct || 0).toFixed(0)}%
              </span>
              {gpuDev.temperature_c != null && (
                <span style={{ color: gpuDev.temperature_c > 80 ? '#f85149' : gpuDev.temperature_c > 60 ? '#d29922' : '#8b949e', marginLeft: 4 }}>
                  {gpuDev.temperature_c}°C
                </span>
              )}
            </span>
          )}
        </div>

        {/* Divider */}
        {procGroups.length > 0 && <div className="status-bar-divider" />}

        {/* Process groups */}
        {procGroups.length > 0 && (
          <div className="status-bar-procs">
            {procGroups.map(g => (
              <span key={g.name} className="status-bar-proc" style={{ color: getProcessColor(g.name) }}>
                {g.name}{g.count > 1 && <span className="status-bar-proc-count">&times;{g.count}</span>}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
