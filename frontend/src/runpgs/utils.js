export function formatElapsed(seconds) {
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

export function fmtBytes(b) {
  if (!b) return '--';
  if (b > 1e9) return (b / 1e9).toFixed(1) + ' GB';
  if (b > 1e6) return (b / 1e6).toFixed(1) + ' MB';
  if (b > 1024) return (b / 1024).toFixed(1) + ' KB';
  return b + ' B';
}

export function fileTypeBadgeInfo(type) {
  const map = {
    vcf: { label: 'VCF', color: '#3fb950', bg: 'rgba(63,185,80,0.15)' },
    'vcf.gz': { label: 'VCF.GZ', color: '#3fb950', bg: 'rgba(63,185,80,0.15)' },
    gvcf: { label: 'gVCF', color: '#58a6ff', bg: 'rgba(88,166,255,0.15)' },
    'gvcf.gz': { label: 'gVCF.GZ', color: '#58a6ff', bg: 'rgba(88,166,255,0.15)' },
    bam: { label: 'BAM', color: '#bc8cff', bg: 'rgba(188,140,255,0.15)' },
    cram: { label: 'CRAM', color: '#bc8cff', bg: 'rgba(188,140,255,0.15)' },
  };
  const t = (type || '').toLowerCase();
  return map[t] || { label: type || 'File', color: '#8b949e', bg: 'rgba(139,148,158,0.15)' };
}

export function isDone(s) {
  return s === 'complete' || s === 'completed' || s === 'failed';
}

export function zscoreColor(z) {
  const az = Math.abs(z);
  if (az >= 2) return '#f85149';
  if (az >= 1) return '#d29922';
  return '#3fb950';
}

export function riskLabel(z) {
  if (z >= 2) return { text: 'High Risk', color: '#f85149' };
  if (z >= 1) return { text: 'Above Average', color: '#d29922' };
  if (z > -1) return { text: 'Average', color: '#3fb950' };
  if (z > -2) return { text: 'Below Average', color: '#d29922' };
  return { text: 'Low Risk', color: '#58a6ff' };
}

export function matchRateColor(rate) {
  if (rate > 0.5) return '#3fb950';
  if (rate > 0.1) return '#d29922';
  return '#f85149';
}

export function pctColor(pct) {
  if (pct == null) return '#484f58';
  if (pct >= 90) return '#f85149';
  if (pct >= 75) return '#d29922';
  if (pct >= 25) return '#3fb950';
  if (pct >= 10) return '#d29922';
  return '#58a6ff';
}

export function shortFilename(path) {
  if (!path) return '--';
  return path.split('/').pop();
}
