import { useState, useEffect, useCallback, useRef } from 'react';

const BASE = '/genomics/api/reports';

/* ── Lightweight Markdown → HTML ──────────────────────────────── */

function mdToHtml(md) {
  let html = md
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/^#### (.+)$/gm, '<h4>$1</h4>')
    .replace(/^### (.+)$/gm, '<h3>$1</h3>')
    .replace(/^## (.+)$/gm, '<h2>$1</h2>')
    .replace(/^# (.+)$/gm, '<h1>$1</h1>')
    .replace(/\*\*\*(.+?)\*\*\*/g, '<strong><em>$1</em></strong>')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>')
    .replace(/^---+$/gm, '<hr/>')
    .replace(/\n\n/g, '</p><p>')
    .replace(/\n/g, '<br/>');

  // Tables
  html = html.replace(/<p>((\\|[^<]*<br\/>)+\\|[^<]*)<\/p>/g, (match, tableBlock) => {
    const rows = tableBlock.split('<br/>').filter(r => r.trim());
    if (rows.length < 2) return match;
    let table = '<div class="md-table-wrap"><table>';
    rows.forEach((row, i) => {
      if (/^\|[\s\-:|]+\|$/.test(row.replace(/<[^>]+>/g, ''))) return;
      const cells = row.split('|').filter((_, idx, arr) => idx > 0 && idx < arr.length - 1);
      const tag = i === 0 ? 'th' : 'td';
      if (i === 0) table += '<thead>';
      table += '<tr>' + cells.map(c => `<${tag}>${c.trim()}</${tag}>`).join('') + '</tr>';
      if (i === 0) table += '</thead><tbody>';
    });
    table += '</tbody></table></div>';
    return table;
  });

  // Lists
  html = html.replace(/(<br\/>- .+(?:<br\/>- .+)*)/g, (match) => {
    const items = match.split('<br/>').filter(l => l.startsWith('- '));
    return '<ul>' + items.map(li => '<li>' + li.slice(2) + '</li>').join('') + '</ul>';
  });

  return '<p>' + html + '</p>';
}

/* ── Category badge ───────────────────────────────────────────── */

const CAT_STYLES = {
  pgs: { bg: 'rgba(63,185,80,0.15)', fg: '#3fb950' },
  run: { bg: 'rgba(88,166,255,0.15)', fg: '#58a6ff' },
  sample: { bg: 'rgba(240,136,62,0.15)', fg: '#f0883e' },
  section: { bg: 'rgba(56,189,248,0.15)', fg: '#38bdf8' },
  qc: { bg: 'rgba(188,140,255,0.15)', fg: '#bc8cff' },
  custom: { bg: 'rgba(210,168,255,0.15)', fg: '#d2a8ff' },
  summary: { bg: 'rgba(240,136,62,0.15)', fg: '#f0883e' },
};

function CatBadge({ category }) {
  const s = CAT_STYLES[category] || CAT_STYLES.custom;
  return (
    <span style={{
      padding: '2px 8px', borderRadius: 10, fontSize: 10, fontWeight: 700,
      background: s.bg, color: s.fg, textTransform: 'uppercase', letterSpacing: 0.5,
    }}>
      {category}
    </span>
  );
}

/* ── Main Reports Panel ───────────────────────────────────────── */

export default function ReportsPanel() {
  const [reports, setReports] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState(null);
  const [content, setContent] = useState('');
  const [contentLoading, setContentLoading] = useState(false);
  const [filter, setFilter] = useState(null);
  const [search, setSearch] = useState('');
  const [regenerating, setRegenerating] = useState(false);
  const contentRef = useRef(null);

  const token = localStorage.getItem('auth_token');
  const headers = { Authorization: 'Bearer ' + token };

  const fetchReports = useCallback(async () => {
    try {
      const res = await fetch(`${BASE}/list`, { headers });
      const data = await res.json();
      setReports(data);
    } catch (e) {
      console.error('Failed to load reports:', e);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetchReports(); }, [fetchReports]);

  const loadReport = async (filename) => {
    setSelected(filename);
    setContentLoading(true);
    try {
      const res = await fetch(`${BASE}/content/${encodeURIComponent(filename)}`, { headers });
      const data = await res.json();
      setContent(data.content);
    } catch {
      setContent('# Error\n\nFailed to load report.');
    }
    setContentLoading(false);
  };

  const handleRegenerate = async () => {
    setRegenerating(true);
    try {
      await fetch(`${BASE}/regenerate-all`, { method: 'POST', headers });
      await fetchReports();
    } catch {}
    setRegenerating(false);
  };

  // Filter + search
  const filtered = reports.filter(r => {
    if (filter && r.category !== filter) return false;
    if (search && !r.title.toLowerCase().includes(search.toLowerCase()) &&
        !r.filename.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  // Group by category
  const groups = {};
  filtered.forEach(r => {
    (groups[r.category] = groups[r.category] || []).push(r);
  });

  if (loading) {
    return <div style={{ padding: 40, textAlign: 'center', color: '#8b949e' }}>Loading reports...</div>;
  }

  return (
    <div style={{ display: 'flex', gap: 0, height: 'calc(100vh - 60px)' }}>
      {/* ── Sidebar ──── */}
      <div style={{
        width: 280, minWidth: 280, borderRight: '1px solid #21262d',
        overflowY: 'auto', background: '#0d1117',
      }}>
        {/* Toolbar */}
        <div style={{ padding: '10px 12px', borderBottom: '1px solid #21262d' }}>
          <div style={{ position: 'relative', marginBottom: 8 }}>
            <input type="text" placeholder="Search reports..."
              value={search} onChange={e => setSearch(e.target.value)}
              style={{
                width: '100%', padding: '6px 10px 6px 28px',
                background: '#161b22', border: '1px solid #30363d',
                borderRadius: 6, color: '#c9d1d9', fontSize: 12, outline: 'none',
              }} />
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#6e7681" strokeWidth="2"
              style={{ position: 'absolute', left: 8, top: '50%', transform: 'translateY(-50%)' }}>
              <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
            </svg>
          </div>
          <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
            {[null, 'pgs', 'run', 'section', 'sample', 'qc', 'custom'].map(cat => (
              <button key={cat || 'all'} onClick={() => setFilter(cat)}
                style={{
                  padding: '3px 8px', borderRadius: 4, fontSize: 10, cursor: 'pointer',
                  border: '1px solid ' + (filter === cat ? '#58a6ff' : '#30363d'),
                  background: filter === cat ? 'rgba(88,166,255,0.1)' : 'transparent',
                  color: filter === cat ? '#58a6ff' : '#8b949e',
                }}>
                {cat || 'All'} ({cat ? reports.filter(r => r.category === cat).length : reports.length})
              </button>
            ))}
          </div>
        </div>

        {/* Report list */}
        {Object.entries(groups).map(([cat, items]) => (
          <div key={cat}>
            <div style={{
              padding: '8px 14px 4px', fontSize: 10, fontWeight: 700,
              color: '#6e7681', textTransform: 'uppercase', letterSpacing: 0.5,
            }}>
              {cat} ({items.length})
            </div>
            {items.map(r => (
              <div key={r.filename}
                onClick={() => loadReport(r.filename)}
                style={{
                  padding: '7px 14px', fontSize: 12, cursor: 'pointer',
                  color: selected === r.filename ? '#58a6ff' : '#c9d1d9',
                  background: selected === r.filename ? 'rgba(88,166,255,0.08)' : 'transparent',
                  borderLeft: selected === r.filename ? '2px solid #58a6ff' : '2px solid transparent',
                  display: 'flex', alignItems: 'center', gap: 6,
                  overflow: 'hidden', whiteSpace: 'nowrap', textOverflow: 'ellipsis',
                }}
                onMouseEnter={e => { if (selected !== r.filename) e.currentTarget.style.background = '#1c2128'; }}
                onMouseLeave={e => { if (selected !== r.filename) e.currentTarget.style.background = 'transparent'; }}>
                <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis' }}>{r.title}</span>
              </div>
            ))}
          </div>
        ))}

        {filtered.length === 0 && (
          <div style={{ padding: 20, textAlign: 'center', color: '#484f58', fontSize: 12 }}>
            No reports found
          </div>
        )}

        {/* Actions */}
        <div style={{ padding: '12px 14px', borderTop: '1px solid #21262d', marginTop: 8 }}>
          <button onClick={handleRegenerate} disabled={regenerating}
            style={{
              width: '100%', padding: '6px 12px', borderRadius: 6, fontSize: 11,
              border: '1px solid #30363d', background: '#21262d', color: '#c9d1d9',
              cursor: regenerating ? 'not-allowed' : 'pointer',
            }}>
            {regenerating ? 'Regenerating...' : 'Regenerate All Run Reports'}
          </button>
          <a href="/genomics/reports/" target="_blank" rel="noopener"
            style={{
              display: 'block', textAlign: 'center', marginTop: 6,
              fontSize: 11, color: '#58a6ff', textDecoration: 'none',
            }}>
            Open standalone viewer
          </a>
        </div>
      </div>

      {/* ── Content ──── */}
      <div ref={contentRef} style={{ flex: 1, overflowY: 'auto', padding: '20px 28px' }}>
        {!selected ? (
          <div>
            <h2 style={{ color: '#e6edf3', fontSize: 18, marginBottom: 4 }}>Reports</h2>
            <p style={{ color: '#8b949e', fontSize: 13, marginBottom: 16 }}>
              {reports.length} reports available. Select one from the sidebar or browse below.
            </p>

            {/* Cards grid */}
            <div style={{
              display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
              gap: 10,
            }}>
              {filtered.slice(0, 30).map(r => {
                const date = new Date(r.modified).toLocaleDateString();
                const size = r.size_bytes > 1024 ? (r.size_bytes / 1024).toFixed(1) + ' KB' : r.size_bytes + ' B';
                return (
                  <div key={r.filename}
                    onClick={() => loadReport(r.filename)}
                    style={{
                      background: '#161b22', border: '1px solid #21262d', borderRadius: 8,
                      padding: '12px 14px', cursor: 'pointer', transition: 'border-color 0.15s',
                    }}
                    onMouseEnter={e => e.currentTarget.style.borderColor = '#58a6ff'}
                    onMouseLeave={e => e.currentTarget.style.borderColor = '#21262d'}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                      <span style={{ color: '#e6edf3', fontWeight: 600, fontSize: 13, flex: 1 }}>{r.title}</span>
                      <CatBadge category={r.category} />
                    </div>
                    <div style={{ color: '#6e7681', fontSize: 11, display: 'flex', gap: 10 }}>
                      <span>{date}</span>
                      <span>{size}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ) : contentLoading ? (
          <div style={{ padding: 40, textAlign: 'center', color: '#8b949e' }}>Loading report...</div>
        ) : (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
              <button onClick={() => setSelected(null)}
                style={{
                  padding: '4px 10px', borderRadius: 4, fontSize: 12,
                  border: '1px solid #30363d', background: '#21262d', color: '#c9d1d9', cursor: 'pointer',
                }}>
                Back
              </button>
              <span style={{ color: '#6e7681', fontSize: 12 }}>{selected}</span>
            </div>
            <div className="md-content" dangerouslySetInnerHTML={{ __html: mdToHtml(content) }} />
          </div>
        )}
      </div>
    </div>
  );
}
