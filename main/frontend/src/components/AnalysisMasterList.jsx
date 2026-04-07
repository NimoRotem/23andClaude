import { useState, useEffect, useRef } from 'react';

/* ── Lightweight Markdown → HTML ──────────────────────────────────── */
function mdToHtml(md) {
  let html = md
    // Escape HTML
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    // Headers
    .replace(/^#### (.+)$/gm, '<h4>$1</h4>')
    .replace(/^### (.+)$/gm, '<h3>$1</h3>')
    .replace(/^## (.+)$/gm, '<h2>$1</h2>')
    .replace(/^# (.+)$/gm, '<h1>$1</h1>')
    // Bold + italic
    .replace(/\*\*\*(.+?)\*\*\*/g, '<strong><em>$1</em></strong>')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    // Inline code
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    // Links: [text](url)
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>')
    // Horizontal rules
    .replace(/^---+$/gm, '<hr/>')
    // Line breaks
    .replace(/\n\n/g, '</p><p>')
    .replace(/\n/g, '<br/>');

  // Tables
  html = html.replace(/<p>((\|[^<]*<br\/>)+\|[^<]*)<\/p>/g, (match, tableBlock) => {
    const rows = tableBlock.split('<br/>').filter(r => r.trim());
    if (rows.length < 2) return match;
    let table = '<div class="md-table-wrap"><table>';
    rows.forEach((row, i) => {
      // Skip separator row (|---|---|)
      if (/^\|[\s\-:|]+\|$/.test(row.replace(/<[^>]+>/g, ''))) return;
      const cells = row.split('|').filter((_, idx, arr) => idx > 0 && idx < arr.length - 1);
      const tag = i === 0 ? 'th' : 'td';
      const wrap = i === 0 ? 'thead' : '';
      if (i === 0) table += '<thead>';
      table += '<tr>' + cells.map(c => `<${tag}>${c.trim()}</${tag}>`).join('') + '</tr>';
      if (i === 0) table += '</thead><tbody>';
    });
    table += '</tbody></table></div>';
    return table;
  });

  // Lists (- item)
  html = html.replace(/(<br\/>- .+(?:<br\/>- .+)*)/g, (match) => {
    const items = match.split('<br/>').filter(l => l.startsWith('- '));
    return '<ul>' + items.map(li => '<li>' + li.slice(2) + '</li>').join('') + '</ul>';
  });

  return '<p>' + html + '</p>';
}

/* ── Table of Contents ────────────────────────────────────────────── */
function extractToc(md) {
  const headers = [];
  const lines = md.split('\n');
  for (const line of lines) {
    const m = line.match(/^(#{1,3}) (.+)$/);
    if (m) {
      const level = m[1].length;
      const text = m[2].trim();
      const id = text.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
      headers.push({ level, text, id });
    }
  }
  return headers;
}

/* ── Search highlight ─────────────────────────────────────────────── */
function highlightSearch(html, query) {
  if (!query || query.length < 2) return html;
  const escaped = query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return html.replace(new RegExp(`(${escaped})`, 'gi'), '<mark>$1</mark>');
}

/* ── Main Component ───────────────────────────────────────────────── */
export default function AnalysisMasterList() {
  const [md, setMd] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [search, setSearch] = useState('');
  const [tocOpen, setTocOpen] = useState(false);
  const contentRef = useRef(null);

  useEffect(() => {
    fetch('/genomics/api/system/docs/genomics_analysis_master_list')
      .then(r => { if (!r.ok) throw new Error('Failed to load'); return r.json(); })
      .then(data => { setMd(data.content); setLoading(false); })
      .catch(e => { setError(e.message); setLoading(false); });
  }, []);

  const toc = md ? extractToc(md) : [];
  let rendered = md ? mdToHtml(md) : '';
  if (search) rendered = highlightSearch(rendered, search);

  // Add IDs to headers for TOC navigation
  if (md) {
    for (const h of toc) {
      const escapedText = h.text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      rendered = rendered.replace(
        new RegExp(`<h${h.level}>${escapedText}</h${h.level}>`),
        `<h${h.level} id="${h.id}">${h.text}</h${h.level}>`
      );
    }
  }

  const scrollTo = (id) => {
    const el = contentRef.current?.querySelector(`#${id}`);
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };

  if (loading) return (
    <div style={{ padding: 24, textAlign: 'center', color: '#8b949e' }}>
      <div className="spinner" />
      <p style={{ marginTop: 8 }}>Loading analysis reference...</p>
    </div>
  );

  if (error) return (
    <div style={{ padding: 16, color: '#f85149', background: 'rgba(248,81,73,0.1)', borderRadius: 8, border: '1px solid rgba(248,81,73,0.3)' }}>
      Error: {error}
    </div>
  );

  return (
    <div style={{ position: 'relative' }}>
      {/* Toolbar */}
      <div style={{
        display: 'flex', gap: 8, marginBottom: 12, alignItems: 'center',
        position: 'sticky', top: 0, zIndex: 10, background: '#0d1117',
        padding: '8px 0', borderBottom: '1px solid #21262d',
      }}>
        <button onClick={() => setTocOpen(!tocOpen)} style={{
          background: tocOpen ? '#21262d' : 'transparent', border: '1px solid #30363d',
          color: '#c9d1d9', padding: '5px 10px', borderRadius: 6, cursor: 'pointer', fontSize: 12,
          display: 'flex', alignItems: 'center', gap: 4,
        }}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M3 12h18M3 6h18M3 18h18"/>
          </svg>
          Contents
        </button>
        <div style={{ position: 'relative', flex: 1, maxWidth: 300 }}>
          <input
            type="text"
            placeholder="Search..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            style={{
              width: '100%', background: '#161b22', border: '1px solid #30363d',
              color: '#c9d1d9', padding: '5px 10px 5px 28px', borderRadius: 6, fontSize: 13,
              outline: 'none',
            }}
          />
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#6e7681" strokeWidth="2"
            style={{ position: 'absolute', left: 8, top: '50%', transform: 'translateY(-50%)' }}>
            <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
          </svg>
        </div>
        <span style={{ color: '#6e7681', fontSize: 11, marginLeft: 'auto' }}>
          {toc.filter(h => h.level === 2).length} sections
        </span>
      </div>

      <div style={{ display: 'flex', gap: 0 }}>
        {/* TOC sidebar */}
        {tocOpen && (
          <div style={{
            width: 260, minWidth: 260, maxHeight: 'calc(100vh - 200px)', overflowY: 'auto',
            borderRight: '1px solid #21262d', paddingRight: 12, marginRight: 12,
            position: 'sticky', top: 60,
          }}>
            {toc.filter(h => h.level <= 2).map((h, i) => (
              <div key={i} onClick={() => scrollTo(h.id)} style={{
                padding: '3px 0 3px ' + (h.level === 1 ? '0' : '12') + 'px',
                fontSize: h.level === 1 ? 13 : 12,
                color: h.level === 1 ? '#c9d1d9' : '#8b949e',
                fontWeight: h.level === 1 ? 600 : 400,
                cursor: 'pointer', borderRadius: 4,
              }}
              onMouseOver={e => e.currentTarget.style.color = '#58a6ff'}
              onMouseOut={e => e.currentTarget.style.color = h.level === 1 ? '#c9d1d9' : '#8b949e'}
              >
                {h.text}
              </div>
            ))}
          </div>
        )}

        {/* Content */}
        <div ref={contentRef} className="md-content" style={{ flex: 1, minWidth: 0 }}
          dangerouslySetInnerHTML={{ __html: rendered }}
        />
      </div>
    </div>
  );
}
