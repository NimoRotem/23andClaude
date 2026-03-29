import { useState, useEffect, useRef, useCallback } from 'react';
import { pgsApi } from '../api.js';

const QUICK_FILTERS = [
  'Cancer',
  'Cardiovascular',
  'Neurological',
  'Metabolic',
  'Psychiatric',
];

// Flatten autocomplete API response into a list of suggestion objects
function flattenAutocomplete(data) {
  if (!data) return [];
  const items = [];
  if (data.traits) {
    for (const t of data.traits) {
      if (t.associated_pgs_count > 0 || data.traits.length <= 5) {
        items.push({
          type: 'trait',
          label: t.label,
          id: t.id,
          detail: t.associated_pgs_count > 0 ? `${t.associated_pgs_count} PGS scores` : 'Trait',
        });
      }
    }
  }
  if (data.scores) {
    for (const s of data.scores) {
      items.push({
        type: 'score',
        label: s.name || s.trait_reported || s.id,
        id: s.id,
        detail: `${s.id} - ${(s.variants_number || 0).toLocaleString()} variants`,
      });
    }
  }
  return items;
}

// Normalize search result to consistent field names
function normalizeResult(r) {
  const pub = r.publication || {};
  const ancestryGwas = r.ancestry_distribution?.gwas?.dist || r.ancestry_gwas?.dist || {};
  const ancestryTags = Object.entries(ancestryGwas).map(([k, v]) => `${k} ${v}%`);
  const builds = r.builds_available || [];
  if (!builds.length && r.ftp_harmonized_scoring_files) {
    if (r.ftp_harmonized_scoring_files.GRCh37) builds.push('GRCh37');
    if (r.ftp_harmonized_scoring_files.GRCh38) builds.push('GRCh38');
  }
  // Eval ancestry
  const ancestryEval = r.ancestry_distribution?.eval?.dist || r.ancestry_eval?.dist || {};
  const ancestryEvalTags = Object.entries(ancestryEval).map(([k, v]) => `${k} ${v}%`);

  // GWAS sample sizes
  const gwasCount = r.ancestry_distribution?.gwas?.count || r.ancestry_gwas?.count || null;
  const devCount = r.ancestry_distribution?.dev?.count || null;
  const evalCount = r.ancestry_distribution?.eval?.count || null;

  // Sample details
  const sampleInfo = r.samples_variants?.[0] || {};
  const sampleN = sampleInfo.sample_number || gwasCount;
  const sampleCases = sampleInfo.sample_cases;
  const sampleControls = sampleInfo.sample_controls;

  // Has rsID column (check from ftp file columns — we'd need to inspect the file)
  // For now, approximate: if variants < 100 and weight_type is NR/OR, likely has rsIDs
  const hasRsIds = r.has_rsids ?? null; // will be null for most

  return {
    id: r.id,
    name: r.name,
    trait_reported: r.trait_reported,
    trait_efo: r.trait_efo,
    variants_number: r.variants_number,
    weight_type: r.weight_type,
    method_name: r.method_name,
    publication: {
      author: pub.firstauthor || '',
      year: pub.date_publication ? pub.date_publication.substring(0, 4) : '',
      journal: pub.journal || '',
      doi: pub.doi,
      pmid: pub.PMID,
    },
    ancestry_gwas_tags: ancestryTags,
    ancestry_eval_tags: ancestryEvalTags,
    gwas_sample_count: sampleN,
    gwas_cases: sampleCases,
    gwas_controls: sampleControls,
    available_builds: builds,
    date_release: r.date_release,
  };
}

export default function PGSSelector({ selectedPgsIds, onAdd, onRemove, onClear }) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [autocomplete, setAutocomplete] = useState([]);
  const [showAC, setShowAC] = useState(false);
  const [activeFilter, setActiveFilter] = useState(null);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [highlightIdx, setHighlightIdx] = useState(-1);

  const acTimerRef = useRef(null);
  const searchBarRef = useRef(null);

  // Debounced autocomplete
  useEffect(() => {
    if (acTimerRef.current) clearTimeout(acTimerRef.current);

    if (query.length < 2) {
      setAutocomplete([]);
      setShowAC(false);
      return;
    }

    acTimerRef.current = setTimeout(async () => {
      try {
        const data = await pgsApi.autocomplete(query);
        const items = flattenAutocomplete(data);
        setAutocomplete(items);
        setShowAC(items.length > 0);
        setHighlightIdx(-1);
      } catch {
        setAutocomplete([]);
      }
    }, 300);

    return () => {
      if (acTimerRef.current) clearTimeout(acTimerRef.current);
    };
  }, [query]);

  // Close autocomplete on outside click
  useEffect(() => {
    const handler = (e) => {
      if (searchBarRef.current && !searchBarRef.current.contains(e.target)) {
        setShowAC(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const doSearch = useCallback(async (q) => {
    if (!q.trim()) return;
    setLoading(true);
    setSearched(true);
    setShowAC(false);
    try {
      const data = await pgsApi.search(q);
      const list = Array.isArray(data) ? data : (data?.results || []);
      setResults(list.map(normalizeResult));
    } catch (err) {
      console.error('Search failed:', err);
      setResults([]);
    } finally {
      setLoading(false);
    }
  }, []);

  const handleKeyDown = (e) => {
    if (showAC && autocomplete.length > 0) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setHighlightIdx((i) => Math.min(i + 1, autocomplete.length - 1));
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        setHighlightIdx((i) => Math.max(i - 1, 0));
      } else if (e.key === 'Enter' && highlightIdx >= 0) {
        e.preventDefault();
        handleACClick(autocomplete[highlightIdx]);
        return;
      }
    }
    if (e.key === 'Enter') {
      doSearch(query);
    }
    if (e.key === 'Escape') {
      setShowAC(false);
    }
  };

  const handleACClick = (item) => {
    const searchTerm = item.label || item.id;
    setQuery(searchTerm);
    setShowAC(false);
    doSearch(searchTerm);
  };

  const handleFilterClick = (f) => {
    if (activeFilter === f) {
      setActiveFilter(null);
      return;
    }
    setActiveFilter(f);
    setQuery(f);
    doSearch(f);
  };

  const isSelected = (pgsId) => selectedPgsIds.some((p) => p.id === pgsId);

  const toggleSelect = (pgs) => {
    if (isSelected(pgs.id)) {
      onRemove(pgs.id);
    } else {
      onAdd(pgs);
    }
  };

  return (
    <div>
      {/* Search bar */}
      <div className="search-bar-wrap" ref={searchBarRef}>
        <span className="search-icon">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <circle cx="11" cy="11" r="8" />
            <path d="M21 21l-4.35-4.35" />
          </svg>
        </span>
        <input
          className="input"
          type="text"
          placeholder="Search by trait, disease, PGS ID (e.g. breast cancer, PGS000004)..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          onFocus={() => { if (autocomplete.length > 0 && query.length >= 2) setShowAC(true); }}
        />
        {showAC && autocomplete.length > 0 && (
          <div className="autocomplete-dropdown">
            {autocomplete.map((item, idx) => (
              <div
                key={`${item.type}-${item.id}-${idx}`}
                className={`autocomplete-item ${idx === highlightIdx ? 'highlighted' : ''}`}
                onMouseEnter={() => setHighlightIdx(idx)}
                onClick={() => handleACClick(item)}
              >
                <span className={`ac-type-badge ${item.type === 'trait' ? 'ac-trait' : 'ac-score'}`}>
                  {item.type === 'trait' ? 'Trait' : 'PGS'}
                </span>
                <span className="ac-label">{item.label}</span>
                <span className="ac-detail">{item.detail}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Quick filters */}
      <div className="filter-tags">
        {QUICK_FILTERS.map((f) => (
          <button
            key={f}
            className={`filter-tag ${activeFilter === f ? 'active' : ''}`}
            onClick={() => handleFilterClick(f)}
          >
            {f}
          </button>
        ))}
      </div>

      {/* Results */}
      {loading ? (
        <div style={{ textAlign: 'center', padding: '40px 0' }}>
          <div className="spinner" />
          <p className="loading-text">Searching PGS Catalog...</p>
        </div>
      ) : results.length > 0 ? (
        <div className="card-grid">
          {results.map((pgs) => (
            <div key={pgs.id} className="card pgs-result-card" onClick={() => toggleSelect(pgs)}>
              <div className="pgs-card-header">
                <input
                  type="checkbox"
                  className="pgs-checkbox"
                  checked={isSelected(pgs.id)}
                  onChange={() => toggleSelect(pgs)}
                  onClick={(e) => e.stopPropagation()}
                />
                <div className="pgs-card-title">
                  <div className="pgs-id">{pgs.id}</div>
                  <div className="pgs-trait">{pgs.trait_reported}</div>
                  {pgs.name && <div className="pgs-name">{pgs.name}</div>}
                </div>
              </div>

              {pgs.publication?.author && (
                <div className="pgs-publication">
                  {pgs.publication.author}
                  {pgs.publication.year && ` (${pgs.publication.year})`}
                  {pgs.publication.journal && ` - ${pgs.publication.journal}`}
                </div>
              )}

              {/* Variant & method info */}
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginBottom: 6 }}>
                {pgs.variants_number != null && (
                  <span className="badge badge-gray">
                    {Number(pgs.variants_number).toLocaleString()} variants
                  </span>
                )}
                {pgs.weight_type && (
                  <span className="badge badge-gray">{pgs.weight_type}</span>
                )}
                {pgs.method_name && (
                  <span className="badge badge-gray">{pgs.method_name}</span>
                )}
              </div>

              {/* Ancestry — GWAS development */}
              {pgs.ancestry_gwas_tags?.length > 0 && (
                <div style={{ fontSize: 11, color: '#8b949e', marginBottom: 4 }}>
                  <span style={{ color: '#6e7681' }}>GWAS:</span>{' '}
                  {pgs.ancestry_gwas_tags.map(a => (
                    <span key={a} className="badge badge-purple" style={{ fontSize: 10, padding: '1px 5px' }}>{a}</span>
                  ))}
                  {pgs.gwas_sample_count && (
                    <span style={{ marginLeft: 6, color: '#6e7681' }}>
                      n={Number(pgs.gwas_sample_count).toLocaleString()}
                      {pgs.gwas_cases && ` (${Number(pgs.gwas_cases).toLocaleString()} cases)`}
                    </span>
                  )}
                </div>
              )}

              {/* Ancestry — evaluation */}
              {pgs.ancestry_eval_tags?.length > 0 && (
                <div style={{ fontSize: 11, color: '#8b949e', marginBottom: 4 }}>
                  <span style={{ color: '#6e7681' }}>Eval:</span>{' '}
                  {pgs.ancestry_eval_tags.map(a => (
                    <span key={a} className="badge badge-blue" style={{ fontSize: 10, padding: '1px 5px' }}>{a}</span>
                  ))}
                </div>
              )}

              {/* Builds + release */}
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                {pgs.available_builds?.map(b => (
                  <span key={b} className="badge badge-green" style={{ fontSize: 10 }}>{b}</span>
                ))}
                {pgs.date_release && (
                  <span style={{ fontSize: 10, color: '#484f58' }}>Released: {pgs.date_release}</span>
                )}
              </div>
            </div>
          ))}
        </div>
      ) : searched ? (
        <div className="empty-state">
          <h3>No results found</h3>
          <p>Try a different search term or click one of the quick filters above.</p>
        </div>
      ) : (
        <div className="empty-state">
          <h3>Search the PGS Catalog</h3>
          <p>Enter a trait, disease name, or PGS ID to find polygenic scores.</p>
          <p style={{ color: '#484f58', fontSize: '0.8rem', marginTop: 8 }}>
            Try: "breast cancer", "alzheimer", "PGS000004", "EFO_0000305"
          </p>
        </div>
      )}

      {/* Selected PGS chips */}
      {selectedPgsIds.length > 0 && (
        <div style={{
          marginTop: 16,
          padding: '12px 16px',
          background: '#161b22',
          border: '1px solid #30363d',
          borderRadius: 8,
        }}>
          <div style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: 8,
          }}>
            <span style={{ fontSize: 13, fontWeight: 600, color: '#e6edf3' }}>
              {selectedPgsIds.length} score{selectedPgsIds.length !== 1 ? 's' : ''} selected
            </span>
            <button
              className="btn btn-sm"
              style={{ fontSize: 11, padding: '2px 8px', color: '#f85149', borderColor: 'rgba(248,81,73,0.3)' }}
              onClick={onClear}
            >
              Clear All
            </button>
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {selectedPgsIds.map((p) => (
              <span key={p.id} className="selection-chip">
                {p.id}
                {p.trait_reported && (
                  <span style={{ color: '#8b949e', marginLeft: 4, fontSize: 11 }}>
                    {p.trait_reported.length > 25 ? p.trait_reported.substring(0, 25) + '...' : p.trait_reported}
                  </span>
                )}
                <span
                  className="chip-remove"
                  onClick={(e) => { e.stopPropagation(); onRemove(p.id); }}
                >
                  &times;
                </span>
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
