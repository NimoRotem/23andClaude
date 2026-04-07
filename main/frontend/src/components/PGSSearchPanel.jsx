import { useState, useEffect, useRef, useCallback } from 'react';
import { pgsApi } from '../api.js';
import { useAppState, useAppDispatch } from '../context.jsx';

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
    ancestry_tags: ancestryTags,
    available_builds: builds,
    date_release: r.date_release,
  };
}

export default function PGSSearchPanel() {
  const { selectedPgsIds } = useAppState();
  const dispatch = useAppDispatch();

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
      dispatch({ type: 'REMOVE_PGS', payload: pgs.id });
    } else {
      dispatch({ type: 'ADD_PGS', payload: pgs });
    }
  };

  const goToRun = () => {
    dispatch({ type: 'SET_TAB', payload: 3 });
  };

  return (
    <div style={{ paddingBottom: selectedPgsIds.length > 0 ? 70 : 0 }}>
      <div className="section-header">
        <h2 className="section-title">PGS Catalog Search</h2>
      </div>

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
          placeholder="Search by trait, disease, PGS ID (e.g. breast cancer, PGS000004, EFO_0000305)..."
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

              <div className="pgs-card-meta">
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
                {pgs.ancestry_tags?.map((a) => (
                  <span key={a} className="badge badge-purple">{a}</span>
                ))}
                {pgs.available_builds?.map((b) => (
                  <span key={b} className="badge badge-blue">{b}</span>
                ))}
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
          <p style={{ color: 'var(--text-dim)', fontSize: '0.8rem', marginTop: 8 }}>
            Try: "breast cancer", "alzheimer", "PGS000004", "EFO_0000305"
          </p>
        </div>
      )}

      {/* Sticky selection bar */}
      {selectedPgsIds.length > 0 && (
        <div className="selection-bar">
          <span className="selection-count">{selectedPgsIds.length} score{selectedPgsIds.length !== 1 ? 's' : ''} selected</span>
          <div className="selection-chips">
            {selectedPgsIds.map((p) => (
              <span key={p.id} className="selection-chip">
                {p.id}
                <span
                  className="chip-remove"
                  onClick={(e) => { e.stopPropagation(); dispatch({ type: 'REMOVE_PGS', payload: p.id }); }}
                >
                  &times;
                </span>
              </span>
            ))}
          </div>
          <button className="btn btn-accent" onClick={goToRun}>
            Configure Run &rarr;
          </button>
        </div>
      )}
    </div>
  );
}
