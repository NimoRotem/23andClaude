import { useEffect } from 'react';
import { filesApi, ancestryApi } from '../../api.js';
import { useRunPGS, useRunPGSDispatch } from '../RunPGSState.jsx';
import { fmtBytes, fileTypeBadgeInfo } from '../utils.js';

export default function FileSelectionStep() {
  const { scannedFiles, filesLoading, filesError, selectedFiles, ancestryData } = useRunPGS();
  const dispatch = useRunPGSDispatch();

  useEffect(() => {
    filesApi.scan()
      .then(data => {
        const list = Array.isArray(data) ? data : (data?.files || []);
        dispatch({ type: 'SET_SCANNED_FILES', payload: list.filter(f => (f.file_type || f.type) !== 'fastq') });
      })
      .catch(err => dispatch({ type: 'SET_FILES_ERROR', payload: err.message }));
  }, [dispatch]);

  useEffect(() => {
    ancestryApi.all().then(data => {
      const map = {};
      for (const s of data) map[s.sample_id] = s;
      dispatch({ type: 'SET_ANCESTRY_DATA', payload: map });
    }).catch(() => {});
  }, [dispatch]);

  const isSelected = (f) => selectedFiles.some(s => s.path === f.path);

  return (
    <div>
      <div className="rpgs-section">
        <h3 className="rpgs-section-title">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M13 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V9z" />
            <polyline points="13 2 13 9 20 9" />
          </svg>
          Select Genomic Files
          {selectedFiles.length > 0 && (
            <span className="badge badge-blue" style={{ marginLeft: 4 }}>
              {selectedFiles.length} selected
            </span>
          )}
        </h3>
        <p style={{ color: '#8b949e', fontSize: 13, margin: '0 0 16px' }}>
          Choose one or more VCF, gVCF, or BAM files to score against PGS models.
        </p>

        {filesLoading && (
          <div style={{ textAlign: 'center', padding: '30px 0' }}>
            <div className="spinner" style={{ margin: '0 auto 8px' }} />
            <p style={{ color: '#8b949e', fontSize: 13 }}>Scanning files...</p>
          </div>
        )}

        {filesError && (
          <p style={{ color: '#f85149', fontSize: 13 }}>Error: {filesError}</p>
        )}

        {!filesLoading && !filesError && scannedFiles.length === 0 && (
          <div style={{ textAlign: 'center', padding: '30px 0', color: '#8b949e' }}>
            No genomic files found. Register a VCF or run the BAM-to-VCF pipeline first.
          </div>
        )}

        {!filesLoading && scannedFiles.length > 0 && (
          <div className="rpgs-file-grid">
            {scannedFiles.map(file => {
              const sel = isSelected(file);
              const badge = fileTypeBadgeInfo(file.file_type || file.type);
              const name = file.sample_name || file.filename || file.path.split('/').pop();
              const ancestry = ancestryData[file.sample_name];
              return (
                <div
                  key={file.path}
                  className={`rpgs-file-card ${sel ? 'selected' : ''}`}
                  onClick={() => dispatch({ type: 'TOGGLE_FILE', payload: file })}
                >
                  <span className="rpgs-file-badge" style={{ background: badge.bg, color: badge.color }}>
                    {badge.label}
                  </span>
                  <div className="rpgs-file-info">
                    <div className="rpgs-file-name">{name}</div>
                    <div className="rpgs-file-meta">
                      {fmtBytes(file.file_size_bytes)}
                      {ancestry && ` · ${ancestry.primary_ancestry}`}
                      {file.variant_count && ` · ${file.variant_count.toLocaleString()} variants`}
                    </div>
                  </div>
                  <div className="rpgs-file-check">
                    {sel && (
                      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                        <polyline points="20 6 9 17 4 12" />
                      </svg>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      <div className="rpgs-step-nav">
        <div />
        <button
          className="btn btn-primary"
          disabled={selectedFiles.length === 0}
          onClick={() => dispatch({ type: 'SET_STEP', payload: 1 })}
        >
          Continue — Select PGS Scores &rarr;
        </button>
      </div>
    </div>
  );
}
