import { useState, useEffect, useCallback, useRef } from 'react';
import ConfidenceDots from './ConfidenceDots.jsx';

const BASE = '/genomics/api/checklist';

/* ── Lightweight markdown renderer ─────────────────────────── */

function renderInline(text) {
  // Escape HTML entities first
  let result = text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  // Replace markdown links [text](url) with <a> tags, using a placeholder to protect them
  const linkPlaceholders = [];
  result = result.replace(/\[([^\]]+)\]\((https?:\/\/[^)]+)\)/g, (_, label, url) => {
    const idx = linkPlaceholders.length;
    linkPlaceholders.push(`<a href="${url}" target="_blank" rel="noopener noreferrer" style="color:#58a6ff;text-decoration:none">${label}</a>`);
    return `\x00LINK${idx}\x00`;
  });

  // Apply inline formatting
  result = result
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/\*([^*]+)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g, '<code style="background:#21262d;padding:1px 5px;border-radius:3px;font-size:12px;color:#e6edf3">$1</code>');

  // Auto-link bare URLs (only those not already converted to placeholders)
  result = result.replace(/(https?:\/\/[^\s<\x00]+)/g, (m, url) => {
    // Strip trailing punctuation that's not part of the URL
    const clean = url.replace(/[)"',;.]+$/, '');
    return `<a href="${clean}" target="_blank" rel="noopener noreferrer" style="color:#58a6ff;text-decoration:none;font-size:12px">${clean.length > 60 ? clean.slice(0, 57) + '...' : clean}</a>`;
  });

  // Restore link placeholders
  for (let i = 0; i < linkPlaceholders.length; i++) {
    result = result.replace(`\x00LINK${i}\x00`, linkPlaceholders[i]);
  }

  return result;
}

function PercentilePills({ scores }) {
  if (!scores || Object.keys(scores).length === 0) return null;
  return (
    <span style={{ display: 'inline-flex', gap: 3, marginLeft: 6 }}>
      {Object.entries(scores).map(([sample, data]) => {
        const pct = data.percentile;
        if (pct == null) return null;
        const color = pct >= 90 ? '#f85149' : pct >= 75 ? '#d29922' : pct >= 25 ? '#8b949e' : '#3fb950';
        return (
          <span key={sample} title={`${sample}: ${pct}th percentile (Z=${data.z_score || '?'})`} style={{
            fontSize: 10, padding: '1px 5px', borderRadius: 6,
            background: `${color}18`, color, border: `1px solid ${color}33`,
            fontWeight: 600, fontFamily: 'monospace',
          }}>
            {sample.slice(0, 3)}:{pct.toFixed(0)}%
          </span>
        );
      })}
    </span>
  );
}

function TraitCard({ title, description, table, sectionId, checkedMap, notesMap, scoresMap, cmdResults, estimatesMap = {}, samples, globalSample, onViewReport, onRun, onRunCommand, onRunSection, runningPgs, runningCmds }) {
  const [open, setOpen] = useState(!title);
  const selectedSample = globalSample || '';

  const parseRow = (row) => row.split('|').slice(1, -1).map(c => c.trim());
  const headers = table.length > 0 ? parseRow(table[0]) : [];
  const dataRows = table.length > 2 ? table.slice(2).map(parseRow) : [];

  // Detect if this is a PGS table or a command table
  const pgsColIdx = headers.findIndex(h => /pgs.id/i.test(h));
  const varColIdx = headers.findIndex(h => /variant/i.test(h));
  const popColIdx = headers.findIndex(h => /^pop(ulation)?$/i.test(h.replace(/\*\*/g, '').trim()));
  const doneColIdx = headers.findIndex(h => /done/i.test(h));
  // Find the "name" column: Check, Condition, Trait, Marker, Disease, Gene, Database, Tool, etc.
  // Find the "name" column — the first non-Done column with a text label
  const nameColIdx = (() => {
    // Try specific known header names first
    const idx = headers.findIndex(h => /^(check|condition|trait|marker|disease|analysis|decision|database|tool|pathway|gene)$/i.test(h.replace(/\*\*/g, '').trim()));
    if (idx >= 0) return idx;
    // Fallback: first column after Done
    return doneColIdx >= 0 ? doneColIdx + 1 : (headers.length > 1 ? 1 : 0);
  })();
  const isPgsTable = pgsColIdx >= 0;

  let pgsCount = 0;
  let totalVariants = 0;
  let checkedCount = 0;
  let hasReport = false;
  const allPgsIds = [];
  const allCmdRows = []; // {ri, checkName}

  // Per-row data for rendering
  const rowData = dataRows.map((row, ri) => {
    let pgsId = null;
    for (const cell of row) {
      const m = cell.match(/(PGS\d{6,})/);
      if (m) { pgsId = m[1]; break; }
    }
    if (pgsId) { pgsCount++; allPgsIds.push(pgsId); }

    if (varColIdx >= 0 && row[varColIdx]) {
      const num = parseInt(row[varColIdx].replace(/,/g, ''));
      if (!isNaN(num)) totalVariants += num;
    }

    // Extract a human-readable name from the first meaningful text column after Done
    const nameIdx = nameColIdx >= 0 ? nameColIdx : (doneColIdx >= 0 ? doneColIdx + 1 : (row.length > 1 ? 1 : 0));
    const checkName = (row[nameIdx] || '').replace(/\*\*/g, '').replace(/\(.*?\)/g, '').trim();

    // Every non-PGS row is a command row
    if (!pgsId && checkName) {
      allCmdRows.push({ ri, checkName });
    }

    const itemId = sectionId ? `${sectionId}:${ri}` : null;
    if (itemId && checkedMap[itemId]) checkedCount++;
    if (itemId && notesMap[itemId] && notesMap[itemId].includes('Report')) hasReport = true;

    // Extract population from Pop column (EUR, AFR, Multi, etc.)
    const popRaw = popColIdx >= 0 && row[popColIdx] ? row[popColIdx].replace(/\*\*/g, '').trim() : '';
    const pop = /^(EUR|AFR|EAS|SAS|AMR)/i.test(popRaw) ? popRaw.split(/[\s(]/)[0].toUpperCase() : null;

    return { pgsId, checkName, itemId, pop };
  });

  const totalItems = dataRows.length;
  const allDone = totalItems > 0 && checkedCount === totalItems;
  const someDone = checkedCount > 0;
  const isRunnable = allPgsIds.length > 0 || allCmdRows.length > 0;

  const varStr = totalVariants >= 1e6 ? `${(totalVariants / 1e6).toFixed(1)}M` : totalVariants >= 1e3 ? `${(totalVariants / 1e3).toFixed(0)}K` : `${totalVariants}`;

  const [runningSection, setRunningSection] = useState(false);

  const handleRunAll = async (e) => {
    e.stopPropagation();
    if (!selectedSample) { alert('Please select a sample file from the dropdown at the top of the Checklist before running.'); return; }
    if (runningSection) return;
    const sample = samples.find(s => s.path === selectedSample);
    if (!sample) return;

    const items = rowData.map(rd => ({
      item_id: rd.itemId,
      check_name: rd.checkName,
      ...(rd.pgsId ? { pgs_id: rd.pgsId } : {}),
    })).filter(i => i.check_name || i.pgs_id);

    if (items.length === 0) return;

    // Check if this section already has results for the selected file
    const existingResults = items.filter(i => i.item_id && (checkedMap[i.item_id] || cmdResults[i.item_id]));
    if (existingResults.length > 0) {
      const confirmed = window.confirm(
        `This section already has ${existingResults.length} result(s) for "${sample.name} [${sample.type.toUpperCase()}]".\n\n` +
        `Re-running will delete the previous report and overwrite existing results.\n\nContinue?`
      );
      if (!confirmed) return;
    }

    if (onRunSection) {
      setRunningSection(true);
      await onRunSection(sectionId, title, items, sample);
      setRunningSection(false);
    }
  };

  const handleRunOne = (pgsId, pop) => {
    if (!selectedSample) { alert('Please select a sample file from the dropdown at the top of the Checklist before running.'); return; }
    const sample = samples.find(s => s.path === selectedSample);
    if (sample && onRun) onRun([pgsId], sample, pop);
  };

  const handleRunCmd = (ri, cmd, checkName) => {
    if (!selectedSample) { alert('Please select a sample file from the dropdown at the top of the Checklist before running.'); return; }
    if (!onRunCommand) return;
    const sample = samples.find(s => s.path === selectedSample);
    if (!sample) return;
    const itemId = sectionId ? `${sectionId}:${ri}` : null;
    if (!itemId) return;

    // Check if this item already has results
    if (checkedMap[itemId] || cmdResults[itemId]) {
      const confirmed = window.confirm(
        `"${checkName}" already has results for "${sample.name} [${sample.type.toUpperCase()}]".\n\n` +
        `Re-running will delete the previous report and overwrite the result.\n\nContinue?`
      );
      if (!confirmed) return;
    }

    onRunCommand(itemId, cmd, sample, checkName);
  };

  return (
    <div style={{ marginBottom: 6, border: '1px solid #21262d', borderRadius: 8, overflow: 'hidden', background: '#161b22' }}>
      <div onClick={() => setOpen(!open)} style={{
        display: 'flex', alignItems: 'center', gap: 10, padding: '10px 14px', cursor: 'pointer', userSelect: 'none',
        borderLeft: `3px solid ${allDone ? '#3fb950' : someDone ? '#58a6ff' : '#30363d'}`,
      }}>
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#8b949e" strokeWidth="2"
          style={{ transition: 'transform 0.15s', transform: open ? 'rotate(90deg)' : 'rotate(0deg)', flexShrink: 0 }}>
          <path d="M9 18l6-6-6-6" />
        </svg>
        <span style={{ fontWeight: 600, color: '#e6edf3', fontSize: 14, flex: 1 }}>{title}</span>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
          {checkedCount > 0 && (
            <span style={{ fontSize: 11, padding: '2px 6px', borderRadius: 8, background: allDone ? '#23863622' : '#388bfd15', color: allDone ? '#3fb950' : '#58a6ff' }}>
              {checkedCount}/{dataRows.length} done
            </span>
          )}
          {pgsCount > 0 && (
            <span style={{ fontSize: 11, color: '#8b949e' }}>{pgsCount} PGS</span>
          )}
          {allCmdRows.length > 0 && pgsCount === 0 && (
            <span style={{ fontSize: 11, color: '#8b949e' }}>{allCmdRows.length} checks</span>
          )}
          {totalVariants > 0 && (
            <span style={{ fontSize: 11, color: '#484f58' }}>{varStr} variants</span>
          )}
          {/* Show Section Report button */}
          {someDone && (() => {
            // Look for section report first, then fall back to any item report
            const sectionKey = `${sectionId}:_section_report`;
            const sectionNote = (notesMap || {})[sectionKey];
            const reportLink = sectionNote || Object.values(notesMap || {}).find(v =>
              typeof v === 'string' && v.includes('Report') && v.includes('section_')
            );
            if (!reportLink) return null;
            const match = String(reportLink).match(/\/report\/([^)]+)/);
            return match ? (
              <span onClick={(e) => { e.stopPropagation(); onViewReport && onViewReport(match[1]); }}
                style={{ fontSize: 11, padding: '2px 8px', borderRadius: 8, background: '#23863622', color: '#3fb950', cursor: 'pointer', border: '1px solid #23863644', fontWeight: 600 }}>
                Section Report
              </span>
            ) : null;
          })()}
        </div>
      </div>
      {!open && description && (
        <div style={{ padding: '0 14px 10px 30px', fontSize: 12, color: '#6e7681', lineHeight: 1.5 }}>
          {description}
        </div>
      )}
      {open && (
        <div style={{ padding: '4px 14px 10px', borderTop: '1px solid #21262d' }}>
          {description && (
            <p style={{ fontSize: 13, color: '#8b949e', margin: '6px 0 8px', lineHeight: 1.5 }}>{description}</p>
          )}
          {/* Run Section button — uses global sample selector */}
          {(allPgsIds.length > 0 || allCmdRows.length > 0) && (
            <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 10 }}>
              <button onClick={handleRunAll} disabled={!selectedSample || runningSection}
                style={{
                  padding: '5px 14px', fontSize: 12, borderRadius: 6, border: '1px solid #2ea043', cursor: 'pointer',
                  background: selectedSample && !runningSection ? '#238636' : '#21262d', color: selectedSample && !runningSection ? '#fff' : '#484f58',
                }}>
                {runningSection ? 'Running...' : selectedSample ? `Run Section for ${selectedSample} (${dataRows.length} items)` : 'Select a sample above to run'}
              </button>
            </div>
          )}
          {dataRows.length > 0 && (
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                <thead>
                  <tr>
                    {headers.map((h, hi) => (
                      <th key={hi} style={{
                        padding: '6px 8px', textAlign: 'left', color: '#8b949e', fontWeight: 600,
                        borderBottom: '2px solid #21262d', fontSize: 12,
                        ...(/done/i.test(h) ? { width: 36, textAlign: 'center' } : {}),
                      }}>
                        <span dangerouslySetInnerHTML={{ __html: renderInline(h) }} />
                      </th>
                    ))}
                    <th style={{ padding: '6px 8px', color: '#8b949e', fontWeight: 600, borderBottom: '2px solid #21262d', fontSize: 12, width: 60 }}>Results</th>
                    <th style={{ padding: '6px 8px', borderBottom: '2px solid #21262d', width: 40 }}></th>
                  </tr>
                </thead>
                <tbody>
                  {dataRows.map((row, ri) => {
                    const rd = rowData[ri] || {};
                    const itemId = rd.itemId;
                    const rowPgsId = rd.pgsId;
                    const rowCheckName = rd.checkName;
                    const isChecked = itemId && checkedMap[itemId];
                    const note = itemId && notesMap[itemId];
                    const rowHasReport = note && note.includes('Report');
                    const rowScores = itemId && scoresMap ? scoresMap[itemId] : null;
                    const doneIdx = doneColIdx;

                    return (
                      <tr key={ri} style={{
                        borderBottom: '1px solid #161b22',
                        background: isChecked ? 'rgba(63,185,80,0.04)' : 'transparent',
                      }}
                        onMouseEnter={e => { if (!isChecked) e.currentTarget.style.background = '#1c2128'; }}
                        onMouseLeave={e => e.currentTarget.style.background = isChecked ? 'rgba(63,185,80,0.04)' : 'transparent'}>
                        {row.map((cell, ci) => {
                          if (ci === doneIdx) {
                            return (
                              <td key={ci} style={{ padding: '5px 8px', textAlign: 'center', fontSize: 14 }}>
                                {isChecked ? '\u2705' : '\u2B1C'}
                              </td>
                            );
                          }
                          const isNameCol = ci === (doneIdx >= 0 ? doneIdx + 1 : 0);
                          return (
                            <td key={ci} style={{ padding: '5px 8px', color: isChecked ? '#8b949e' : '#c9d1d9', maxWidth: 300 }}>
                              <span dangerouslySetInnerHTML={{ __html: renderInline(cell) }} />
                              {isNameCol && rowHasReport && (
                                <span onClick={(e) => {
                                  e.stopPropagation();
                                  const match = note.match(/\/report\/([^)]+)/);
                                  if (match && onViewReport) onViewReport(match[1]);
                                }} style={{
                                  marginLeft: 6, fontSize: 10, padding: '1px 6px', borderRadius: 8,
                                  background: 'rgba(63,185,80,0.12)', color: '#3fb950',
                                  border: '1px solid rgba(63,185,80,0.25)', cursor: 'pointer',
                                }}>Report</span>
                              )}
                            </td>
                          );
                        })}
                        {/* Inline results + Report badge */}
                        <td style={{ padding: '4px 6px' }}>
                          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, flexWrap: 'wrap' }}>
                            {rowScores && <PercentilePills scores={rowScores} />}
                            {!rowScores && itemId && cmdResults && cmdResults[itemId] && (
                              <span style={{ fontSize: 10, color: cmdResults[itemId].exit_code === 0 ? '#3fb950' : cmdResults[itemId].exit_code === -1 ? '#8b949e' : '#f85149', fontFamily: 'monospace' }}
                                title={cmdResults[itemId].output}>
                                {cmdResults[itemId].exit_code === -1 ? 'manual' : `${cmdResults[itemId].output?.split('\n')[0]?.slice(0, 35) || 'done'}`}
                              </span>
                            )}
                            {note && note.includes('Report') && (
                              <span onClick={(e) => {
                                e.stopPropagation();
                                const match = note.match(/\/report\/([^)]+)/);
                                if (match && onViewReport) onViewReport(match[1]);
                              }} style={{
                                fontSize: 9, padding: '1px 5px', borderRadius: 6,
                                background: 'rgba(63,185,80,0.12)', color: '#3fb950',
                                border: '1px solid rgba(63,185,80,0.25)', cursor: 'pointer',
                              }}>Report</span>
                            )}
                          </span>
                        </td>
                        {/* Run button — every row is runnable */}
                        <td style={{ padding: '4px 6px', textAlign: 'center', whiteSpace: 'nowrap' }}>
                          {(rowPgsId || rowCheckName) && itemId && estimatesMap[itemId] && (
                            <span style={{ fontSize: 10, color: '#6e7681', marginRight: 6 }}
                              title={`Estimated runtime (${estimatesMap[itemId].seconds}s) — based on server resources and PGS variant counts`}>
                              {estimatesMap[itemId].label}
                            </span>
                          )}
                          {selectedSample && (rowPgsId || rowCheckName) && !(runningPgs[rowPgsId] || (runningCmds && runningCmds[itemId])) && (
                            <span onClick={() => {
                              if (rowPgsId) { handleRunOne(rowPgsId, rd.pop); }
                              else { handleRunCmd(ri, rowCheckName, rowCheckName); }
                            }}
                              style={{ cursor: 'pointer', fontSize: 14, color: rowPgsId ? '#3fb950' : '#58a6ff' }}
                              title={rowPgsId ? `Run ${rowPgsId} for ${selectedSample}` : `Run "${rowCheckName}" for ${selectedSample}`}>
                              &#9654;
                            </span>
                          )}
                          {!selectedSample && (rowPgsId || rowCheckName) && (
                            <span style={{ fontSize: 11, color: '#30363d' }} title="Select a sample first">&#9654;</span>
                          )}
                          {(runningPgs[rowPgsId] || (runningCmds && runningCmds[itemId])) && (
                            <span style={{ fontSize: 11, color: '#d29922' }}>...</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function SectionRunBar({ pgsIds, samples, onRun, runningPgs }) {
  const [sample, setSample] = useState('');
  if (!pgsIds.length || !samples.length) return null;
  const anyRunning = pgsIds.some(id => runningPgs[id]);
  return (
    <div style={{ display: 'inline-flex', gap: 6, alignItems: 'center', marginLeft: 12 }} onClick={e => e.stopPropagation()}>
      <select value={sample} onChange={e => setSample(e.target.value)}
        style={{ padding: '3px 6px', fontSize: 11, background: '#0d1117', border: '1px solid #30363d', color: '#c9d1d9', borderRadius: 4 }}>
        <option value="">Sample...</option>
        {samples.map(s => <option key={s.name} value={s.name}>{s.name}</option>)}
      </select>
      <button disabled={!sample || anyRunning} onClick={() => {
        const s = samples.find(x => x.name === sample);
        if (s && onRun) onRun(pgsIds, s);
      }} style={{
        padding: '3px 8px', fontSize: 11, borderRadius: 4, cursor: sample && !anyRunning ? 'pointer' : 'default',
        border: '1px solid #2ea043', background: sample && !anyRunning ? '#238636' : '#21262d',
        color: sample && !anyRunning ? '#fff' : '#484f58',
      }}>
        {anyRunning ? `Running...` : `Run all ${pgsIds.length} PGS`}
      </button>
    </div>
  );
}

function RenderedMarkdown({ markdown, checkedMap = {}, notesMap = {}, scoresMap = {}, cmdResults = {}, estimatesMap = {}, samples = [], globalSample = '', onViewReport, onRun, onRunCommand, onRunSection, runningPgs = {}, runningCmds = {} }) {
  const lines = markdown.split('\n');
  const elements = [];
  let currentSectionId = null;
  let tableRowIdx = -1;
  let i = 0;

  // Pre-scan to collect PGS IDs per H2 section for section-level run buttons
  const sectionPgsIds = {};
  let scanSection = null;
  for (const l of lines) {
    const h2m = l.match(/^## (\d+)\./);
    if (h2m) { scanSection = h2m[1]; sectionPgsIds[scanSection] = []; continue; }
    if (scanSection && l.includes('PGS')) {
      const matches = l.match(/PGS\d{6,}/g);
      if (matches) {
        for (const m of matches) {
          if (!sectionPgsIds[scanSection].includes(m)) sectionPgsIds[scanSection].push(m);
        }
      }
    }
  }

  while (i < lines.length) {
    const line = lines[i];

    // H1
    if (line.startsWith('# ') && !line.startsWith('## ')) {
      elements.push(<h1 key={i} style={{ color: '#e6edf3', fontSize: 24, fontWeight: 700, margin: '24px 0 8px', borderBottom: '1px solid #21262d', paddingBottom: 8 }}>{line.slice(2)}</h1>);
      i++; continue;
    }

    // H2
    if (line.startsWith('## ')) {
      const numMatch = line.match(/^## (\d+)\./);
      let sectionNum = null;
      if (numMatch) {
        currentSectionId = `s${numMatch[1]}`;
        sectionNum = numMatch[1];
        tableRowIdx = -1;
      }
      const secPgs = sectionNum ? (sectionPgsIds[sectionNum] || []) : [];
      elements.push(
        <div key={i} style={{ display: 'flex', alignItems: 'center', margin: '28px 0 8px', borderBottom: '1px solid #21262d', paddingBottom: 6 }}>
          <h2 style={{ color: '#e6edf3', fontSize: 18, fontWeight: 600, margin: 0, flex: 1 }}>{line.slice(3)}</h2>
          {secPgs.length > 0 && (
            <SectionRunBar pgsIds={secPgs} samples={samples} onRun={onRun} runningPgs={runningPgs} />
          )}
        </div>
      );
      i++; continue;
    }

    // H3 — collect description + table into a collapsible TraitCard
    if (line.startsWith('### ')) {
      const title = line.slice(4).trim();
      const h3Idx = i;
      if (currentSectionId) {
        const sub = title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
        currentSectionId = currentSectionId.split('-')[0] + '-' + sub;
        tableRowIdx = -1;
      }
      i++;

      // Collect description (> lines)
      let description = '';
      while (i < lines.length && lines[i].startsWith('>')) {
        description += lines[i].replace(/^>\s?/, '') + ' ';
        i++;
      }
      description = description.trim();

      // Skip empty lines
      while (i < lines.length && lines[i].trim() === '') i++;

      // Collect table lines
      const tableLines = [];
      while (i < lines.length && lines[i].startsWith('|')) {
        tableLines.push(lines[i]);
        i++;
      }

      // Skip separator-only tables
      if (tableLines.length >= 2) {
        elements.push(
          <TraitCard key={h3Idx} title={title} description={description}
            table={tableLines} sectionId={currentSectionId}
            checkedMap={checkedMap} notesMap={notesMap} scoresMap={scoresMap} cmdResults={cmdResults}
            estimatesMap={estimatesMap}
            samples={samples} globalSample={globalSample} onViewReport={onViewReport} onRun={onRun} onRunCommand={onRunCommand} onRunSection={onRunSection}
            runningPgs={runningPgs} runningCmds={runningCmds} />
        );
      } else {
        // No table — just render as heading + description
        elements.push(<h3 key={h3Idx} style={{ color: '#c9d1d9', fontSize: 15, fontWeight: 600, margin: '20px 0 6px' }}>{title}</h3>);
        if (description) {
          elements.push(<p key={`desc-${h3Idx}`} style={{ color: '#8b949e', fontSize: 13, margin: '0 0 10px', lineHeight: 1.5 }}>{description}</p>);
        }
      }
      continue;
    }

    // Horizontal rule
    if (/^---+$/.test(line.trim())) {
      elements.push(<hr key={i} style={{ border: 'none', borderTop: '1px solid #21262d', margin: '16px 0' }} />);
      i++; continue;
    }

    // Blockquote
    if (line.startsWith('>')) {
      const quoteLines = [];
      while (i < lines.length && lines[i].startsWith('>')) {
        quoteLines.push(lines[i].replace(/^>\s?/, ''));
        i++;
      }
      elements.push(
        <blockquote key={`bq-${i}`} style={{ borderLeft: '3px solid #30363d', paddingLeft: 14, margin: '8px 0', color: '#8b949e', fontSize: 14, lineHeight: 1.6 }}>
          <span dangerouslySetInnerHTML={{ __html: renderInline(quoteLines.join(' ')) }} />
        </blockquote>
      );
      continue;
    }

    // Table — if it has a Done column, render as a TraitCard for full run support
    if (line.startsWith('|')) {
      const tableLines = [];
      const tableStartIdx = i;
      while (i < lines.length && lines[i].startsWith('|')) {
        tableLines.push(lines[i]);
        i++;
      }
      tableRowIdx = -1;
      if (tableLines.length >= 2) {
        const parseRow = (row) => row.split('|').slice(1, -1).map(c => c.trim());
        const testHeaders = parseRow(tableLines[0]);
        const hasDone = testHeaders.some(h => /done/i.test(h));

        // If table has a Done column, render as TraitCard (with run controls)
        if (hasDone) {
          // Use the last description text we collected (paragraph before the table)
          const lastDesc = '';
          elements.push(
            <TraitCard key={`tbl-${tableStartIdx}`} title="" description={lastDesc}
              table={tableLines} sectionId={currentSectionId}
              checkedMap={checkedMap} notesMap={notesMap} scoresMap={scoresMap} cmdResults={cmdResults}
              estimatesMap={estimatesMap}
              samples={samples} globalSample={globalSample} onViewReport={onViewReport} onRun={onRun} onRunCommand={onRunCommand} onRunSection={onRunSection}
              runningPgs={runningPgs} runningCmds={runningCmds} />
          );
          continue;
        }

        // Non-Done tables: render as plain tables (databases, tools, etc.)
        const headers = testHeaders;
        const dataRows = tableLines.slice(2).map(parseRow);
        const doneColIdx = -1;
        const savedSectionId = currentSectionId;

        elements.push(
          <div key={`tbl-${tableStartIdx}`} style={{ overflowX: 'auto', margin: '8px 0 16px' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr>
                  {headers.map((h, hi) => (
                    <th key={hi} style={{
                      padding: '7px 10px', textAlign: 'left', color: '#8b949e', fontWeight: 600,
                      borderBottom: '2px solid #21262d', whiteSpace: 'nowrap', fontSize: 12,
                    }}>
                      <span dangerouslySetInnerHTML={{ __html: renderInline(h) }} />
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dataRows.map((row, ri) => {
                  const itemId = savedSectionId ? `${savedSectionId}:${ri}` : null;
                  const isChecked = itemId && checkedMap[itemId];
                  const note = itemId && notesMap[itemId];
                  const hasReport = note && note.includes('Report');

                  return (
                    <tr key={ri} style={{
                      borderBottom: '1px solid #161b22',
                      background: isChecked ? 'rgba(63, 185, 80, 0.04)' : 'transparent',
                    }}
                      onMouseEnter={e => { if (!isChecked) e.currentTarget.style.background = '#1c2128'; }}
                      onMouseLeave={e => e.currentTarget.style.background = isChecked ? 'rgba(63, 185, 80, 0.04)' : 'transparent'}>
                      {row.map((cell, ci) => {
                        if (ci === doneColIdx) {
                          return (
                            <td key={ci} style={{ padding: '6px 10px', textAlign: 'center', fontSize: 14 }}>
                              {isChecked
                                ? <span title="Scored — results available">{'\u2705'}</span>
                                : <span style={{ color: '#484f58' }}>{cell.includes('[x]') || cell.includes('[X]') ? '\u2705' : '\u2B1C'}</span>}
                            </td>
                          );
                        }
                        // Add report link badge after the condition/trait name (first text column after Done)
                        const isNameCol = ci === (doneColIdx >= 0 ? doneColIdx + 1 : 1);
                        return (
                          <td key={ci} style={{
                            padding: '6px 10px', color: isChecked ? '#8b949e' : '#c9d1d9',
                            maxWidth: 350, wordBreak: 'break-word',
                          }}>
                            <span dangerouslySetInnerHTML={{ __html: renderInline(cell) }} />
                            {isNameCol && hasReport && (
                              <span onClick={() => {
                                // Extract report filename from note
                                const match = note.match(/\/report\/([^)]+)/);
                                if (match && onViewReport) onViewReport(match[1]);
                              }} style={{
                                marginLeft: 6, fontSize: 10, padding: '1px 6px', borderRadius: 8,
                                background: 'rgba(63,185,80,0.12)', color: '#3fb950',
                                border: '1px solid rgba(63,185,80,0.25)', cursor: 'pointer',
                                whiteSpace: 'nowrap',
                              }}>
                                View Report
                              </span>
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        );
      }
      continue;
    }

    // Empty line
    if (line.trim() === '') {
      i++; continue;
    }

    // Regular paragraph
    elements.push(
      <p key={i} style={{ color: '#c9d1d9', fontSize: 14, lineHeight: 1.6, margin: '6px 0' }}>
        <span dangerouslySetInnerHTML={{ __html: renderInline(line) }} />
      </p>
    );
    i++;
  }

  return <div>{elements}</div>;
}

/* ── Main Component ────────────────────────────────────────── */

function ReportViewer({ filename, onClose }) {
  const [content, setContent] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${BASE}/report/${filename}`)
      .then(r => r.json())
      .then(data => { setContent(data.content || 'Report not found'); setLoading(false); })
      .catch(() => { setContent('Failed to load report'); setLoading(false); });
  }, [filename]);

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)', zIndex: 1000, display: 'flex', justifyContent: 'center', alignItems: 'center' }}
      onClick={onClose}>
      <div style={{ background: '#161b22', border: '1px solid #30363d', borderRadius: 12, padding: 24, maxWidth: 700, width: '90%', maxHeight: '80vh', overflow: 'auto' }}
        onClick={e => e.stopPropagation()}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <h3 style={{ color: '#e6edf3', margin: 0 }}>{filename.replace('.md', '')}</h3>
          <span onClick={onClose} style={{ cursor: 'pointer', color: '#8b949e', fontSize: 20 }}>&times;</span>
        </div>
        {loading ? (
          <div style={{ color: '#8b949e' }}>Loading...</div>
        ) : (
          <RenderedMarkdown markdown={content} />
        )}
      </div>
    </div>
  );
}

export default function ChecklistPanel() {
  const [markdown, setMarkdown] = useState('');
  const [editMd, setEditMd] = useState('');
  const [checkedMap, setCheckedMap] = useState({});
  const [notesMap, setNotesMap] = useState({});
  const [scoresMap, setScoresMap] = useState({});
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [viewingReport, setViewingReport] = useState(null);
  const [samples, setSamples] = useState([]);
  const [selectedSample, setSelectedSample] = useState('');
  const [samplesWithData, setSamplesWithData] = useState([]);
  const [runningPgs, setRunningPgs] = useState({});
  const [runningCmds, setRunningCmds] = useState({});
  const [cmdResults, setCmdResults] = useState({});
  const [estimatesMap, setEstimatesMap] = useState({});
  const [refreshingEstimates, setRefreshingEstimates] = useState(false);
  const [skillTask, setSkillTask] = useState(null); // {taskId, checkName, log, status}

  const [ancestrySummary, setAncestrySummary] = useState({});

  useEffect(() => {
    const token = localStorage.getItem('auth_token');
    fetch('/genomics/api/ancestry/confidence-summary', {
      headers: { Authorization: 'Bearer ' + token },
    })
      .then(r => r.ok ? r.json() : {})
      .then(setAncestrySummary)
      .catch(() => {});
  }, []);

  const load = useCallback(async () => {
    try {
      const url = selectedSample ? `${BASE}?sample=${encodeURIComponent(selectedSample)}` : BASE;
      const res = await fetch(url);
      const data = await res.json();
      if (data.markdown) {
        setMarkdown(data.markdown);
        setEditMd(data.markdown);
      }
      setCheckedMap(data.checked || {});
      setNotesMap(data.notes || {});
      setScoresMap(data.scores || {});
      setCmdResults(data.command_results || {});
      if (data.estimates) setEstimatesMap(data.estimates);
      if (data.available_samples) setSamplesWithData(data.available_samples);
    } catch {}
    setLoading(false);
  }, [selectedSample]);

  useEffect(() => {
    load();
    // Load available samples
    fetch(`${BASE}/samples`).then(r => r.json()).then(setSamples).catch(() => {});
  }, [load]);

  const refreshEstimates = useCallback(async () => {
    setRefreshingEstimates(true);
    try {
      const res = await fetch(`${BASE}/estimates/refresh`, { method: 'POST' });
      const data = await res.json();
      if (data && data.estimates) setEstimatesMap(data.estimates);
    } catch {}
    setRefreshingEstimates(false);
  }, []);

  // Poll skill task log when active
  const skillTaskId = skillTask ? skillTask.taskId : null;
  const skillTaskStatus = skillTask ? skillTask.status : null;
  useEffect(() => {
    if (!skillTaskId || skillTaskStatus === 'complete' || skillTaskStatus === 'failed') return;
    const interval = setInterval(async () => {
      try {
        const res = await fetch(`${BASE}/skill-status/${skillTaskId}`);
        const data = await res.json();
        setSkillTask(prev => prev ? { ...prev, log: data.log || '', status: data.status, exit_code: data.exit_code } : null);
        if (data.status === 'complete' || data.status === 'failed') {
          clearInterval(interval);
          load();
        }
      } catch {}
    }, 2000);
    return () => clearInterval(interval);
  }, [skillTaskId, skillTaskStatus, load]);

  const handleRun = async (pgsIds, sample, refPop) => {
    if (!pgsIds.length || !sample) return;

    // Mark all as running
    setRunningPgs(prev => {
      const next = { ...prev };
      for (const id of pgsIds) next[id] = true;
      return next;
    });

    try {
      const res = await fetch(`${BASE}/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pgs_ids: pgsIds,
          source_file: { path: sample.path, type: sample.type },
          ref_population: refPop || 'EUR',
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        console.error('Run failed:', data);
        setRunningPgs(prev => {
          const next = { ...prev };
          for (const id of pgsIds) delete next[id];
          return next;
        });
        return;
      }

      // Poll the run status until complete
      const runId = data.run_id;
      if (!runId) return;

      const poll = setInterval(async () => {
        try {
          const statusRes = await fetch(`/genomics/api/runs/${runId}`);
          if (!statusRes.ok) return;
          const run = await statusRes.json();

          if (run.status === 'complete' || run.status === 'completed' || run.status === 'failed') {
            clearInterval(poll);
            // Sync checklist and reload all state
            await fetch(`${BASE}/sync`, { method: 'POST' });
            await load();
            setRunningPgs(prev => {
              const next = { ...prev };
              for (const id of pgsIds) delete next[id];
              return next;
            });
          }
        } catch {}
      }, 3000);

      // Safety timeout: 10 minutes
      setTimeout(() => {
        clearInterval(poll);
        load();
        setRunningPgs(prev => {
          const next = { ...prev };
          for (const id of pgsIds) delete next[id];
          return next;
        });
      }, 600000);
    } catch (err) {
      console.error('Run error:', err);
      setRunningPgs(prev => {
        const next = { ...prev };
        for (const id of pgsIds) delete next[id];
        return next;
      });
    }
  };

  // Debounced reload — only reload once after all concurrent commands finish
  const reloadTimerRef = useRef(null);
  const scheduleReload = useCallback(() => {
    if (reloadTimerRef.current) clearTimeout(reloadTimerRef.current);
    reloadTimerRef.current = setTimeout(() => { load(); }, 1000);
  }, [load]);

  const handleRunCommand = async (itemId, cmd, sample, checkName) => {
    setRunningCmds(prev => ({ ...prev, [itemId]: true }));
    try {
      const res = await fetch(`${BASE}/run-command`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          item_id: itemId,
          command: cmd,
          sample_name: sample.name,
          sample_path: sample.path,
          check_name: checkName,
        }),
      });
      const data = await res.json();
      if (data.ok) {
        // Check if this is a skill task (async, runs in background)
        if (data.skill_task) {
          setSkillTask({ taskId: data.skill_task, checkName, status: 'running', log: 'Starting Claude Code...\n', skillFile: data.skill_file });
          return; // Don't update results yet — polling will handle it
        }
        if (data.exit_code === 0) {
          setCheckedMap(prev => ({ ...prev, [itemId]: true }));
        }
        setCmdResults(prev => ({
          ...prev,
          [itemId]: { sample: sample.name, check: checkName, output: data.output || '', exit_code: data.exit_code }
        }));
        if (data.report) {
          setNotesMap(prev => ({
            ...prev,
            [itemId]: `[Report](/genomics/api/checklist/report/${data.report}) | ${sample.name}: ${(data.output || '').split('\n')[0]?.slice(0, 50)}`,
          }));
        }
      }
      scheduleReload();
    } catch (err) {
      console.error('Command error:', err);
    } finally {
      setRunningCmds(prev => { const next = { ...prev }; delete next[itemId]; return next; });
    }
  };

  const handleRunSection = async (sectionId, sectionTitle, items, sample) => {
    try {
      const res = await fetch(`${BASE}/run-section`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          section_id: sectionId,
          section_title: sectionTitle,
          items,
          sample_name: sample.name,
          sample_path: sample.path,
          sample_type: sample.type,
        }),
      });
      const data = await res.json();
      if (data.ok) {
        // Update local state with all results at once
        const newChecked = {};
        const newCmd = {};
        const newNotes = {};
        for (const r of (data.results || [])) {
          // Check if any result is a skill task
          if (r.skill_task) {
            setSkillTask({ taskId: r.skill_task, checkName: r.check, status: 'running', log: 'Starting Claude Code...\n' });
          }
          if (r.item_id && r.exit_code === 0) newChecked[r.item_id] = true;
          if (r.item_id) newCmd[r.item_id] = { sample: sample.name, check: r.check, output: r.output || '', exit_code: r.exit_code };
        }
        setCheckedMap(prev => ({ ...prev, ...newChecked }));
        setCmdResults(prev => ({ ...prev, ...newCmd }));
        if (data.report) {
          newNotes[`${sectionId}:_section_report`] = `[Report](/genomics/api/checklist/report/${data.report})`;
          setNotesMap(prev => ({ ...prev, ...newNotes }));
        }
      }
      await load();
    } catch (err) {
      console.error('Section run error:', err);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      await fetch(BASE, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ markdown: editMd }),
      });
      setMarkdown(editMd);
      setEditing(false);
    } catch {}
    setSaving(false);
  };

  if (loading) {
    return <div style={{ padding: 40, textAlign: 'center', color: '#8b949e' }}>Loading checklist...</div>;
  }

  /* ── Edit mode ── */
  if (editing) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 60px)' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 16px', borderBottom: '1px solid #21262d', flexShrink: 0 }}>
          <h2 style={{ color: '#e6edf3', fontSize: 16, margin: 0 }}>Edit Checklist</h2>
          <div style={{ display: 'flex', gap: 8 }}>
            <button onClick={() => { setEditMd(markdown); setEditing(false); }}
              style={{ padding: '6px 14px', borderRadius: 6, border: '1px solid #30363d', background: '#21262d', color: '#c9d1d9', cursor: 'pointer', fontSize: 13 }}>
              Cancel
            </button>
            <button onClick={handleSave} disabled={saving}
              style={{ padding: '6px 14px', borderRadius: 6, border: '1px solid #2ea043', background: '#238636', color: '#fff', cursor: 'pointer', fontSize: 13 }}>
              {saving ? 'Saving...' : 'Save'}
            </button>
          </div>
        </div>
        <textarea
          value={editMd}
          onChange={e => setEditMd(e.target.value)}
          style={{
            flex: 1, background: '#0d1117', color: '#c9d1d9', border: 'none',
            padding: 16, fontFamily: 'monospace', fontSize: 13, resize: 'none',
            outline: 'none', lineHeight: 1.5,
          }}
          spellCheck={false}
        />
      </div>
    );
  }

  /* ── Read mode ── */
  const checkedCount = Object.keys(checkedMap).length;

  return (
    <div>
      {/* Global sample selector */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 12, padding: '12px 16px', marginBottom: 12,
        background: '#161b22', border: '1px solid #30363d', borderRadius: 8,
      }}>
        <span style={{ fontSize: 14, fontWeight: 600, color: '#e6edf3', whiteSpace: 'nowrap' }}>Sample:</span>
        <select value={selectedSample} onChange={e => setSelectedSample(e.target.value)}
          style={{
            flex: 1, maxWidth: 400, padding: '8px 12px', fontSize: 14, fontWeight: 600,
            background: '#0d1117', border: '1px solid #58a6ff', color: '#e6edf3',
            borderRadius: 6, cursor: 'pointer',
          }}>
          <option value="">Select a file...</option>
          {samples.map((s, i) => (
            <option key={`${s.path}-${i}`} value={s.path}>
              {s.name} [{s.type.toUpperCase()}] — {s.path.split('/').pop()}{samplesWithData.includes(s.path) ? ' *' : ''}
            </option>
          ))}
        </select>
        {selectedSample && (
          <span style={{ fontSize: 13, color: '#8b949e' }}>
            Showing results for <strong style={{ color: '#58a6ff' }}>{selectedSample.split('/').pop()}</strong> only
          </span>
        )}
        {checkedCount > 0 && (
          <span style={{ fontSize: 12, color: '#3fb950', marginLeft: 'auto' }}>
            {checkedCount} completed
          </span>
        )}
        <div style={{ display: 'flex', gap: 6, marginLeft: checkedCount ? 0 : 'auto' }}>
          <button onClick={load}
            style={{ padding: '5px 10px', borderRadius: 6, border: '1px solid #30363d', background: '#21262d', color: '#c9d1d9', cursor: 'pointer', fontSize: 11 }}>
            Refresh
          </button>
          <button onClick={refreshEstimates} disabled={refreshingEstimates}
            title="Recompute estimated run times based on current server resources and PGS variant counts"
            style={{ padding: '5px 10px', borderRadius: 6, border: '1px solid #30363d', background: '#21262d', color: refreshingEstimates ? '#6e7681' : '#c9d1d9', cursor: refreshingEstimates ? 'default' : 'pointer', fontSize: 11 }}>
            {refreshingEstimates ? 'Estimating...' : 'Refresh estimates'}
          </button>
          <button onClick={() => setEditing(true)}
            style={{ padding: '5px 10px', borderRadius: 6, border: '1px solid #30363d', background: '#21262d', color: '#c9d1d9', cursor: 'pointer', fontSize: 11 }}>
            Edit
          </button>
        </div>
      </div>

      <div style={{ display: 'none' /* old header removed */ }}>
      </div>
      <RenderedMarkdown markdown={markdown} checkedMap={checkedMap} notesMap={notesMap}
        scoresMap={scoresMap} cmdResults={cmdResults} estimatesMap={estimatesMap}
        samples={samples} globalSample={selectedSample}
        onViewReport={(filename) => setViewingReport(filename)}
        onRun={handleRun} onRunCommand={handleRunCommand} onRunSection={handleRunSection}
        runningPgs={runningPgs} runningCmds={runningCmds} />
      {viewingReport && (
        <ReportViewer filename={viewingReport} onClose={() => setViewingReport(null)} />
      )}
      {/* Skill execution log modal */}
      {skillTask && (
        <div style={{
          position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
          background: 'rgba(0,0,0,0.7)', zIndex: 1000, display: 'flex', alignItems: 'center', justifyContent: 'center',
        }} onClick={() => { if (skillTask.status !== 'running') setSkillTask(null); }}>
          <div style={{
            background: '#0d1117', border: '1px solid #30363d', borderRadius: 12,
            width: '80%', maxWidth: 900, maxHeight: '80vh', display: 'flex', flexDirection: 'column',
          }} onClick={e => e.stopPropagation()}>
            <div style={{
              padding: '14px 20px', borderBottom: '1px solid #21262d',
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            }}>
              <div>
                <span style={{ fontSize: 16, fontWeight: 700, color: '#e6edf3' }}>
                  Claude Code — {skillTask.checkName}
                </span>
                {skillTask.skillFile && (
                  <span style={{ fontSize: 11, color: '#6e7681', marginLeft: 10 }}>
                    skill: {skillTask.skillFile}
                  </span>
                )}
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                <span style={{
                  fontSize: 11, padding: '2px 8px', borderRadius: 10, fontWeight: 700,
                  background: skillTask.status === 'running' ? 'rgba(210,153,34,0.15)' : skillTask.status === 'complete' ? 'rgba(63,185,80,0.15)' : 'rgba(248,81,73,0.15)',
                  color: skillTask.status === 'running' ? '#d29922' : skillTask.status === 'complete' ? '#3fb950' : '#f85149',
                }}>
                  {skillTask.status === 'running' ? 'RUNNING...' : skillTask.status === 'complete' ? 'COMPLETE' : 'FAILED'}
                </span>
                {skillTask.status !== 'running' && (
                  <button onClick={() => { setSkillTask(null); load(); }} style={{
                    padding: '4px 12px', fontSize: 12, borderRadius: 6, cursor: 'pointer',
                    background: '#21262d', border: '1px solid #30363d', color: '#e6edf3',
                  }}>Close</button>
                )}
              </div>
            </div>
            <pre style={{
              flex: 1, overflow: 'auto', padding: '12px 20px', margin: 0,
              fontSize: 12, lineHeight: 1.6, color: '#8b949e', fontFamily: 'monospace',
              whiteSpace: 'pre-wrap', wordBreak: 'break-word',
            }}>
              {skillTask.log || 'Waiting for output...'}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}
// rebuilt 1774833695
