import { useState, useEffect, useRef, useCallback } from 'react';
import { chatApi } from '../api.js';

function processInline(text) {
  return text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/`([^`]+)`/g, '<code class="chat-inline-code">$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/\n/g, '<br/>');
}

function renderMarkdown(text) {
  if (!text) return null;
  const parts = text.split(/(```[\s\S]*?```)/g);
  return parts.map((part, i) => {
    if (part.startsWith('```')) {
      const code = part.replace(/^```\w*\n?/, '').replace(/\n?```$/, '');
      return <pre key={i} className="chat-code-block"><code>{code}</code></pre>;
    }
    return <span key={i} dangerouslySetInnerHTML={{ __html: processInline(part) }} />;
  });
}

function formatTime(ts) {
  if (!ts) return '';
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function WelcomeMessage() {
  return (
    <div className="chat-welcome">
      <h3>Welcome to the 23andClaude AI Assistant</h3>
      <p>I can help you with:</p>
      <ul>
        <li>Running PGS scoring on your BAM/VCF files</li>
        <li>Converting FASTQ &rarr; BAM &rarr; VCF</li>
        <li>Inspecting and validating genomic files</li>
        <li>Searching the PGS Catalog</li>
        <li>Analyzing scoring results</li>
      </ul>
      <p style={{ marginTop: 16, fontSize: '0.85rem' }}>Type a message to get started.</p>
    </div>
  );
}

function TypingIndicator() {
  return (
    <div className="chat-typing">
      <div className="typing-dots">
        <span />
        <span />
        <span />
      </div>
      <span>Thinking...</span>
    </div>
  );
}

// ---- Skills Editor sub-component ----
function SkillsEditor() {
  const [files, setFiles] = useState([]);
  const [selectedFile, setSelectedFile] = useState(null);
  const [content, setContent] = useState('');
  const [originalContent, setOriginalContent] = useState('');
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveMsg, setSaveMsg] = useState('');
  const [creating, setCreating] = useState(false);
  const editorRef = useRef(null);

  const loadFiles = useCallback(async () => {
    try {
      const data = await chatApi.skills();
      setFiles(data.files || []);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadFiles(); }, [loadFiles]);

  const selectFile = async (file) => {
    setLoading(true);
    setSaveMsg('');
    setCreating(false);
    try {
      const data = await chatApi.readSkill(file.dir ? `${file.dir}/${file.name}` : file.name);
      setSelectedFile(file);
      setContent(data.content || '');
      setOriginalContent(data.content || '');
    } catch (err) {
      setSaveMsg(`Error loading: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    if (!selectedFile) return;
    setSaving(true);
    setSaveMsg('');
    try {
      const name = selectedFile.dir ? `${selectedFile.dir}/${selectedFile.name}` : selectedFile.name;
      await chatApi.saveSkill(name, content);
      setOriginalContent(content);
      setSaveMsg('Saved');
      setTimeout(() => setSaveMsg(''), 3000);
      loadFiles();
    } catch (err) {
      setSaveMsg(`Error: ${err.message}`);
    } finally {
      setSaving(false);
    }
  };

  const handleCreate = async () => {
    setSaving(true);
    setSaveMsg('');
    try {
      const data = await chatApi.createSkill(content);
      setSaveMsg('Created');
      setCreating(false);
      await loadFiles();
      // Select the new file
      setSelectedFile({ name: data.name, path: data.path, dir: 'skills' });
      setOriginalContent(content);
      setTimeout(() => setSaveMsg(''), 3000);
    } catch (err) {
      setSaveMsg(`Error: ${err.message}`);
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!selectedFile || !selectedFile.dir) return;
    if (!confirm(`Delete ${selectedFile.name}?`)) return;
    try {
      await chatApi.deleteSkill(selectedFile.name);
      setSelectedFile(null);
      setContent('');
      setOriginalContent('');
      loadFiles();
    } catch (err) {
      setSaveMsg(`Error: ${err.message}`);
    }
  };

  const startCreate = () => {
    setCreating(true);
    setSelectedFile(null);
    setContent('# New Skill\n\nDescribe instructions for the AI assistant here.\n');
    setOriginalContent('');
    setSaveMsg('');
  };

  const isDirty = content !== originalContent;

  const formatSize = (bytes) => {
    if (bytes < 1024) return `${bytes} B`;
    return `${(bytes / 1024).toFixed(1)} KB`;
  };

  const formatDate = (ts) => {
    if (!ts) return '';
    return new Date(ts * 1000).toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  };

  return (
    <div className="skills-editor">
      {/* File list sidebar */}
      <div className="skills-sidebar">
        <div className="skills-sidebar-header">
          <span>Instruction Files</span>
          <button className="skills-new-btn" onClick={startCreate} title="New skill file">+</button>
        </div>
        <div className="skills-file-list">
          {files.map((f) => (
            <div
              key={f.path}
              className={`skills-file-item ${selectedFile?.path === f.path ? 'active' : ''}`}
              onClick={() => selectFile(f)}
            >
              <div className="skills-file-name">
                {f.dir ? <span className="skills-file-dir">{f.dir}/</span> : null}
                {f.name}
              </div>
              <div className="skills-file-meta">
                {formatSize(f.size)} &middot; {formatDate(f.modified)}
              </div>
            </div>
          ))}
          {files.length === 0 && (
            <div className="skills-empty">No .md files found</div>
          )}
        </div>
      </div>

      {/* Editor area */}
      <div className="skills-content">
        {(selectedFile || creating) ? (
          <>
            <div className="skills-editor-header">
              <div className="skills-editor-title">
                {creating ? 'New Skill File' : selectedFile.name}
                {isDirty && <span className="skills-dirty-dot" title="Unsaved changes" />}
              </div>
              <div className="skills-editor-actions">
                {saveMsg && <span className={`skills-save-msg ${saveMsg === 'Saved' || saveMsg === 'Created' ? 'ok' : 'err'}`}>{saveMsg}</span>}
                {selectedFile?.dir && !creating && (
                  <button className="skills-delete-btn" onClick={handleDelete}>Delete</button>
                )}
                <button
                  className="skills-save-btn"
                  onClick={creating ? handleCreate : handleSave}
                  disabled={saving || (!isDirty && !creating)}
                >
                  {saving ? 'Saving...' : creating ? 'Create' : 'Save'}
                </button>
              </div>
            </div>
            <textarea
              ref={editorRef}
              className="skills-textarea"
              value={content}
              onChange={(e) => setContent(e.target.value)}
              spellCheck={false}
              placeholder="Write your instructions here..."
            />
          </>
        ) : (
          <div className="skills-placeholder">
            <p>Select a file to edit, or click <strong>+</strong> to create a new skill.</p>
            <p style={{ marginTop: 8, fontSize: '0.8rem', color: '#484f58' }}>
              These .md files are automatically loaded by the Claude Code session as instructions.
              Root-level files (like GENOMICS_CLAUDE.md) are protected from deletion.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

// ---- Setup Screen ----
function SetupScreen({ onComplete }) {
  const [setupStatus, setSetupStatus] = useState(null);
  const [running, setRunning] = useState(false);
  const [output, setOutput] = useState('');
  const [done, setDone] = useState(false);
  const [exitCode, setExitCode] = useState(null);
  const outputRef = useRef(null);

  useEffect(() => {
    fetch('/genomics/api/system/setup-status')
      .then(r => r.json())
      .then(setSetupStatus)
      .catch(() => {});
  }, []);

  const runSetup = () => {
    setRunning(true);
    setOutput('Starting setup...\n');
    const es = new EventSource('/genomics/api/system/setup-run');
    es.onmessage = (e) => {
      const line = e.data;
      if (line.startsWith('[SETUP_EXIT_CODE:')) {
        const code = parseInt(line.match(/\d+/)?.[0] || '1');
        setExitCode(code);
        setDone(true);
        setRunning(false);
        es.close();
        if (code === 0) setTimeout(() => onComplete(), 2000);
        return;
      }
      setOutput(prev => prev + line);
      if (outputRef.current) {
        outputRef.current.scrollTop = outputRef.current.scrollHeight;
      }
    };
    es.onerror = () => {
      es.close();
      setRunning(false);
      setDone(true);
    };
  };

  const checkItem = (key, label) => {
    if (!setupStatus) return null;
    const item = setupStatus[key];
    if (!item) return null;
    const ok = item.installed;
    if (item.relevant === false && !ok) return null; // Skip irrelevant items
    return (
      <div key={key} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '4px 0', fontSize: 13 }}>
        <span style={{ color: ok ? '#3fb950' : '#f85149', fontSize: 16 }}>{ok ? '\u2713' : '\u2717'}</span>
        <span style={{ color: ok ? '#8b949e' : '#e6edf3' }}>{label}</span>
        {!ok && <span style={{ color: '#484f58', fontSize: 11 }}>missing</span>}
      </div>
    );
  };

  if (running || output) {
    return (
      <div className="chat-panel">
        <div className="chat-header">
          <div className="chat-header-left">
            <h2>{done ? (exitCode === 0 ? 'Setup Complete' : 'Setup Failed') : 'Setting Up...'}</h2>
            <span className={`chat-status-badge ${running ? 'busy' : exitCode === 0 ? 'idle' : 'stopped'}`}>
              <span className="chat-status-dot" />
              {running ? 'Installing dependencies...' : exitCode === 0 ? 'Ready to launch' : 'Check errors above'}
            </span>
          </div>
        </div>
        <div className="chat-raw-output" ref={outputRef} style={{ flex: 1 }}>
          <pre className="chat-raw-pre" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{output}</pre>
        </div>
        {done && exitCode === 0 && (
          <div style={{ padding: 16, textAlign: 'center' }}>
            <button className="chat-send-btn" onClick={onComplete}>Launch AI Assistant</button>
          </div>
        )}
        {done && exitCode !== 0 && (
          <div style={{ padding: 16, textAlign: 'center' }}>
            <button className="chat-send-btn" onClick={runSetup}>Retry Setup</button>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="chat-panel">
      <div className="chat-header">
        <div className="chat-header-left">
          <h2>First-Time Setup</h2>
          <span className="chat-status-badge stopped">
            <span className="chat-status-dot" />
            Setup required
          </span>
        </div>
      </div>
      <div style={{ flex: 1, overflow: 'auto', padding: 20 }}>
        <div style={{ maxWidth: 600, margin: '0 auto' }}>
          <p style={{ color: '#c9d1d9', fontSize: 15, lineHeight: 1.6, marginBottom: 20 }}>
            The Genomics Dashboard needs to download reference data and set up dependencies before first use.
            This will download ~5 GB of data and may take 15-60 minutes depending on your connection.
          </p>

          {setupStatus && (
            <div style={{ background: '#161b22', border: '1px solid #30363d', borderRadius: 8, padding: 16, marginBottom: 20 }}>
              <div style={{ fontWeight: 600, color: '#e6edf3', marginBottom: 8 }}>Component Status</div>
              {checkItem('bcftools', 'bcftools (variant manipulation)')}
              {checkItem('samtools', 'samtools (alignment tools)')}
              {checkItem('plink2', 'plink2 (genotype analysis)')}
              {checkItem('bwa', 'bwa (sequence aligner)')}
              {checkItem('minimap2', 'minimap2 (long-read aligner)')}
              {checkItem('reference_genome', 'GRCh38 reference genome (~3.1 GB)')}
              {checkItem('ref_panel', '1000 Genomes reference panel (~700 MB)')}
              {checkItem('dv_cpu_container', 'DeepVariant CPU container (~2.8 GB)')}
              {checkItem('dv_gpu_container', 'DeepVariant GPU container (~11 GB)')}
              {checkItem('redis', 'Redis cache')}
              {checkItem('frontend', 'Frontend build')}
              {checkItem('apptainer', 'Apptainer (container runtime)')}
            </div>
          )}

          {setupStatus && setupStatus.setup_complete ? (
            <div style={{ textAlign: 'center' }}>
              <p style={{ color: '#3fb950', marginBottom: 12 }}>All critical components are installed.</p>
              <button className="chat-send-btn" onClick={onComplete}>Launch AI Assistant</button>
            </div>
          ) : (
            <div style={{ textAlign: 'center' }}>
              <button className="chat-send-btn" onClick={runSetup} style={{ fontSize: 15, padding: '10px 24px' }}>
                Run Auto Setup
              </button>
              <p style={{ color: '#484f58', fontSize: 12, marginTop: 8 }}>
                Downloads reference genome, 1000G panel, and DeepVariant containers.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ---- Main ChatPanel ----
export default function ChatPanel() {
  const [setupChecked, setSetupChecked] = useState(false);
  const [needsSetup, setNeedsSetup] = useState(false);

  useEffect(() => {
    fetch('/genomics/api/system/setup-status')
      .then(r => r.json())
      .then(data => {
        setNeedsSetup(!data.setup_complete);
        setSetupChecked(true);
      })
      .catch(() => setSetupChecked(true)); // If API fails, skip setup check
  }, []);

  if (!setupChecked) return null; // Loading
  if (needsSetup) return <SetupScreen onComplete={() => setNeedsSetup(false)} />;

  return <ChatPanelMain />;
}

function ChatPanelMain() {
  const [activeTab, setActiveTab] = useState('terminal');
  const [messages, setMessages] = useState([]);
  const [status, setStatus] = useState('idle');
  const [detail, setDetail] = useState('');
  const [sessionExists, setSessionExists] = useState(false);
  const [inputText, setInputText] = useState('');
  const [sending, setSending] = useState(false);

  // Terminal state
  const [rawContent, setRawContent] = useState('');
  const [rawLines, setRawLines] = useState(0);
  const [rawLoading, setRawLoading] = useState(false);
  const [rawCmdText, setRawCmdText] = useState('');

  const messagesRef = useRef(null);
  const wasAtBottom = useRef(true);
  const pollInterval = useRef(null);
  const textareaRef = useRef(null);
  const rawRef = useRef(null);
  const rawPollRef = useRef(null);
  const rawWasAtBottom = useRef(true);
  const rawCmdRef = useRef(null);

  const checkAtBottom = useCallback(() => {
    const el = messagesRef.current;
    if (!el) return;
    wasAtBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 50;
  }, []);

  const scrollToBottom = useCallback(() => {
    const el = messagesRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, []);

  const checkRawAtBottom = useCallback(() => {
    const el = rawRef.current;
    if (!el) return;
    rawWasAtBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 50;
  }, []);

  const scrollRawToBottom = useCallback(() => {
    const el = rawRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, []);

  const mergeMessages = useCallback((incoming) => {
    if (!incoming || !Array.isArray(incoming)) return;
    setMessages(prev => {
      const existingTs = new Set(prev.map(m => `${m.role}-${m.ts}`));
      const newMsgs = incoming.filter(m => !existingTs.has(`${m.role}-${m.ts}`));
      if (newMsgs.length === 0) return prev;
      return [...prev, ...newMsgs].sort((a, b) => a.ts - b.ts);
    });
  }, []);

  const pollStatus = useCallback(async () => {
    try {
      const data = await chatApi.status();
      setStatus(data.status || 'idle');
      setDetail(data.detail || '');
      setSessionExists(data.session_exists ?? false);
      if (data.messages) mergeMessages(data.messages);
    } catch { /* ignore */ }
  }, [mergeMessages]);

  useEffect(() => {
    pollStatus();
    chatApi.history().then(data => {
      if (data?.messages) {
        setMessages(data.messages.sort((a, b) => a.ts - b.ts));
        setTimeout(scrollToBottom, 100);
      }
    }).catch(() => {});
    return () => { if (pollInterval.current) clearInterval(pollInterval.current); };
  }, []);

  useEffect(() => {
    if (pollInterval.current) clearInterval(pollInterval.current);
    const rate = status === 'busy' ? 2000 : 5000;
    pollInterval.current = setInterval(pollStatus, rate);
    return () => clearInterval(pollInterval.current);
  }, [status, pollStatus]);

  useEffect(() => {
    if (wasAtBottom.current) scrollToBottom();
  }, [messages, scrollToBottom]);

  // Terminal raw polling
  const loadRawFull = useCallback(async () => {
    setRawLoading(true);
    try {
      const data = await chatApi.raw();
      setRawContent(data.raw || '');
      setRawLines(data.lines || 0);
      setTimeout(scrollRawToBottom, 50);
    } catch { /* ignore */ }
    finally { setRawLoading(false); }
  }, [scrollRawToBottom]);

  const pollRawTail = useCallback(async () => {
    try {
      const data = await chatApi.rawTail(rawLines);
      if (!data) return;
      if (data.mode === 'full') {
        setRawContent(data.raw || '');
        setRawLines(data.total_lines || 0);
        if (rawWasAtBottom.current) setTimeout(scrollRawToBottom, 30);
      } else if (data.mode === 'delta' && data.raw) {
        setRawContent(prev => prev + data.raw);
        setRawLines(data.total_lines || 0);
        if (rawWasAtBottom.current) setTimeout(scrollRawToBottom, 30);
      }
    } catch { /* ignore */ }
  }, [rawLines, scrollRawToBottom]);

  useEffect(() => {
    if (activeTab === 'terminal') {
      loadRawFull();
      rawPollRef.current = setInterval(pollRawTail, 1500);
    } else {
      if (rawPollRef.current) { clearInterval(rawPollRef.current); rawPollRef.current = null; }
    }
    return () => { if (rawPollRef.current) clearInterval(rawPollRef.current); };
  }, [activeTab, loadRawFull, pollRawTail]);

  const handleInputChange = (e) => {
    setInputText(e.target.value);
    const ta = textareaRef.current;
    if (ta) { ta.style.height = 'auto'; ta.style.height = Math.min(ta.scrollHeight, 150) + 'px'; }
  };

  const handleSend = async () => {
    const text = inputText.trim();
    if (!text || sending) return;
    setSending(true);
    setInputText('');
    if (textareaRef.current) textareaRef.current.style.height = 'auto';
    const userMsg = { role: 'user', text, ts: Date.now() / 1000 };
    setMessages(prev => [...prev, userMsg]);
    wasAtBottom.current = true;
    try {
      const data = await chatApi.send(text);
      if (data?.messages) mergeMessages(data.messages);
      setStatus('busy');
    } catch (err) {
      setMessages(prev => [...prev, { role: 'assistant', text: `Error: ${err.message}`, ts: Date.now() / 1000 }]);
    } finally { setSending(false); }
  };

  const handleRawSend = async () => {
    const cmd = rawCmdText.trim();
    if (!cmd) return;
    setRawCmdText('');
    try { await chatApi.send(cmd); setTimeout(pollRawTail, 500); } catch { /* ignore */ }
  };

  const handleKeyDown = (e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend(); } };
  const handleRawKeyDown = (e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleRawSend(); } };
  const handleInterrupt = async () => { try { await chatApi.interrupt(); } catch { /* ignore */ } };

  const handleRestart = async () => {
    try { await chatApi.restart(); setMessages([]); setStatus('idle'); setDetail(''); setRawContent(''); setRawLines(0); } catch { /* ignore */ }
  };

  const handleClear = async () => {
    try { await chatApi.clear(); setMessages([]); } catch { /* ignore */ }
  };

  const renderBubble = (msg, i) => (
    <div key={`${msg.role}-${msg.ts}-${i}`} className={`chat-bubble ${msg.role}`}>
      <div className="chat-bubble-content">
        {msg.role === 'assistant' ? renderMarkdown(msg.text) : msg.text}
      </div>
      <div className="chat-bubble-time">{formatTime(msg.ts)}</div>
    </div>
  );

  return (
    <div className="chat-panel">
      {/* Header */}
      <div className="chat-header">
        <div className="chat-header-left">
          <h2>AI Assistant</h2>
          <span className={`chat-status-badge ${status}`}>
            <span className="chat-status-dot" />
            {status === 'busy' ? detail || 'Working...' : status === 'idle' ? 'Ready' : 'Session stopped'}
          </span>
        </div>
        <div className="chat-header-actions">
          <button onClick={handleRestart}>Restart</button>
          <button onClick={handleClear}>Clear</button>
        </div>
      </div>

      {/* Sub-tabs */}
      <div className="chat-tab-bar">
        <button className={`chat-tab ${activeTab === 'terminal' ? 'active' : ''}`} onClick={() => setActiveTab('terminal')}>Terminal</button>
        <button className={`chat-tab ${activeTab === 'chat' ? 'active' : ''}`} onClick={() => setActiveTab('chat')}>Chat</button>
        <button className={`chat-tab ${activeTab === 'skills' ? 'active' : ''}`} onClick={() => setActiveTab('skills')}>Skills</button>
        {activeTab === 'terminal' && (
          <button className="chat-tab-reload" onClick={loadRawFull} disabled={rawLoading}>
            {rawLoading ? 'Loading...' : 'Reload'}
          </button>
        )}
        {status === 'busy' && (
          <button className="chat-tab-stop" onClick={handleInterrupt}>Stop</button>
        )}
      </div>

      {/* Chat tab */}
      {activeTab === 'chat' && (
        <>
          <div className="chat-messages" ref={messagesRef} onScroll={checkAtBottom}>
            {messages.length === 0 ? <WelcomeMessage /> : messages.map(renderBubble)}
            {status === 'busy' && <TypingIndicator />}
          </div>
          <div className="chat-input-bar">
            <textarea ref={textareaRef} value={inputText} onChange={handleInputChange} onKeyDown={handleKeyDown} placeholder="Ask the AI assistant anything about your genomic data..." disabled={sending} rows={1} />
            {status === 'busy' ? (
              <button className="chat-stop-btn" onClick={handleInterrupt}>Stop</button>
            ) : (
              <button className="chat-send-btn" onClick={handleSend} disabled={sending || !inputText.trim()}>Send</button>
            )}
          </div>
        </>
      )}

      {/* Terminal tab */}
      {activeTab === 'terminal' && (
        <>
          <div className="chat-raw-output" ref={rawRef} onScroll={checkRawAtBottom}>
            {rawContent ? (
              <pre className="chat-raw-pre">{rawContent}</pre>
            ) : (
              <div className="chat-raw-empty">
                {sessionExists
                  ? (rawLoading ? 'Loading terminal output...' : 'No output yet. Send a message to get started.')
                  : 'Session not running. Send a message or click Restart to start.'}
              </div>
            )}
          </div>
          <div className="chat-input-bar">
            <span className="chat-raw-prompt">$</span>
            <textarea ref={rawCmdRef} value={rawCmdText} onChange={(e) => setRawCmdText(e.target.value)} onKeyDown={handleRawKeyDown} placeholder="Type a command and press Enter..." rows={1} />
            <button className="chat-send-btn" onClick={handleRawSend} disabled={!rawCmdText.trim()}>Send</button>
          </div>
        </>
      )}

      {/* Skills tab */}
      {activeTab === 'skills' && <SkillsEditor />}
    </div>
  );
}
