// Use relative URLs so nginx proxy handles routing
const BASE = '/genomics/api';

async function request(path, options = {}) {
  const url = `${BASE}${path}`;
  const headers = { ...options.headers };
  if (options.body) headers['Content-Type'] = 'application/json';

  const token = localStorage.getItem('auth_token');
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(url, { ...options, headers });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`API ${res.status}: ${body}`);
  }
  const ct = res.headers.get('content-type');
  if (ct && ct.includes('application/json')) return res.json();
  return res;
}

// VCF endpoints
export const vcfApi = {
  list: () => request('/vcfs/'),
  get: (id) => request(`/vcfs/${id}`),
  register: (data) => request('/vcfs/register', { method: 'POST', body: JSON.stringify(data) }),
  duplicate: (id, target) => request(`/vcfs/${id}/duplicate`, { method: 'POST', body: JSON.stringify({ target }) }),
  delete: (id) => request(`/vcfs/${id}`, { method: 'DELETE' }),
};

export const storageApi = {
  status: () => request('/storage/status'),
};

// PGS endpoints
export const pgsApi = {
  search: (q, limit = 20) => request(`/pgs/search?q=${encodeURIComponent(q)}&limit=${limit}`),
  autocomplete: (q, limit = 8) => request(`/pgs/autocomplete?q=${encodeURIComponent(q)}&limit=${limit}`),
  get: (id) => request(`/pgs/${id}`),
  downloadStatus: (id) => request(`/pgs/${id}/download-status`),
  download: (ids, build) => request('/pgs/download', { method: 'POST', body: JSON.stringify({ pgs_ids: ids, build: build || 'GRCh38' }) }),
  cache: () => request('/pgs/cache'),
};

// File scan endpoints
export const filesApi = {
  scan: () => request('/files/scan'),
};

// Run endpoints
export const runApi = {
  create: (data) => request('/runs/', { method: 'POST', body: JSON.stringify(data) }),
  estimate: (data) => request('/runs/estimate', { method: 'POST', body: JSON.stringify(data) }),
  list: () => request('/runs/'),
  get: (id) => request(`/runs/${id}`),
  results: (id) => request(`/runs/${id}/results`),
  rawFiles: (id) => request(`/runs/${id}/results/raw`),
  rerun: (id) => request(`/runs/${id}/rerun`, { method: 'POST' }),
  delete: (id) => request(`/runs/${id}`, { method: 'DELETE' }),
};

// WebSocket for run progress
export function connectRunProgress(runId, onMessage, onClose) {
  const wsProto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsHost = window.location.host;
  const token = localStorage.getItem('auth_token') || '';
  const tokenParam = token ? `?token=${encodeURIComponent(token)}` : '';
  const ws = new WebSocket(`${wsProto}//${wsHost}/genomics/api/runs/${runId}/progress${tokenParam}`);
  ws.onmessage = (e) => { try { onMessage(JSON.parse(e.data)); } catch { onMessage({ raw: e.data }); } };
  ws.onclose = () => { if (onClose) onClose(); };
  ws.onerror = (err) => console.error('WS error:', err);
  return ws;
}

export function getRawFileUrl(runId, filename) {
  return `${BASE}/runs/${runId}/results/raw/${encodeURIComponent(filename)}`;
}

// Chat endpoints
export const chatApi = {
  send: (message) => request('/chat/send', { method: 'POST', body: JSON.stringify({ message }) }),
  status: () => request('/chat/status'),
  interrupt: () => request('/chat/interrupt', { method: 'POST' }),
  restart: () => request('/chat/restart', { method: 'POST' }),
  history: () => request('/chat/history'),
  clear: () => request('/chat/clear', { method: 'POST' }),
  raw: () => request('/chat/raw'),
  rawTail: (knownLines) => request(`/chat/raw-tail?known_lines=${knownLines}`),
  skills: () => request('/chat/skills'),
  readSkill: (name) => request(`/chat/skills/${encodeURIComponent(name)}`),
  saveSkill: (name, content) => request(`/chat/skills/${encodeURIComponent(name)}`, { method: 'PUT', body: JSON.stringify({ content }) }),
  createSkill: (content) => request('/chat/skills/new', { method: 'POST', body: JSON.stringify({ content }) }),
  deleteSkill: (name) => request(`/chat/skills/${encodeURIComponent(name)}`, { method: 'DELETE' }),
};

// System monitoring
export const systemApi = {
  stats: () => request('/system/stats'),
};


// Ancestry endpoints
export const ancestryApi = {
  all: () => request('/ancestry/all'),
  pca: () => request('/ancestry/pca'),
  status: () => request('/ancestry/status'),
  sample: (id) => request(`/ancestry/samples/${id}`),
  scores: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return request(`/ancestry/scores${qs ? '?' + qs : ''}`);
  },
  confidenceSummary: () => request('/ancestry/confidence-summary'),
  gwasAvailability: () => request('/ancestry/gwas-availability'),
  runInference: () => request('/ancestry/run-inference', { method: 'POST' }),
  importResults: (results) => request('/ancestry/import-results', { method: 'POST', body: JSON.stringify(results) }),
};

// Admin / user management endpoints (admin only)
export const adminApi = {
  listUsers: () => request('/admin/users'),
  getUser: (id) => request(`/admin/users/${id}`),
  createUser: (data) => request('/admin/users', { method: 'POST', body: JSON.stringify(data) }),
  updateUser: (id, data) => request(`/admin/users/${id}`, { method: 'PATCH', body: JSON.stringify(data) }),
  deleteUser: (id, purgeFiles = false) => request(`/admin/users/${id}${purgeFiles ? '?purge_files=true' : ''}`, { method: 'DELETE' }),
  ensureWorkspace: (id) => request(`/admin/users/${id}/workspace`, { method: 'POST' }),
};

// Reports endpoints
export const reportsApi = {
  list: (category) => request('/reports/list' + (category ? '?category=' + category : '')),
  categories: () => request('/reports/categories'),
  content: (filename) => request('/reports/content/' + encodeURIComponent(filename)),
  create: (filename, content, category) => request('/reports/create', { method: 'POST', body: JSON.stringify({ filename, content, category }) }),
  update: (filename, content) => request('/reports/content/' + encodeURIComponent(filename), { method: 'PUT', body: JSON.stringify({ content }) }),
  delete: (filename) => request('/reports/content/' + encodeURIComponent(filename), { method: 'DELETE' }),
  generate: (runId) => request('/reports/generate/' + runId, { method: 'POST' }),
  regenerateAll: () => request('/reports/regenerate-all', { method: 'POST' }),
};