import { useEffect, useState } from 'react';
import { adminApi } from '../api.js';

const S = {
  panel: { padding: '24px 32px', color: '#c9d1d9' },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 },
  title: { fontSize: 22, fontWeight: 600, margin: 0 },
  newBtn: {
    background: '#238636', color: '#fff', border: 'none', borderRadius: 6,
    padding: '8px 16px', fontWeight: 600, cursor: 'pointer', fontSize: 14,
  },
  table: {
    width: '100%', borderCollapse: 'collapse', background: '#161b22',
    border: '1px solid #30363d', borderRadius: 8, overflow: 'hidden',
  },
  th: {
    textAlign: 'left', padding: '10px 14px', fontSize: 12,
    color: '#8b949e', fontWeight: 600, borderBottom: '1px solid #30363d',
    textTransform: 'uppercase', letterSpacing: 0.4,
  },
  td: {
    padding: '12px 14px', fontSize: 14, borderBottom: '1px solid #30363d',
    color: '#c9d1d9',
  },
  badge: {
    display: 'inline-block', padding: '2px 8px', borderRadius: 10,
    fontSize: 11, fontWeight: 600, textTransform: 'uppercase',
  },
  badgeAdmin: { background: '#1f6feb22', color: '#58a6ff', border: '1px solid #1f6feb55' },
  badgeUser: { background: '#30363d', color: '#8b949e', border: '1px solid #30363d' },
  actionBtn: {
    background: 'transparent', border: '1px solid #30363d', color: '#c9d1d9',
    borderRadius: 4, padding: '4px 10px', fontSize: 12, cursor: 'pointer',
    marginRight: 6,
  },
  dangerBtn: {
    background: 'transparent', border: '1px solid #f8514955', color: '#f85149',
    borderRadius: 4, padding: '4px 10px', fontSize: 12, cursor: 'pointer',
  },
  modalBg: {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 100,
  },
  modal: {
    background: '#161b22', border: '1px solid #30363d', borderRadius: 12,
    padding: 28, width: 420, maxWidth: '90vw',
  },
  modalTitle: { margin: '0 0 16px 0', fontSize: 18, fontWeight: 600 },
  field: { marginBottom: 14 },
  label: { display: 'block', fontSize: 12, color: '#8b949e', marginBottom: 5, fontWeight: 500 },
  input: {
    width: '100%', padding: '8px 10px', background: '#0d1117',
    border: '1px solid #30363d', borderRadius: 6, color: '#c9d1d9',
    fontSize: 14, outline: 'none', boxSizing: 'border-box',
  },
  select: {
    width: '100%', padding: '8px 10px', background: '#0d1117',
    border: '1px solid #30363d', borderRadius: 6, color: '#c9d1d9',
    fontSize: 14, outline: 'none', boxSizing: 'border-box',
  },
  modalActions: { display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 20 },
  cancelBtn: {
    background: 'transparent', border: '1px solid #30363d', color: '#c9d1d9',
    borderRadius: 6, padding: '8px 16px', cursor: 'pointer', fontSize: 14,
  },
  primaryBtn: {
    background: '#238636', color: '#fff', border: 'none',
    borderRadius: 6, padding: '8px 16px', fontWeight: 600, cursor: 'pointer', fontSize: 14,
  },
  error: { color: '#f85149', fontSize: 13, marginTop: 8 },
  empty: { padding: 32, textAlign: 'center', color: '#8b949e' },
  loading: { padding: 32, textAlign: 'center', color: '#8b949e' },
};

function formatDate(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
  } catch {
    return iso;
  }
}

export default function UserManagementPanel({ currentUserId }) {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [modal, setModal] = useState(null); // null | 'create' | { mode: 'edit', user }
  const [form, setForm] = useState({ email: '', password: '', display_name: '', role: 'user' });
  const [submitting, setSubmitting] = useState(false);
  const [formError, setFormError] = useState('');

  const load = async () => {
    setLoading(true);
    setError('');
    try {
      const data = await adminApi.listUsers();
      setUsers(data);
    } catch (err) {
      setError(err.message || 'Failed to load users');
    }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const openCreate = () => {
    setForm({ email: '', password: '', display_name: '', role: 'user' });
    setFormError('');
    setModal('create');
  };

  const openEdit = (user) => {
    setForm({
      email: user.email,
      password: '',
      display_name: user.display_name,
      role: user.role,
    });
    setFormError('');
    setModal({ mode: 'edit', user });
  };

  const closeModal = () => { setModal(null); setFormError(''); };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setFormError('');
    setSubmitting(true);
    try {
      if (modal === 'create') {
        await adminApi.createUser(form);
      } else if (modal && modal.mode === 'edit') {
        const patch = {
          display_name: form.display_name,
          role: form.role,
        };
        if (form.password) patch.password = form.password;
        await adminApi.updateUser(modal.user.id, patch);
      }
      await load();
      closeModal();
    } catch (err) {
      setFormError(err.message || 'Operation failed');
    }
    setSubmitting(false);
  };

  const handleDelete = async (user) => {
    const purge = confirm(
      `Delete user ${user.email}?\n\n` +
      `Click OK to delete the account only (their data files are kept).\n` +
      `Click Cancel to abort.\n\n` +
      `(After confirming, you'll be asked whether to also purge their files.)`
    );
    if (!purge) return;
    const purgeFiles = confirm(
      `Also delete ${user.email}'s data files from disk?\n\n` +
      `OK = delete files too.\nCancel = keep files.`
    );
    try {
      await adminApi.deleteUser(user.id, purgeFiles);
      await load();
    } catch (err) {
      alert('Failed to delete: ' + (err.message || 'unknown error'));
    }
  };

  return (
    <div style={S.panel}>
      <div style={S.header}>
        <h2 style={S.title}>User Management</h2>
        <button style={S.newBtn} onClick={openCreate}>+ New User</button>
      </div>

      {loading && <div style={S.loading}>Loading users…</div>}
      {error && <div style={{ ...S.error, marginBottom: 16 }}>{error}</div>}

      {!loading && !error && (
        <table style={S.table}>
          <thead>
            <tr>
              <th style={S.th}>Email</th>
              <th style={S.th}>Display name</th>
              <th style={S.th}>Role</th>
              <th style={S.th}>Created</th>
              <th style={S.th}>Last login</th>
              <th style={S.th}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {users.length === 0 && (
              <tr><td style={S.td} colSpan="6"><div style={S.empty}>No users yet.</div></td></tr>
            )}
            {users.map(u => (
              <tr key={u.id}>
                <td style={S.td}>{u.email}</td>
                <td style={S.td}>{u.display_name}</td>
                <td style={S.td}>
                  <span style={{ ...S.badge, ...(u.role === 'admin' ? S.badgeAdmin : S.badgeUser) }}>
                    {u.role}
                  </span>
                </td>
                <td style={S.td}>{formatDate(u.created_at)}</td>
                <td style={S.td}>{formatDate(u.last_login)}</td>
                <td style={S.td}>
                  <button style={S.actionBtn} onClick={() => openEdit(u)}>Edit</button>
                  {u.id !== currentUserId && (
                    <button style={S.dangerBtn} onClick={() => handleDelete(u)}>Delete</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {modal && (
        <div style={S.modalBg} onClick={closeModal}>
          <form style={S.modal} onClick={(e) => e.stopPropagation()} onSubmit={handleSubmit}>
            <h3 style={S.modalTitle}>
              {modal === 'create' ? 'Create new user' : `Edit ${modal.user.email}`}
            </h3>

            {modal === 'create' && (
              <div style={S.field}>
                <label style={S.label}>Email</label>
                <input
                  style={S.input}
                  type="email"
                  value={form.email}
                  onChange={(e) => setForm({ ...form, email: e.target.value })}
                  required
                  autoFocus
                />
              </div>
            )}

            <div style={S.field}>
              <label style={S.label}>Display name</label>
              <input
                style={S.input}
                type="text"
                value={form.display_name}
                onChange={(e) => setForm({ ...form, display_name: e.target.value })}
                required
                minLength={1}
                maxLength={120}
                autoFocus={modal !== 'create'}
              />
            </div>

            <div style={S.field}>
              <label style={S.label}>
                {modal === 'create' ? 'Password' : 'New password (leave empty to keep current)'}
              </label>
              <input
                style={S.input}
                type="password"
                value={form.password}
                onChange={(e) => setForm({ ...form, password: e.target.value })}
                required={modal === 'create'}
                minLength={6}
              />
            </div>

            <div style={S.field}>
              <label style={S.label}>Role</label>
              <select
                style={S.select}
                value={form.role}
                onChange={(e) => setForm({ ...form, role: e.target.value })}
              >
                <option value="user">user</option>
                <option value="admin">admin</option>
              </select>
            </div>

            {formError && <div style={S.error}>{formError}</div>}

            <div style={S.modalActions}>
              <button type="button" style={S.cancelBtn} onClick={closeModal}>Cancel</button>
              <button type="submit" style={S.primaryBtn} disabled={submitting}>
                {submitting ? 'Saving…' : (modal === 'create' ? 'Create' : 'Save')}
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  );
}
