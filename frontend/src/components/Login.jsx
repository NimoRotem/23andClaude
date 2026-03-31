import { useState } from 'react';

const S = {
  wrapper: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    minHeight: '100vh',
    background: '#0d1117',
  },
  card: {
    background: '#161b22',
    border: '1px solid #30363d',
    borderRadius: 12,
    padding: '40px 36px',
    width: 360,
    boxShadow: '0 8px 24px rgba(0,0,0,.4)',
  },
  logo: {
    textAlign: 'center',
    marginBottom: 28,
  },
  logoIcon: {
    fontSize: 36,
    marginBottom: 8,
  },
  title: {
    color: '#c9d1d9',
    fontSize: 22,
    fontWeight: 600,
    margin: 0,
  },
  subtitle: {
    color: '#8b949e',
    fontSize: 13,
    marginTop: 4,
  },
  field: {
    marginBottom: 16,
  },
  label: {
    display: 'block',
    color: '#8b949e',
    fontSize: 13,
    marginBottom: 6,
    fontWeight: 500,
  },
  input: {
    width: '100%',
    padding: '10px 12px',
    background: '#0d1117',
    border: '1px solid #30363d',
    borderRadius: 6,
    color: '#c9d1d9',
    fontSize: 14,
    outline: 'none',
    boxSizing: 'border-box',
    transition: 'border-color 0.2s',
  },
  btn: {
    width: '100%',
    padding: '10px 0',
    background: '#238636',
    color: '#fff',
    border: 'none',
    borderRadius: 6,
    fontSize: 15,
    fontWeight: 600,
    cursor: 'pointer',
    marginTop: 8,
    transition: 'background 0.2s',
  },
  btnDisabled: {
    background: '#1a5c2a',
    cursor: 'not-allowed',
  },
  error: {
    color: '#f85149',
    fontSize: 13,
    marginTop: 12,
    textAlign: 'center',
  },
};

export default function Login({ onLogin }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      const base = import.meta.env.BASE_URL || '/genomics/';
      const res = await fetch(`${base}api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });

      const data = await res.json();

      if (!res.ok) {
        setError(data.detail || 'Login failed');
        setLoading(false);
        return;
      }

      localStorage.setItem('auth_token', data.access_token);
      localStorage.setItem('refresh_token', data.refresh_token);
      onLogin(data);
    } catch (err) {
      setError('Connection failed');
    }
    setLoading(false);
  };

  return (
    <div style={S.wrapper}>
      <form style={S.card} onSubmit={handleSubmit}>
        <div style={S.logo}>
          <div style={S.logoIcon}>&#x1F9EC;</div>
          <h1 style={S.title}>23andClaude</h1>
          <div style={S.subtitle}>Sign in to continue</div>
        </div>

        <div style={S.field}>
          <label style={S.label}>Email</label>
          <input
            style={S.input}
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder="admin@genomics.local"
            autoFocus
            required
          />
        </div>

        <div style={S.field}>
          <label style={S.label}>Password</label>
          <input
            style={S.input}
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="Enter password"
            required
          />
        </div>

        <button
          type="submit"
          style={{ ...S.btn, ...(loading ? S.btnDisabled : {}) }}
          disabled={loading}
        >
          {loading ? 'Signing in...' : 'Sign in'}
        </button>

        {error && <div style={S.error}>{error}</div>}
      </form>
    </div>
  );
}
