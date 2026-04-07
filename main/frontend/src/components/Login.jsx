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
  toggleRow: {
    marginTop: 18,
    textAlign: 'center',
    color: '#8b949e',
    fontSize: 13,
  },
  toggleLink: {
    background: 'none',
    border: 'none',
    color: '#58a6ff',
    cursor: 'pointer',
    fontSize: 13,
    fontWeight: 500,
    padding: 0,
    marginLeft: 4,
    textDecoration: 'underline',
  },
};

export default function Login({ onLogin }) {
  const [mode, setMode] = useState('login'); // 'login' | 'signup'
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      const base = import.meta.env.BASE_URL || '/genomics/';
      const endpoint = mode === 'signup' ? 'register' : 'login';
      const body =
        mode === 'signup'
          ? { email, password, display_name: displayName }
          : { email, password };

      const res = await fetch(`${base}api/auth/${endpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      const data = await res.json();

      if (!res.ok) {
        setError(data.detail || (mode === 'signup' ? 'Sign up failed' : 'Login failed'));
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

  const toggleMode = () => {
    setError('');
    setMode(mode === 'login' ? 'signup' : 'login');
  };

  const isSignup = mode === 'signup';

  return (
    <div style={S.wrapper}>
      <form style={S.card} onSubmit={handleSubmit}>
        <div style={S.logo}>
          <div style={S.logoIcon}>&#x1F9EC;</div>
          <h1 style={S.title}>Genomics Dashboard</h1>
          <div style={S.subtitle}>
            {isSignup ? 'Create your account' : 'Sign in to continue'}
          </div>
        </div>

        {isSignup && (
          <div style={S.field}>
            <label style={S.label}>Display name</label>
            <input
              style={S.input}
              type="text"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              placeholder="Your name"
              autoFocus
              required
              minLength={1}
              maxLength={120}
            />
          </div>
        )}

        <div style={S.field}>
          <label style={S.label}>Email</label>
          <input
            style={S.input}
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder={isSignup ? 'you@example.com' : 'admin@genomics.local'}
            autoFocus={!isSignup}
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
            placeholder={isSignup ? 'At least 6 characters' : 'Enter password'}
            required
            minLength={isSignup ? 6 : undefined}
          />
        </div>

        <button
          type="submit"
          style={{ ...S.btn, ...(loading ? S.btnDisabled : {}) }}
          disabled={loading}
        >
          {loading
            ? isSignup
              ? 'Creating account...'
              : 'Signing in...'
            : isSignup
              ? 'Create account'
              : 'Sign in'}
        </button>

        {error && <div style={S.error}>{error}</div>}

        <div style={S.toggleRow}>
          {isSignup ? 'Already have an account?' : "Don't have an account?"}
          <button type="button" style={S.toggleLink} onClick={toggleMode}>
            {isSignup ? 'Sign in' : 'Sign up'}
          </button>
        </div>
      </form>
    </div>
  );
}
