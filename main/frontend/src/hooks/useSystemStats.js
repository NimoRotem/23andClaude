import { useState, useEffect, useCallback, useRef } from 'react';
import { systemApi } from '../api.js';

export default function useSystemStats(intervalMs = 5000) {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const mountedRef = useRef(true);

  const fetchStats = useCallback(async () => {
    try {
      const data = await systemApi.stats();
      if (!mountedRef.current) return;
      setStats(data);
      setError(false);
      setLoading(false);
    } catch (e) {
      if (!mountedRef.current) return;
      console.error('Failed to fetch system stats:', e);
      if (!stats) {
        setError(true);
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    fetchStats();
    const iv = setInterval(fetchStats, intervalMs);
    return () => {
      mountedRef.current = false;
      clearInterval(iv);
    };
  }, [fetchStats, intervalMs]);

  return { stats, loading, error };
}
