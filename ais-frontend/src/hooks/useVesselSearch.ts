import { useEffect, useState } from 'react';
import { fetchVessels } from '../api/vessels';
import type { VesselRecord } from '../types/ais';

export function useVesselSearch(name: string): VesselRecord[] {
  const [results, setResults] = useState<VesselRecord[]>([]);

  useEffect(() => {
    if (name.length < 2) {
      setResults([]);
      return;
    }

    const controller = new AbortController();

    fetchVessels({ name }, controller.signal)
      .then(setResults)
      .catch((err: unknown) => {
        if (err instanceof Error && err.name !== 'AbortError') {
          console.error('fetchVessels failed:', err);
        }
      });

    return () => controller.abort();
  }, [name]);

  return results;
}
