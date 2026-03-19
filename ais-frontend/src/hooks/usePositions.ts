import { useEffect, useState } from 'react';
import { fetchPositions } from '../api/positions';
import type { BboxState, FilterState, PositionRecord } from '../types/ais';

export function usePositions(
  bbox: BboxState | null,
  filter: FilterState,
): PositionRecord[] {
  const [positions, setPositions] = useState<PositionRecord[]>([]);

  useEffect(() => {
    if (!bbox) return;

    const controller = new AbortController();

    fetchPositions(bbox, filter, controller.signal)
      .then(setPositions)
      .catch((err: unknown) => {
        if (err instanceof Error && err.name !== 'AbortError') {
          console.error('fetchPositions failed:', err);
        }
      });

    return () => controller.abort();
  }, [
    bbox?.minLon,
    bbox?.minLat,
    bbox?.maxLon,
    bbox?.maxLat,
    filter.atTime,
    filter.mmsi,
    filter.vesselName,
  ]);

  return positions;
}
