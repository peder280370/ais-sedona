import { fetchJson } from './client';
import type { BboxState, FilterState, PositionRecord } from '../types/ais';

export async function fetchPositions(
  bbox: BboxState,
  filter: FilterState,
  signal?: AbortSignal,
): Promise<PositionRecord[]> {
  return fetchJson<PositionRecord[]>(
    '/api/positions',
    {
      minLon: bbox.minLon,
      minLat: bbox.minLat,
      maxLon: bbox.maxLon,
      maxLat: bbox.maxLat,
      mmsi: filter.mmsi || undefined,
      at: filter.atTime || undefined,
      limit: 10000,
    },
    signal,
  );
}
