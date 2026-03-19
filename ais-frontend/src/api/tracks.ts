import { fetchJson } from './client';
import type { BboxState, FilterState, GeoJsonFeatureCollection } from '../types/ais';

export async function fetchTracks(
  bbox: BboxState,
  filter: FilterState,
  signal?: AbortSignal,
): Promise<GeoJsonFeatureCollection> {
  return fetchJson<GeoJsonFeatureCollection>(
    '/api/tracks',
    {
      minLon: bbox.minLon,
      minLat: bbox.minLat,
      maxLon: bbox.maxLon,
      maxLat: bbox.maxLat,
      mmsi: filter.mmsi || undefined,
      endDate: filter.atTime ? filter.atTime.split('T')[0] : undefined,
    },
    signal,
  );
}
