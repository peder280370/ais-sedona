import { useEffect, useState } from 'react';
import { fetchTracks } from '../api/tracks';
import type { BboxState, FilterState, GeoJsonFeatureCollection } from '../types/ais';

const EMPTY: GeoJsonFeatureCollection = { type: 'FeatureCollection', features: [] };

export function useTracks(
  bbox: BboxState | null,
  filter: FilterState,
  enabled: boolean,
): GeoJsonFeatureCollection {
  const [tracks, setTracks] = useState<GeoJsonFeatureCollection>(EMPTY);

  useEffect(() => {
    if (!enabled || !bbox) {
      setTracks(EMPTY);
      return;
    }

    const controller = new AbortController();

    fetchTracks(bbox, filter, controller.signal)
      .then(setTracks)
      .catch((err: unknown) => {
        if (err instanceof Error && err.name !== 'AbortError') {
          console.error('fetchTracks failed:', err);
        }
      });

    return () => controller.abort();
  }, [
    enabled,
    bbox?.minLon,
    bbox?.minLat,
    bbox?.maxLon,
    bbox?.maxLat,
    filter.atTime,
    filter.mmsi,
  ]);

  return tracks;
}
