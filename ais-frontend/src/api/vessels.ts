import { fetchJson } from './client';
import type { VesselRecord } from '../types/ais';

export async function fetchVessels(
  params: { mmsi?: string; name?: string },
  signal?: AbortSignal,
): Promise<VesselRecord[]> {
  return fetchJson<VesselRecord[]>('/api/vessels', params, signal);
}

export async function fetchVesselByMmsi(
  mmsi: number,
  signal?: AbortSignal,
): Promise<VesselRecord | null> {
  try {
    return await fetchJson<VesselRecord>(`/api/vessels/${mmsi}`, {}, signal);
  } catch {
    return null;
  }
}
