export interface PositionRecord {
  mmsi: number;
  ts: string;
  geomWkt: string;
  sog: number | null;
  cog: number | null;
  heading: number | null;
  navStatus: number | null;
  rot: number | null;
  msgType: number | null;
  // Enriched from vessel metadata
  vesselName: string | null;
  shipType: number | null;
  shipTypeDesc: string | null;
  lengthM: number | null;
}

export interface VesselRecord {
  mmsi: number;
  imo: number | null;
  vesselName: string | null;
  callsign: string | null;
  shipType: number | null;
  shipTypeDesc: string | null;
  lengthM: number | null;
  beamM: number | null;
  draughtM: number | null;
  destination: string | null;
  lastSeen: string | null;
}

export interface GeoJsonFeatureCollection {
  type: 'FeatureCollection';
  features: GeoJsonFeature[];
}

export interface GeoJsonFeature {
  type: 'Feature';
  geometry: {
    type: string;
    coordinates: unknown;
  };
  properties: Record<string, unknown>;
}

export interface BboxState {
  minLon: number;
  minLat: number;
  maxLon: number;
  maxLat: number;
}

export interface FilterState {
  atTime: string;
  mmsi: string;
  vesselName: string;
  showTracks: boolean;
}
