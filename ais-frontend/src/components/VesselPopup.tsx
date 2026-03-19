import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import Overlay from 'ol/Overlay';
import { fromLonLat } from 'ol/proj';
import WKT from 'ol/format/WKT';
import type Point from 'ol/geom/Point';

import { useMap } from '../map/MapContext';
import { fetchVesselByMmsi } from '../api/vessels';
import type { PositionRecord, VesselRecord } from '../types/ais';

const wkt = new WKT();

interface VesselPopupProps {
  position: PositionRecord;
  onClose: () => void;
}

export function VesselPopup({ position, onClose }: VesselPopupProps) {
  const map = useMap();
  // Create the container element imperatively so OL owns it, not React.
  const containerRef = useRef<HTMLDivElement>(document.createElement('div'));
  const [vessel, setVessel] = useState<VesselRecord | null>(null);

  // Attach OL Overlay
  useEffect(() => {
    if (!map) return;

    const overlay = new Overlay({
      element: containerRef.current,
      positioning: 'bottom-center',
      offset: [0, -8],
      stopEvent: true,
    });

    map.addOverlay(overlay);

    // Parse coordinate from WKT POINT
    try {
      const geom = wkt.readGeometry(position.geomWkt, {
        dataProjection: 'EPSG:4326',
        featureProjection: 'EPSG:3857',
      });
      const coords = (geom as unknown as Point).getCoordinates();
      overlay.setPosition(coords);
    } catch {
      // Fallback: try to parse POINT(lon lat)
      const m = position.geomWkt.match(/POINT\s*\(([^\)]+)\)/i);
      if (m) {
        const [lon, lat] = m[1].split(/\s+/).map(Number);
        overlay.setPosition(fromLonLat([lon, lat]));
      }
    }

    return () => {
      map.removeOverlay(overlay);
    };
  }, [map, position.geomWkt]);

  // Fetch vessel details
  useEffect(() => {
    const controller = new AbortController();
    fetchVesselByMmsi(position.mmsi, controller.signal).then(setVessel);
    return () => controller.abort();
  }, [position.mmsi]);

  const navStatusLabel = position.navStatus != null ? NAV_STATUS[position.navStatus] ?? `Status ${position.navStatus}` : '—';

  return createPortal(
    <div style={popupStyle}>
      <button onClick={onClose} style={closeStyle} title="Close">×</button>
      <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 4 }}>
        {position.vesselName ?? vessel?.vesselName ?? `MMSI ${position.mmsi}`}
      </div>
      <table style={{ fontSize: 12, borderCollapse: 'collapse', width: '100%' }}>
        <tbody>
          <Row label="MMSI" value={String(position.mmsi)} />
          <Row label="Type" value={position.shipTypeDesc ?? vessel?.shipTypeDesc ?? '—'} />
          <Row label="SOG" value={position.sog != null ? `${position.sog.toFixed(1)} kn` : '—'} />
          <Row label="COG" value={position.cog != null ? `${position.cog.toFixed(1)}°` : '—'} />
          <Row label="Nav status" value={navStatusLabel} />
          <Row label="Last seen" value={position.ts ?? vessel?.lastSeen ?? '—'} />
          {vessel?.destination && <Row label="Destination" value={vessel.destination} />}
        </tbody>
      </table>
    </div>,
    containerRef.current,
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <tr>
      <td style={{ color: '#888', paddingRight: 8, paddingBottom: 2, whiteSpace: 'nowrap' }}>{label}</td>
      <td style={{ paddingBottom: 2 }}>{value}</td>
    </tr>
  );
}

const NAV_STATUS: Record<number, string> = {
  0: 'Under way (engine)',
  1: 'At anchor',
  2: 'Not under command',
  3: 'Restricted maneuverability',
  4: 'Constrained by draught',
  5: 'Moored',
  6: 'Aground',
  7: 'Engaged in fishing',
  8: 'Under way (sailing)',
  15: 'Undefined',
};

const popupStyle: React.CSSProperties = {
  background: 'rgba(20,20,30,0.95)',
  color: '#fff',
  borderRadius: 8,
  padding: '12px 14px',
  minWidth: 200,
  boxShadow: '0 4px 16px rgba(0,0,0,0.6)',
  position: 'relative',
  fontSize: 13,
  backdropFilter: 'blur(4px)',
  border: '1px solid rgba(255,255,255,0.12)',
};

const closeStyle: React.CSSProperties = {
  position: 'absolute',
  top: 6,
  right: 8,
  background: 'none',
  border: 'none',
  color: '#aaa',
  fontSize: 18,
  cursor: 'pointer',
  lineHeight: 1,
  padding: 0,
};
