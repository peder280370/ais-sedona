import { useEffect, useRef, useState } from 'react';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import Overlay from 'ol/Overlay';
import type { Geometry } from 'ol/geom';
import WKT from 'ol/format/WKT';
import Feature from 'ol/Feature';
import type { MapBrowserEvent } from 'ol';

import { useMap } from './MapContext';
import { vesselStyleFn } from './vesselStyle';
import type { PositionRecord } from '../types/ais';

const wktParser = new WKT();

interface VesselLayerProps {
  positions: PositionRecord[];
  onVesselClick: (pos: PositionRecord) => void;
}

export function VesselLayer({ positions, onVesselClick }: VesselLayerProps) {
  const map = useMap();
  const layerRef = useRef<VectorLayer<Feature<Geometry>> | null>(null);
  const sourceRef = useRef<VectorSource<Feature<Geometry>> | null>(null);
  const onClickRef = useRef(onVesselClick);
  onClickRef.current = onVesselClick;

  const tooltipRef = useRef<HTMLDivElement>(null);
  const [hoveredPos, setHoveredPos] = useState<PositionRecord | null>(null);

  // Layer setup
  useEffect(() => {
    if (!map) return;

    const source = new VectorSource<Feature<Geometry>>();
    const layer = new VectorLayer<Feature<Geometry>>({
      source,
      style: vesselStyleFn,
    });

    map.addLayer(layer);
    sourceRef.current = source;
    layerRef.current = layer;

    function handleClick(evt: MapBrowserEvent<MouseEvent>) {
      const features = map!.getFeaturesAtPixel(evt.pixel, { layerFilter: (l) => l === layer });
      if (features.length > 0) {
        const pos = (features[0] as Feature).get('pos') as PositionRecord;
        onClickRef.current(pos);
      }
    }

    map.on('click', handleClick);

    return () => {
      map.un('click', handleClick);
      map.removeLayer(layer);
    };
  }, [map]);

  // Hover tooltip overlay
  useEffect(() => {
    if (!map || !tooltipRef.current) return;

    const overlay = new Overlay({
      element: tooltipRef.current,
      positioning: 'bottom-center',
      offset: [0, -12],
      stopEvent: false,
    });
    map.addOverlay(overlay);

    function handlePointerMove(evt: MapBrowserEvent<MouseEvent>) {
      const features = map!.getFeaturesAtPixel(evt.pixel, {
        layerFilter: (l) => l === layerRef.current,
      });
      if (features.length > 0) {
        const pos = (features[0] as Feature).get('pos') as PositionRecord;
        setHoveredPos(pos);
        overlay.setPosition(evt.coordinate);
        map!.getViewport().style.cursor = 'pointer';
      } else {
        setHoveredPos(null);
        overlay.setPosition(undefined);
        map!.getViewport().style.cursor = '';
      }
    }

    map.on('pointermove', handlePointerMove);

    return () => {
      map.un('pointermove', handlePointerMove);
      map.removeOverlay(overlay);
    };
  }, [map]);

  // Sync positions to vector source
  useEffect(() => {
    const source = sourceRef.current;
    if (!source) return;

    source.clear();

    const features: Feature[] = [];
    for (const pos of positions) {
      try {
        const geom = wktParser.readGeometry(pos.geomWkt, {
          dataProjection: 'EPSG:4326',
          featureProjection: 'EPSG:3857',
        });
        const feature = new Feature({ geometry: geom, pos });
        features.push(feature);
      } catch {
        // skip malformed WKT
      }
    }
    source.addFeatures(features);
  }, [positions]);

  return (
    <div ref={tooltipRef} style={{ display: hoveredPos ? 'block' : 'none', ...tooltipStyle }}>
      {hoveredPos && (
        <>
          <div style={{ fontWeight: 700, marginBottom: 2 }}>
            {hoveredPos.vesselName ?? `MMSI ${hoveredPos.mmsi}`}
          </div>
          <div style={{ color: '#bbb', fontSize: 11 }}>
            {[
              hoveredPos.shipTypeDesc,
              hoveredPos.sog != null ? `${hoveredPos.sog.toFixed(1)} kn` : null,
            ]
              .filter(Boolean)
              .join(' · ')}
          </div>
        </>
      )}
    </div>
  );
}

const tooltipStyle: React.CSSProperties = {
  background: 'rgba(20,20,30,0.92)',
  color: '#fff',
  borderRadius: 6,
  padding: '6px 10px',
  fontSize: 12,
  pointerEvents: 'none',
  whiteSpace: 'nowrap',
  boxShadow: '0 2px 8px rgba(0,0,0,0.5)',
  border: '1px solid rgba(255,255,255,0.1)',
};
