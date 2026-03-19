import { useEffect, useRef, useState } from 'react';
import Map from 'ol/Map';
import View from 'ol/View';
import TileLayer from 'ol/layer/Tile';
import OSM from 'ol/source/OSM';
import { fromLonLat } from 'ol/proj';
import 'ol/ol.css';

import { MapContext } from './MapContext';
import { useMapBbox } from '../hooks/useMapBbox';
import type { BboxState, FilterState, PositionRecord } from '../types/ais';
import { VesselLayer } from './VesselLayer';
import { TracksLayer } from './TracksLayer';
import { VesselPopup } from '../components/VesselPopup';
import { usePositions } from '../hooks/usePositions';
import { useTracks } from '../hooks/useTracks';

interface OlMapProps {
  filter: FilterState;
  selectedPosition: PositionRecord | null;
  onVesselClick: (pos: PositionRecord) => void;
  onPopupClose: () => void;
}

export function OlMap({ filter, selectedPosition, onVesselClick, onPopupClose }: OlMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [olMap, setOlMap] = useState<Map | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new Map({
      target: containerRef.current,
      layers: [new TileLayer({ source: new OSM() })],
      view: new View({
        center: fromLonLat([11, 56]),
        zoom: 7,
      }),
    });

    setOlMap(map);

    return () => {
      map.setTarget(undefined);
      setOlMap(null);
    };
  }, []);

  const bbox: BboxState | null = useMapBbox(olMap);
  const positions = usePositions(bbox, filter);
  const tracks = useTracks(bbox, filter, filter.showTracks);

  return (
    <MapContext.Provider value={olMap}>
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      {olMap && (
        <>
          <VesselLayer positions={positions} onVesselClick={onVesselClick} />
          {filter.showTracks && <TracksLayer tracks={tracks} />}
          {selectedPosition && (
            <VesselPopup position={selectedPosition} onClose={onPopupClose} />
          )}
        </>
      )}
    </MapContext.Provider>
  );
}
