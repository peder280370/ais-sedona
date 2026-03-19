import { useState } from 'react';
import { OlMap } from './map/OlMap';
import { FilterPanel } from './components/FilterPanel';
import type { FilterState, PositionRecord } from './types/ais';

const DEFAULT_FILTER: FilterState = {
  atTime: '2026-03-14T12:00',
  mmsi: '',
  vesselName: '',
  showTracks: false,
};

export function App() {
  const [filter, setFilter] = useState<FilterState>(DEFAULT_FILTER);
  const [selectedPosition, setSelectedPosition] = useState<PositionRecord | null>(null);

  return (
    <div style={{ width: '100%', height: '100%', position: 'relative' }}>
      <OlMap
        filter={filter}
        selectedPosition={selectedPosition}
        onVesselClick={(pos) => setSelectedPosition(pos)}
        onPopupClose={() => setSelectedPosition(null)}
      />
      <FilterPanel filter={filter} onChange={setFilter} />
    </div>
  );
}
