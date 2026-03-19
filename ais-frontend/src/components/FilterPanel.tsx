import { useState, useEffect } from 'react';
import { useVesselSearch } from '../hooks/useVesselSearch';
import type { FilterState } from '../types/ais';

interface FilterPanelProps {
  filter: FilterState;
  onChange: (f: FilterState) => void;
}

export function FilterPanel({ filter, onChange }: FilterPanelProps) {
  const [nameInput, setNameInput] = useState(filter.vesselName);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [playing, setPlaying] = useState(false);
  const suggestions = useVesselSearch(nameInput);

  useEffect(() => {
    if (!playing) return;
    const id = setInterval(() => {
      const d = new Date(filter.atTime + ':00Z');
      d.setMinutes(d.getMinutes() + 5);
      set('atTime', d.toISOString().slice(0, 16));
    }, 5000);
    return () => clearInterval(id);
  }, [playing, filter.atTime]);

  function set<K extends keyof FilterState>(key: K, value: FilterState[K]) {
    onChange({ ...filter, [key]: value });
  }

  return (
    <div style={panelStyle}>
      <h3 style={{ marginBottom: 8, fontSize: 13, fontWeight: 700, letterSpacing: 0.5 }}>
        FILTERS
      </h3>

      <label style={labelStyle}>At time (UTC)</label>
      <div style={{ display: 'flex', gap: 4 }}>
        <input
          type="datetime-local"
          value={filter.atTime}
          onChange={(e) => set('atTime', e.target.value)}
          style={{ ...inputStyle, flex: 1 }}
        />
        <button
          onClick={() => setPlaying((p) => !p)}
          title={playing ? 'Pause' : 'Play'}
          style={{
            background: playing ? '#4a9eff' : '#1e1e2e',
            border: `1px solid ${playing ? '#4a9eff' : '#444'}`,
            borderRadius: 4,
            color: '#fff',
            cursor: 'pointer',
            fontSize: 13,
            padding: '0 7px',
            lineHeight: 1,
          }}
        >
          {playing ? '⏸' : '▶'}
        </button>
      </div>

      <label style={labelStyle}>MMSI</label>
      <input
        type="text"
        inputMode="numeric"
        pattern="[0-9]*"
        placeholder="e.g. 219000001"
        value={filter.mmsi}
        onChange={(e) => {
          const v = e.target.value.replace(/\D/g, '');
          set('mmsi', v);
        }}
        style={inputStyle}
      />

      <label style={labelStyle}>Vessel name</label>
      <div style={{ position: 'relative' }}>
        <input
          type="text"
          placeholder="Search name…"
          value={nameInput}
          onChange={(e) => {
            setNameInput(e.target.value);
            setShowSuggestions(true);
            if (!e.target.value) set('vesselName', '');
          }}
          onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
          style={inputStyle}
        />
        {showSuggestions && suggestions.length > 0 && (
          <ul style={dropdownStyle}>
            {suggestions.slice(0, 8).map((v) => (
              <li
                key={v.mmsi}
                style={suggestionStyle}
                onMouseDown={() => {
                  setNameInput(v.vesselName ?? '');
                  set('vesselName', v.vesselName ?? '');
                  setShowSuggestions(false);
                }}
              >
                {v.vesselName} <span style={{ color: '#aaa', fontSize: 11 }}>{v.mmsi}</span>
              </li>
            ))}
          </ul>
        )}
      </div>

      <label style={{ ...labelStyle, display: 'flex', alignItems: 'center', gap: 6, marginTop: 10 }}>
        <input
          type="checkbox"
          checked={filter.showTracks}
          onChange={(e) => set('showTracks', e.target.checked)}
        />
        Show tracks
      </label>
    </div>
  );
}

const panelStyle: React.CSSProperties = {
  position: 'absolute',
  top: 12,
  left: 12,
  zIndex: 100,
  background: 'rgba(20,20,30,0.90)',
  color: '#fff',
  borderRadius: 8,
  padding: '14px 16px',
  width: 220,
  boxShadow: '0 2px 12px rgba(0,0,0,0.5)',
  backdropFilter: 'blur(4px)',
  fontSize: 13,
};

const labelStyle: React.CSSProperties = {
  display: 'block',
  marginBottom: 2,
  marginTop: 8,
  fontSize: 11,
  color: '#aaa',
  textTransform: 'uppercase',
  letterSpacing: 0.5,
};

const inputStyle: React.CSSProperties = {
  width: '100%',
  padding: '4px 6px',
  borderRadius: 4,
  border: '1px solid #444',
  background: '#1e1e2e',
  color: '#fff',
  fontSize: 13,
  outline: 'none',
};

const dropdownStyle: React.CSSProperties = {
  position: 'absolute',
  top: '100%',
  left: 0,
  right: 0,
  background: '#1e1e2e',
  border: '1px solid #444',
  borderRadius: 4,
  listStyle: 'none',
  zIndex: 200,
  maxHeight: 160,
  overflowY: 'auto',
};

const suggestionStyle: React.CSSProperties = {
  padding: '5px 8px',
  cursor: 'pointer',
  fontSize: 12,
};
