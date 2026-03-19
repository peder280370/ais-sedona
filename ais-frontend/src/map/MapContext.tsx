import { createContext, useContext } from 'react';
import type Map from 'ol/Map';

export const MapContext = createContext<Map | null>(null);

export function useMap(): Map | null {
  return useContext(MapContext);
}
