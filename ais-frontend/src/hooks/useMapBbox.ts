import { useEffect, useState } from 'react';
import { toLonLat } from 'ol/proj';
import type Map from 'ol/Map';
import type { BboxState } from '../types/ais';

export function useMapBbox(map: Map | null): BboxState | null {
  const [bbox, setBbox] = useState<BboxState | null>(null);

  useEffect(() => {
    if (!map) return;

    let timer: ReturnType<typeof setTimeout> | null = null;

    function update() {
      const extent = map!.getView().calculateExtent(map!.getSize());
      const [minLon, minLat] = toLonLat([extent[0], extent[1]]);
      const [maxLon, maxLat] = toLonLat([extent[2], extent[3]]);
      setBbox({ minLon, minLat, maxLon, maxLat });
    }

    function onMoveEnd() {
      if (timer) clearTimeout(timer);
      timer = setTimeout(update, 400);
    }

    map.on('moveend', onMoveEnd);
    update();

    return () => {
      map.un('moveend', onMoveEnd);
      if (timer) clearTimeout(timer);
    };
  }, [map]);

  return bbox;
}
