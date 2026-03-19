import { useEffect, useRef } from 'react';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import GeoJSON from 'ol/format/GeoJSON';
import Style from 'ol/style/Style';
import Stroke from 'ol/style/Stroke';

import { useMap } from './MapContext';
import type { GeoJsonFeatureCollection } from '../types/ais';

const trackStyle = new Style({
  stroke: new Stroke({
    color: '#0099FF',
    width: 2,
    lineDash: [4, 4],
  }),
});

const geojsonParser = new GeoJSON();

interface TracksLayerProps {
  tracks: GeoJsonFeatureCollection;
}

export function TracksLayer({ tracks }: TracksLayerProps) {
  const map = useMap();
  const sourceRef = useRef<VectorSource | null>(null);

  useEffect(() => {
    if (!map) return;

    const source = new VectorSource();
    const layer = new VectorLayer({ source, style: trackStyle });

    map.addLayer(layer);
    sourceRef.current = source;

    return () => {
      map.removeLayer(layer);
    };
  }, [map]);

  useEffect(() => {
    const source = sourceRef.current;
    if (!source) return;

    source.clear();
    const features = geojsonParser.readFeatures(tracks, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857',
    });
    source.addFeatures(features);
  }, [tracks]);

  return null;
}
