import Style from 'ol/style/Style';
import Icon from 'ol/style/Icon';
import type Feature from 'ol/Feature';
import type { FeatureLike } from 'ol/Feature';
import type { PositionRecord } from '../types/ais';

const canvasCache = new Map<string, HTMLCanvasElement>();

function shipColorFromCode(code: number): string {
  if (code === 36 || code === 37) return '#ffd600'; // Sailing / pleasure craft
  if (code >= 70 && code <= 79) return '#4caf50';   // Cargo
  if (code >= 80 && code <= 89) return '#f44336';   // Tanker
  if (code >= 60 && code <= 69) return '#9c27b0';   // Passenger
  if (code >= 30 && code <= 39) return '#2196f3';   // Fishing
  if (code >= 50 && code <= 59) return '#ff9800';   // Special (tug, SAR, pilot…)
  if (code >= 40 && code <= 49) return '#00bcd4';   // High-speed craft
  return '#888888';
}

function shipColor(shipType: number | null | undefined, shipTypeDesc: string | null | undefined): string {
  // Integer code takes priority (NMEA data and fixed CSV imports)
  if (shipType != null) return shipColorFromCode(shipType);

  // Fallback: shipTypeDesc may be a numeric string (old CSV imports) or a text label
  if (shipTypeDesc != null) {
    const n = Number(shipTypeDesc);
    if (Number.isFinite(n)) return shipColorFromCode(n);

    const d = shipTypeDesc.toLowerCase();
    if (d.includes('cargo'))     return '#4caf50';
    if (d.includes('tanker'))    return '#f44336';
    if (d.includes('passenger')) return '#9c27b0';
    if (d.includes('fishing'))   return '#2196f3';
    if (d.includes('sailing') || d.includes('pleasure')) return '#ffd600';
    if (d.includes('tug') || d.includes('sar') || d.includes('pilot')) return '#ff9800';
    if (d.includes('high') && d.includes('speed')) return '#00bcd4';
  }

  return '#888888';
}

function shipSizePx(lengthM: number | null | undefined): number {
  if (lengthM == null) return 16;
  if (lengthM < 50) return 12;
  if (lengthM < 200) return 18;
  return 24;
}

function getChevronCanvas(color: string, size: number): HTMLCanvasElement {
  const key = `${color}-${size}`;
  const cached = canvasCache.get(key);
  if (cached) return cached;

  const canvas = document.createElement('canvas');
  canvas.width = size;
  canvas.height = size;
  const ctx = canvas.getContext('2d')!;

  const cx = size / 2;
  const cy = size / 2;
  // Scale chevron proportionally to canvas size (designed for size=20)
  const s = size / 20;

  ctx.beginPath();
  ctx.moveTo(cx,           2 * s);           // tip
  ctx.lineTo(cx + 7 * s,  (cy + 7) * s);    // bottom-right
  ctx.lineTo(cx,           (cy + 3) * s);    // inner bottom
  ctx.lineTo(cx - 7 * s,  (cy + 7) * s);    // bottom-left
  ctx.closePath();

  ctx.fillStyle = color;
  ctx.fill();
  ctx.strokeStyle = 'rgba(0,0,0,0.55)';
  ctx.lineWidth = 1;
  ctx.stroke();

  canvasCache.set(key, canvas);
  return canvas;
}

export function vesselStyleFn(feature: FeatureLike): Style {
  const pos = (feature as Feature).get('pos') as PositionRecord;
  const color = shipColor(pos?.shipType, pos?.shipTypeDesc);
  const size = shipSizePx(pos?.lengthM);
  const canvas = getChevronCanvas(color, size);

  // Use COG; fall back to heading (511 = unavailable), then 0
  let angleDeg = pos?.cog ?? 0;
  if (angleDeg == null || isNaN(angleDeg)) {
    const h = pos?.heading;
    angleDeg = h != null && h !== 511 ? h : 0;
  }
  const rotation = (angleDeg * Math.PI) / 180;

  return new Style({
    image: new Icon({
      img: canvas,
      width: size,
      height: size,
      rotation,
      rotateWithView: false,
    }),
  });
}
