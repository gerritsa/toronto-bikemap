export function decodePolyline(encoded: string, precision = 5): [number, number][] {
  let index = 0;
  let lat = 0;
  let lng = 0;
  const coordinates: [number, number][] = [];
  const factor = Math.pow(10, precision);

  while (index < encoded.length) {
    let result = 0;
    let shift = 0;
    let byte: number;

    do {
      byte = encoded.charCodeAt(index++) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20);

    lat += result & 1 ? ~(result >> 1) : result >> 1;
    result = 0;
    shift = 0;

    do {
      byte = encoded.charCodeAt(index++) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20);

    lng += result & 1 ? ~(result >> 1) : result >> 1;
    coordinates.push([lng / factor, lat / factor]);
  }

  return coordinates;
}

export function timestampsForPath(path: [number, number][], startSeconds: number, endSeconds: number): number[] {
  if (path.length <= 1) {
    return [startSeconds];
  }

  const duration = Math.max(1, endSeconds - startSeconds);
  const segmentDistances = path.slice(1).map((point, index) => distanceMeters(path[index], point));
  const totalDistance = segmentDistances.reduce((sum, distance) => sum + distance, 0);
  if (totalDistance <= 0) {
    return path.map((_, index) => startSeconds + (duration * index) / (path.length - 1));
  }

  let elapsedDistance = 0;
  return path.map((_, index) => {
    if (index === 0) {
      return startSeconds;
    }
    elapsedDistance += segmentDistances[index - 1];
    return startSeconds + (duration * elapsedDistance) / totalDistance;
  });
}

function distanceMeters(from: [number, number], to: [number, number]) {
  const earthRadius = 6371000;
  const lat1 = (from[1] * Math.PI) / 180;
  const lat2 = (to[1] * Math.PI) / 180;
  const dLat = ((to[1] - from[1]) * Math.PI) / 180;
  const dLon = ((to[0] - from[0]) * Math.PI) / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return earthRadius * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}
