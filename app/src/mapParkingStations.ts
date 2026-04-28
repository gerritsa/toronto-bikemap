import type { GeoJSONSource, Map, MapMouseEvent } from "maplibre-gl";
import type { MutableRefObject } from "react";
import type { ParkingStation } from "./types";

export const STATIONS_SOURCE_ID = "bike-share-stations";
export const STATIONS_LAYER_ID = "bike-share-stations-dots";
export const STATIONS_HIT_LAYER_ID = "bike-share-stations-hit-area";

type StationPickHandlers = {
  onSelectRef: MutableRefObject<(payload: { lng: number; lat: number; name: string; stationId: string }) => void>;
  onClearRef: MutableRefObject<() => void>;
};

function stationDisplayName(name: string) {
  return name.replace(/\s+-\s+SMART$/i, "").trim();
}

function stationFeatureCollection(stations: ParkingStation[]) {
  const features = stations
    .filter((s) => Number.isFinite(s.lat) && Number.isFinite(s.lon))
    .map((s) => ({
      type: "Feature" as const,
      properties: { name: s.name, stationId: s.stationId },
      geometry: { type: "Point" as const, coordinates: [s.lon, s.lat] as [number, number] }
    }));
  return { type: "FeatureCollection" as const, features };
}

export function syncParkingStationsLayer(map: Map, stations: ParkingStation[], handlers: StationPickHandlers) {
  const data = stationFeatureCollection(stations);

  if (!map.getSource(STATIONS_SOURCE_ID)) {
    map.addSource(STATIONS_SOURCE_ID, { type: "geojson", data });
    map.addLayer({
      id: STATIONS_LAYER_ID,
      type: "circle",
      source: STATIONS_SOURCE_ID,
      paint: {
        "circle-radius": ["interpolate", ["linear"], ["zoom"], 9, 1.2, 13, 2.2, 17, 3.8],
        "circle-color": "rgba(125, 207, 255, 0.62)",
        "circle-stroke-width": 1,
        "circle-stroke-color": "rgba(9, 12, 17, 0.95)",
        "circle-opacity": 0.9
      }
    });
    map.addLayer({
      id: STATIONS_HIT_LAYER_ID,
      type: "circle",
      source: STATIONS_SOURCE_ID,
      paint: {
        "circle-radius": ["interpolate", ["linear"], ["zoom"], 9, 7, 13, 10, 17, 14],
        "circle-color": "rgba(125, 207, 255, 0.01)",
        "circle-stroke-opacity": 0,
        "circle-opacity": 0.01
      }
    });

    const onMapClick = (event: MapMouseEvent) => {
      if (!map.getLayer(STATIONS_HIT_LAYER_ID)) {
        return;
      }
      const hits = map.queryRenderedFeatures(event.point, { layers: [STATIONS_HIT_LAYER_ID] });
      if (hits.length) {
        const geometry = hits[0].geometry;
        if (geometry.type !== "Point") {
          return;
        }
        const [lng, lat] = geometry.coordinates;
        const rawName = String(hits[0].properties?.name ?? "Station");
        const stationId = String(hits[0].properties?.stationId ?? "");
        handlers.onSelectRef.current({
          lng,
          lat,
          name: stationDisplayName(rawName),
          stationId
        });
        return;
      }
      handlers.onClearRef.current();
    };

    map.on("click", onMapClick);
    map.on("mouseenter", STATIONS_HIT_LAYER_ID, () => {
      map.getCanvas().style.cursor = "pointer";
    });
    map.on("mouseleave", STATIONS_HIT_LAYER_ID, () => {
      map.getCanvas().style.cursor = "";
    });
  } else {
    (map.getSource(STATIONS_SOURCE_ID) as GeoJSONSource).setData(data);
  }
}
