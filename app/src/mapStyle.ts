import { Protocol } from "pmtiles";
import type { StyleSpecification } from "maplibre-gl";

let protocolInstalled = false;

export function installPmtilesProtocol(maplibregl: typeof import("maplibre-gl")) {
  if (protocolInstalled) {
    return;
  }

  const protocol = new Protocol();
  maplibregl.addProtocol("pmtiles", protocol.tile);
  protocolInstalled = true;
}

export function createMapStyle(basemapUrl: string): StyleSpecification | string {
  if (!basemapUrl) {
    return "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";
  }

  if (basemapUrl.endsWith(".json")) {
    return basemapUrl;
  }

  return {
    version: 8,
    glyphs: "https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf",
    sources: {
      protomaps: {
        type: "vector",
        url: `pmtiles://${basemapUrl}`,
        attribution: "© OpenStreetMap contributors © Protomaps"
      }
    },
    layers: [
      { id: "background", type: "background", paint: { "background-color": "#0a0c10" } },
      { id: "water", type: "fill", source: "protomaps", "source-layer": "water", paint: { "fill-color": "#11161d" } },
      { id: "parks", type: "fill", source: "protomaps", "source-layer": "landuse", paint: { "fill-color": "#0f1418", "fill-opacity": 0.72 } },
      {
        id: "roads-minor",
        type: "line",
        source: "protomaps",
        "source-layer": "roads",
        filter: ["in", ["get", "kind"], ["literal", ["minor_road", "path", "other"]]],
        paint: { "line-color": "#242932", "line-width": ["interpolate", ["linear"], ["zoom"], 10, 0.2, 15, 1.2] }
      },
      {
        id: "roads-major",
        type: "line",
        source: "protomaps",
        "source-layer": "roads",
        filter: ["in", ["get", "kind"], ["literal", ["major_road", "highway"]]],
        paint: { "line-color": "#343b46", "line-width": ["interpolate", ["linear"], ["zoom"], 10, 0.8, 15, 2.8] }
      },
      {
        id: "places",
        type: "symbol",
        source: "protomaps",
        "source-layer": "places",
        layout: {
          "text-field": ["get", "name"],
          "text-font": ["Noto Sans Regular"],
          "text-size": ["interpolate", ["linear"], ["zoom"], 9, 10, 14, 14]
        },
        paint: {
          "text-color": "#6b7480",
          "text-halo-color": "#0a0c10",
          "text-halo-width": 1
        }
      }
    ]
  };
}
