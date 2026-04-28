import { Deck } from "@deck.gl/core";
import { TripsLayer } from "@deck.gl/geo-layers";
import { IconLayer } from "@deck.gl/layers";
import { CalendarDays, Pause, Play, RotateCcw, SlidersHorizontal } from "lucide-react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DAY_SECONDS, DEFAULT_FILTERS, DEFAULT_MAX_TRIPS, SPEEDS, TORONTO_VIEW_STATE } from "./constants";
import { loadManifest, TripDataClient, tripPartitionUrl } from "./dataClient";
import { syncParkingStationsLayer } from "./mapParkingStations";
import { createMapStyle, installPmtilesProtocol } from "./mapStyle";
import type { BikeCategory, BikeModel, FilterState, FlowTrip, ParkingStation, PublishedManifest, UserType } from "./types";
import "./styles.css";

type LoadState = "idle" | "loading" | "ready" | "empty" | "error";
type TripMarker = {
  trip: FlowTrip;
  position: [number, number];
  bearing: number;
};
type StationPopup = {
  type: "station";
  lng: number;
  lat: number;
  name: string;
  stationId: string;
};
type TripPopup = {
  type: "trip";
  lng: number;
  lat: number;
  trip: FlowTrip;
};
type MapPopup = StationPopup | TripPopup;

const ARROW_ICON_ATLAS =
  `data:image/svg+xml;base64,${btoa(`
<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="white" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
  <g transform="rotate(45 12 12)">
    <path d="M4.037 4.688a.495.495 0 0 1 .651-.651l16 6.5a.5.5 0 0 1-.063.947l-6.124 1.58a2 2 0 0 0-1.438 1.435l-1.579 6.126a.5.5 0 0 1-.947.063z"/>
  </g>
</svg>
`)}`;
const ARROW_ICON_MAPPING = {
  arrow: { x: 0, y: 0, width: 24, height: 24, anchorX: 12, anchorY: 12, mask: true }
};

function toggleValue<T extends string>(values: T[], value: T) {
  return values.includes(value) ? values.filter((item) => item !== value) : [...values, value];
}

function formatClock(seconds: number) {
  const normalized = Math.max(0, Math.min(DAY_SECONDS - 1, seconds));
  const hours = Math.floor(normalized / 3600);
  const minutes = Math.floor((normalized % 3600) / 60);
  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
}

function formatDuration(seconds: number) {
  const normalized = Math.max(0, Math.floor(seconds));
  const hours = Math.floor(normalized / 3600);
  const minutes = Math.floor((normalized % 3600) / 60);
  if (hours > 0) {
    return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  }
  return `${minutes}m`;
}

function formatDistance(meters: number) {
  if (!Number.isFinite(meters) || meters <= 0) {
    return "Unknown distance";
  }
  if (meters >= 1000) {
    return `${(meters / 1000).toFixed(1)} km`;
  }
  return `${Math.round(meters)} m`;
}

function formatClockWithSeconds(seconds: number) {
  const normalized = Math.max(0, Math.min(DAY_SECONDS - 1, Math.floor(seconds)));
  const hours = Math.floor(normalized / 3600);
  const minutes = Math.floor((normalized % 3600) / 60);
  const secs = normalized % 60;
  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
}

function formatClockDetailed(seconds: number) {
  const normalized = Math.max(0, Math.min(DAY_SECONDS - 1, Math.floor(seconds)));
  const hours24 = Math.floor(normalized / 3600);
  const minutes = Math.floor((normalized % 3600) / 60);
  const secs = normalized % 60;
  const meridiem = hours24 >= 12 ? "PM" : "AM";
  const hours12 = hours24 % 12 || 12;
  return {
    time: `${String(hours12).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(secs).padStart(2, "0")}`,
    meridiem
  };
}

function formatDateLabel(dateString: string) {
  if (!dateString) {
    return "Wed, Jan 1, 2025";
  }

  const [year, month, day] = dateString.split("-").map(Number);
  const date = new Date(year, (month || 1) - 1, day || 1);
  return new Intl.DateTimeFormat("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric"
  }).format(date);
}

function tripColor(trip: FlowTrip): [number, number, number] {
  if (trip.isSameStation) {
    return [201, 93, 70];
  }
  if (trip.bikeCategory === "E-bike") {
    return [47, 142, 201];
  }
  return trip.userType === "Member" ? [14, 124, 102] : [198, 162, 75];
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

function interpolatePoint(from: [number, number], to: [number, number], progress: number): [number, number] {
  return [from[0] + (to[0] - from[0]) * progress, from[1] + (to[1] - from[1]) * progress];
}

function lookAheadPoint(path: [number, number][], startIndex: number, currentPosition: [number, number], lookAheadMeters = 20) {
  let remaining = lookAheadMeters;
  let from = currentPosition;

  for (let index = startIndex + 1; index < path.length; index += 1) {
    const to = path[index];
    const segmentLength = distanceMeters(from, to);
    if (segmentLength >= remaining && segmentLength > 0) {
      return interpolatePoint(from, to, remaining / segmentLength);
    }
    remaining -= segmentLength;
    from = to;
  }

  return path[path.length - 1] ?? currentPosition;
}

function markerForTrip(trip: FlowTrip, currentTime: number): TripMarker | null {
  if (trip.path.length < 2 || trip.timestamps.length < 2 || currentTime < trip.startSeconds || currentTime > trip.endSeconds) {
    return null;
  }

  for (let index = 0; index < trip.timestamps.length - 1; index += 1) {
    const start = trip.timestamps[index];
    const end = trip.timestamps[index + 1];
    if (currentTime < start || currentTime > end) {
      continue;
    }

    const from = trip.path[index];
    const to = trip.path[index + 1];
    const progress = end === start ? 0 : (currentTime - start) / (end - start);
    const position = interpolatePoint(from, to, progress);
    const lookAhead = lookAheadPoint(trip.path, index, position);
    const dx = lookAhead[0] - position[0];
    const dy = lookAhead[1] - position[1];
    return {
      trip,
      position,
      bearing: Math.atan2(dx, dy) * (180 / Math.PI)
    };
  }

  return null;
}

function filterSummary(filters: FilterState) {
  return [
    `${filters.userTypes.length} rider ${filters.userTypes.length === 1 ? "type" : "types"}`,
    `${filters.bikeModels.length} bike ${filters.bikeModels.length === 1 ? "model" : "models"}`,
    `${filters.bikeCategories.length} ${filters.bikeCategories.length === 1 ? "class" : "classes"}`
  ].join(" · ");
}

function formatLoadState(loadState: LoadState) {
  return loadState.charAt(0).toUpperCase() + loadState.slice(1);
}

function pickedFlowTrip(object: unknown): FlowTrip | null {
  if (!object || typeof object !== "object") {
    return null;
  }
  if ("trip" in object) {
    return (object as TripMarker).trip;
  }
  if ("tripId" in object) {
    return object as FlowTrip;
  }
  return null;
}

export default function App() {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const deckRef = useRef<Deck | null>(null);
  const dataClientRef = useRef<TripDataClient | null>(null);
  const popupRef = useRef<maplibregl.Popup | null>(null);
  const parkingStationsRef = useRef<ParkingStation[]>([]);
  const tripClickHandledRef = useRef(false);
  const stationSelectHandlerRef = useRef<(payload: Omit<StationPopup, "type">) => void>(() => {});
  const stationClearHandlerRef = useRef<() => void>(() => {});
  const animationRef = useRef<number | null>(null);
  const lastFrameRef = useRef<number | null>(null);

  const [manifest, setManifest] = useState<PublishedManifest | null>(null);
  const [selectedDate, setSelectedDate] = useState("");
  const [filters, setFilters] = useState<FilterState>(DEFAULT_FILTERS);
  const [trips, setTrips] = useState<FlowTrip[]>([]);
  const [loadState, setLoadState] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState("");
  const [currentTime, setCurrentTime] = useState(8 * 3600);
  const [isPlaying, setIsPlaying] = useState(false);
  const [speed, setSpeed] = useState(30);
  const [isFiltersOpen, setIsFiltersOpen] = useState(false);
  const [parkingStations, setParkingStations] = useState<ParkingStation[]>([]);
  const [mapPopup, setMapPopup] = useState<MapPopup | null>(null);

  stationSelectHandlerRef.current = (payload) => {
    if (!tripClickHandledRef.current) {
      setMapPopup({ type: "station", ...payload });
    }
  };
  stationClearHandlerRef.current = () => {
    if (!tripClickHandledRef.current) {
      setMapPopup(null);
    }
  };
  parkingStationsRef.current = parkingStations;

  const availableDates = manifest?.dates ?? [];
  const detailedClock = useMemo(() => formatClockDetailed(currentTime), [currentTime]);
  const formattedDateLabel = useMemo(() => formatDateLabel(selectedDate), [selectedDate]);
  const timelineClock = useMemo(() => formatClockWithSeconds(currentTime), [currentTime]);

  const activeTrips = useMemo(
    () => trips.filter((trip) => trip.startSeconds <= currentTime && trip.endSeconds >= currentTime - 1800),
    [currentTime, trips]
  );
  const currentTripMarkers = useMemo(
    () => activeTrips.flatMap((trip) => {
      const marker = markerForTrip(trip, currentTime);
      return marker ? [marker] : [];
    }),
    [activeTrips, currentTime]
  );

  useEffect(() => {
    dataClientRef.current = new TripDataClient();
    return () => dataClientRef.current?.dispose();
  }, []);

  useEffect(() => {
    if (!manifest || !dataClientRef.current) {
      return;
    }
    let cancelled = false;
    setParkingStations([]);
    void dataClientRef.current
      .loadStations(manifest.assets.stationsUrl)
      .then((stations) => {
        if (!cancelled) {
          setParkingStations(stations);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setParkingStations([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [manifest]);

  useEffect(() => {
    let cancelled = false;
    setLoadState("loading");
    loadManifest()
      .then((loadedManifest) => {
        if (cancelled) {
          return;
        }
        setManifest(loadedManifest);
        setSelectedDate(loadedManifest.dates[0] ?? "");
        setLoadState("idle");
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setErrorMessage(error instanceof Error ? error.message : "Could not load manifest");
        setLoadState("error");
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!manifest || !mapContainerRef.current) {
      return;
    }

    installPmtilesProtocol(maplibregl);

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: createMapStyle(manifest.assets.basemapUrl),
      center: [TORONTO_VIEW_STATE.longitude, TORONTO_VIEW_STATE.latitude],
      zoom: TORONTO_VIEW_STATE.zoom,
      pitch: TORONTO_VIEW_STATE.pitch,
      bearing: TORONTO_VIEW_STATE.bearing,
      attributionControl: false
    });

    map.dragRotate.disable();
    map.touchZoomRotate.disableRotation();
    map.keyboard.disableRotation();

    map.addControl(
      new maplibregl.AttributionControl({
        compact: true,
        customAttribution: [
          'Bike Share Toronto data licensed under the <a href="https://open.toronto.ca/open-data-licence/" target="_blank" rel="noreferrer">Open Government Licence - Toronto</a>'
        ]
      }),
      "bottom-right"
    );
    map.addControl(new maplibregl.NavigationControl({ showCompass: false, visualizePitch: false }), "bottom-right");

    const deck = new Deck({
      parent: mapContainerRef.current,
      controller: false,
      // Deck canvas is stacked above the map; without this it captures all drags
      // and MapLibre never receives pan / touch-zoom gestures.
      style: { pointerEvents: "none" },
      initialViewState: TORONTO_VIEW_STATE,
      layers: []
    });
    deckRef.current = deck;

    const syncDeck = () => {
      const center = map.getCenter();
      deck.setProps({
        viewState: {
          longitude: center.lng,
          latitude: center.lat,
          zoom: map.getZoom(),
          pitch: map.getPitch(),
          bearing: map.getBearing()
        }
      });
    };

    map.on("move", syncDeck);
    map.on("resize", syncDeck);
    syncDeck();

    const handleTripClick = (event: maplibregl.MapMouseEvent) => {
      const pick = deck.pickObject({
        x: event.point.x,
        y: event.point.y,
        radius: 10,
        layerIds: ["bike-share-trip-arrows", "bike-share-trips"]
      });
      const trip = pickedFlowTrip(pick?.object);
      if (!trip) {
        tripClickHandledRef.current = false;
        setMapPopup(null);
        return;
      }

      const lngLat = map.unproject(event.point);
      tripClickHandledRef.current = true;
      setMapPopup({
        type: "trip",
        lng: lngLat.lng,
        lat: lngLat.lat,
        trip
      });
    };

    map.on("click", handleTripClick);

    mapRef.current = map;

    const syncStationsToMap = () => {
      syncParkingStationsLayer(map, parkingStationsRef.current, {
        onSelectRef: stationSelectHandlerRef,
        onClearRef: stationClearHandlerRef
      });
    };

    if (map.isStyleLoaded()) {
      syncStationsToMap();
    } else {
      map.once("load", syncStationsToMap);
    }

    return () => {
      setMapPopup(null);
      popupRef.current?.remove();
      popupRef.current = null;
      mapRef.current = null;
      map.off("click", handleTripClick);
      map.remove();
      deck.finalize();
      deckRef.current = null;
    };
  }, [manifest]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) {
      return;
    }
    const run = () => {
      syncParkingStationsLayer(map, parkingStations, {
        onSelectRef: stationSelectHandlerRef,
        onClearRef: stationClearHandlerRef
      });
    };
    if (map.isStyleLoaded()) {
      run();
    } else {
      map.once("load", run);
    }
  }, [parkingStations]);

  useEffect(() => {
    const map = mapRef.current;
    if (!mapPopup || !map) {
      popupRef.current?.remove();
      popupRef.current = null;
      return;
    }

    popupRef.current?.remove();
    const content = document.createElement("div");
    content.className = "station-popup-inner";

    if (mapPopup.type === "trip") {
      const eyebrow = document.createElement("div");
      eyebrow.className = "station-popup-eyebrow";
      eyebrow.textContent = "Trip details";
      content.appendChild(eyebrow);
    }

    if (mapPopup.type === "station") {
      const title = document.createElement("div");
      title.className = "station-popup-title";
      title.textContent = mapPopup.name;
      content.appendChild(title);
    }

    if (mapPopup.type === "trip") {
      const trip = mapPopup.trip;
      const route = document.createElement("div");
      route.className = "trip-route";

      const stops = [
        [formatClock(trip.startSeconds), trip.startStationName],
        [formatClock(trip.endSeconds), trip.endStationName]
      ];

      for (const [time, station] of stops) {
        const row = document.createElement("div");
        row.className = "trip-route-row";

        const timeNode = document.createElement("span");
        timeNode.className = "trip-route-time";
        timeNode.textContent = time;

        const dot = document.createElement("span");
        dot.className = "trip-route-dot";
        dot.setAttribute("aria-hidden", "true");

        const stationNode = document.createElement("strong");
        stationNode.className = "trip-route-station";
        stationNode.textContent = station;

        row.append(timeNode, dot, stationNode);
        route.appendChild(row);
      }

      const meta = document.createElement("div");
      meta.className = "trip-popup-meta-row";
      meta.textContent = `${formatDuration(trip.durationSeconds)} · ${formatDistance(trip.distanceMeters)} · ${trip.bikeCategory} · ${trip.userType}`;

      content.append(route, meta);
    }

    const popup = new maplibregl.Popup({
      closeButton: true,
      closeOnClick: true,
      className: "station-popup",
      offset: 14,
      maxWidth: "320px"
    })
      .setLngLat([mapPopup.lng, mapPopup.lat])
      .setDOMContent(content)
      .addTo(map);

    popupRef.current = popup;
    return () => {
      popup.remove();
      if (popupRef.current === popup) {
        popupRef.current = null;
      }
    };
  }, [mapPopup]);

  useEffect(() => {
    if (!deckRef.current) {
      return;
    }

    const layer = new TripsLayer<FlowTrip>({
      id: "bike-share-trips",
      data: activeTrips,
      getPath: (trip) => trip.path,
      getTimestamps: (trip) => trip.timestamps,
      getColor: tripColor,
      opacity: 0.35,
      widthMinPixels: 2,
      widthMaxPixels: 5,
      trailLength: 90,
      currentTime,
      capRounded: true,
      jointRounded: true,
      pickable: true
    });
    const arrowLayer = new IconLayer<TripMarker>({
      id: "bike-share-trip-arrows",
      data: currentTripMarkers,
      iconAtlas: ARROW_ICON_ATLAS,
      iconMapping: ARROW_ICON_MAPPING,
      getIcon: () => "arrow",
      getPosition: (marker) => marker.position,
      getAngle: (marker) => -marker.bearing,
      getColor: (marker) => tripColor(marker.trip),
      getSize: 9,
      sizeUnits: "pixels",
      billboard: false,
      pickable: true
    });

    deckRef.current.setProps({ layers: [layer, arrowLayer] });
  }, [activeTrips, currentTime, currentTripMarkers]);

  const loadTripsForSelection = useCallback(async () => {
    if (!manifest || !selectedDate || !dataClientRef.current) {
      return;
    }

    if (!filters.userTypes.length || !filters.bikeModels.length || !filters.bikeCategories.length) {
      setTrips([]);
      setLoadState("empty");
      return;
    }

    setLoadState("loading");
    setErrorMessage("");

    const response = await dataClientRef.current.loadTrips({
      date: selectedDate,
      tripsUrl: tripPartitionUrl(manifest, selectedDate),
      routesUrl: manifest.assets.routesUrl,
      filters,
      maxTrips: DEFAULT_MAX_TRIPS
    });

    if (response.type === "loadError") {
      setTrips([]);
      setErrorMessage(response.message);
      setLoadState("error");
      return;
    }

    setTrips(response.trips);
    setLoadState(response.trips.length ? "ready" : "empty");
  }, [filters, manifest, selectedDate]);

  useEffect(() => {
    void loadTripsForSelection();
  }, [loadTripsForSelection]);

  useEffect(() => {
    if (!isPlaying) {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
        animationRef.current = null;
      }
      lastFrameRef.current = null;
      return;
    }

    const tick = (frameTime: number) => {
      if (lastFrameRef.current === null) {
        lastFrameRef.current = frameTime;
      }
      const delta = (frameTime - lastFrameRef.current) / 1000;
      lastFrameRef.current = frameTime;
      setCurrentTime((value) => (value + delta * speed) % DAY_SECONDS);
      animationRef.current = requestAnimationFrame(tick);
    };

    animationRef.current = requestAnimationFrame(tick);
    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
      animationRef.current = null;
      lastFrameRef.current = null;
    };
  }, [isPlaying, speed]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target && ["INPUT", "SELECT", "TEXTAREA"].includes(target.tagName)) {
        return;
      }
      if (event.code === "Space") {
        event.preventDefault();
        setIsPlaying((value) => !value);
      }
      if (event.key.toLowerCase() === "r") {
        setCurrentTime(0);
      }
      if (event.key.toLowerCase() === "f") {
        setIsFiltersOpen((value) => !value);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  const setUserType = (userType: UserType) => setFilters((value) => ({ ...value, userTypes: toggleValue(value.userTypes, userType) }));
  const setBikeModel = (bikeModel: BikeModel) => setFilters((value) => ({ ...value, bikeModels: toggleValue(value.bikeModels, bikeModel) }));
  const setBikeCategory = (bikeCategory: BikeCategory) =>
    setFilters((value) => ({ ...value, bikeCategories: toggleValue(value.bikeCategories, bikeCategory) }));

  return (
    <main className="app-shell">
      <div ref={mapContainerRef} className="map" />

      <aside className="action-rail" aria-label="Bike Share flow controls">
        <div className="rail-meta">
          <span className="rail-brand">Toronto Bike Share</span>
          <span className={`status-pill status-${loadState}`}>{formatLoadState(loadState)}</span>
        </div>

        <label className="rail-control rail-select">
          <span className="rail-main">
            <CalendarDays size={18} />
            <select value={selectedDate} onChange={(event) => setSelectedDate(event.target.value)} disabled={!availableDates.length}>
              {availableDates.map((date) => (
                <option key={date} value={date}>
                  {date}
                </option>
              ))}
            </select>
          </span>
        </label>

        <button type="button" className="rail-control rail-control-primary" onClick={() => setIsPlaying((value) => !value)}>
          <span className="rail-main">
            {isPlaying ? <Pause size={18} /> : <Play size={18} />}
            {isPlaying ? "Pause" : "Play"}
          </span>
          <kbd>Space</kbd>
        </button>

        <button type="button" className="rail-control" onClick={() => setCurrentTime(0)}>
          <span className="rail-main">
            <RotateCcw size={17} />
            Reset
          </span>
          <kbd>R</kbd>
        </button>

        <div className="rail-row">
          <label className="rail-control rail-control-compact rail-select">
            <span className="rail-main">
              <SlidersHorizontal size={17} />
              <select value={speed} onChange={(event) => setSpeed(Number(event.target.value))}>
                {SPEEDS.map((value) => (
                  <option key={value} value={value}>
                    {value}x
                  </option>
                ))}
              </select>
            </span>
          </label>

          <button type="button" className="rail-control rail-control-compact" onClick={() => setIsFiltersOpen((value) => !value)}>
            <span className="rail-main">
              <SlidersHorizontal size={17} />
              Filters
            </span>
            <kbd>F</kbd>
          </button>
        </div>

        {isFiltersOpen && (
          <div className="filter-drawer">
            <div className="filter-drawer-header">
              <h2>Filters</h2>
            </div>

            <div className="filter-block">
              <h3>Rider type</h3>
              <div className="chip-row">
                {(["Member", "Casual"] as UserType[]).map((value) => (
                  <button
                    key={value}
                    type="button"
                    className={filters.userTypes.includes(value) ? "chip active" : "chip"}
                    onClick={() => setUserType(value)}
                  >
                    {value}
                  </button>
                ))}
              </div>
            </div>

            <div className="filter-block">
              <h3>Bike model</h3>
              <div className="chip-row">
                {(["ICONIC", "EFIT", "ASTRO"] as BikeModel[]).map((value) => (
                  <button
                    key={value}
                    type="button"
                    className={filters.bikeModels.includes(value) ? "chip active" : "chip"}
                    onClick={() => setBikeModel(value)}
                  >
                    {value}
                  </button>
                ))}
              </div>
            </div>

            <div className="filter-block">
              <h3>Bike class</h3>
              <div className="chip-row">
                {(["Classic", "E-bike"] as BikeCategory[]).map((value) => (
                  <button
                    key={value}
                    type="button"
                    className={filters.bikeCategories.includes(value) ? "chip active" : "chip"}
                    onClick={() => setBikeCategory(value)}
                  >
                    {value}
                  </button>
                ))}
              </div>
            </div>
          </div>
        )}
      </aside>

      <section className="time-hud" aria-label="Current playback time">
        <span className="time-date">{formattedDateLabel}</span>
        <div className="time-readout">
          <strong>{detailedClock.time}</strong>
          <span className="time-meridiem">{detailedClock.meridiem}</span>
        </div>
      </section>

      <section className="timeline-dock" aria-label="Playback timeline">
        <input
          type="range"
          min={0}
          max={DAY_SECONDS - 1}
          step={60}
          value={Math.floor(currentTime)}
          onChange={(event) => setCurrentTime(Number(event.target.value))}
        />
        <div className="timeline-meta-line">
          <span className="timeline-inline-time">{timelineClock}</span>
          <span className="timeline-inline-separator" aria-hidden="true">
            ·
          </span>
          <span className="timeline-inline-live">
            {currentTripMarkers.length.toLocaleString()} live trip{currentTripMarkers.length === 1 ? "" : "s"}
          </span>
        </div>
      </section>

      {loadState === "error" && (
        <p className="error-toast" role="alert">
          {errorMessage}
        </p>
      )}
    </main>
  );
}
