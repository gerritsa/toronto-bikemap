import { Deck } from "@deck.gl/core";
import { TripsLayer } from "@deck.gl/geo-layers";
import { IconLayer, PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { Bike, CalendarDays, Pause, Play, Search, SlidersHorizontal, X } from "lucide-react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DAY_SECONDS, DEFAULT_FILTERS, DEFAULT_MAX_TRIPS, SPEEDS, TORONTO_VIEW_STATE } from "./constants";
import { loadManifest, TripDataClient, tripPartitionUrl } from "./dataClient";
import { syncParkingStationsLayer } from "./mapParkingStations";
import { createMapStyle, installPmtilesProtocol } from "./mapStyle";
import type { BikeCategory, FilterState, FlowTrip, ParkingStation, PublishedManifest, UserType } from "./types";
import "./styles.css";

type LoadState = "idle" | "loading" | "ready" | "empty" | "error";
type TripMarker = {
  trip: FlowTrip;
  position: [number, number];
  bearing: number;
};
type FinishBurst = {
  id: string;
  position: [number, number];
  color: [number, number, number];
  startedAt: number;
};
type FinishPulse = {
  id: string;
  position: [number, number];
  color: [number, number, number, number];
  radius: number;
};
type StationPopup = {
  type: "station";
  lng: number;
  lat: number;
  name: string;
  stationId: string;
};
type MapPopup = StationPopup;
type SearchMode = "time" | "ride";
type RideSearchMatch = {
  trip: FlowTrip;
  score: number;
};

const TRIP_TRAIL_LENGTH_SECONDS = 180;
const FINISH_BURST_MS = 1100;
const DEFAULT_SELECTED_DATE = "2026-01-01";
const DEFAULT_START_SECONDS = 0;
const SELECTED_ROUTE_COLOR: [number, number, number, number] = [232, 190, 118, 220];

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

function formatClockShort(seconds: number) {
  const normalized = Math.max(0, Math.min(DAY_SECONDS - 1, Math.floor(seconds)));
  const hours24 = Math.floor(normalized / 3600);
  const minutes = Math.floor((normalized % 3600) / 60);
  const meridiem = hours24 >= 12 ? "PM" : "AM";
  const hours12 = hours24 % 12 || 12;
  return `${hours12}:${String(minutes).padStart(2, "0")} ${meridiem}`;
}

function formatClockCompact(seconds: number) {
  const normalized = Math.max(0, Math.min(DAY_SECONDS - 1, Math.floor(seconds)));
  const hours24 = Math.floor(normalized / 3600);
  const minutes = Math.floor((normalized % 3600) / 60);
  const meridiem = hours24 >= 12 ? "PM" : "AM";
  const hours12 = hours24 % 12 || 12;
  return `${hours12}:${String(minutes).padStart(2, "0")}${meridiem.toLowerCase()}`;
}

function formatDateLabel(dateString: string) {
  if (!dateString) {
    return formatDateLabel(DEFAULT_SELECTED_DATE);
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
  if (trip.bikeCategory === "E-bike") {
    return [47, 142, 201];
  }
  return [14, 124, 102];
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

function formatLoadState(loadState: LoadState) {
  return loadState.charAt(0).toUpperCase() + loadState.slice(1);
}

function formatBikeLabel(trip: FlowTrip) {
  return trip.bikeCategory === "E-bike" ? "Electric Bike" : "Classic Bike";
}

function formatAverageSpeed(trip: FlowTrip) {
  if (!Number.isFinite(trip.distanceMeters) || !Number.isFinite(trip.durationSeconds) || trip.durationSeconds <= 0) {
    return "Unknown speed";
  }
  const kilometersPerHour = trip.distanceMeters / 1000 / (trip.durationSeconds / 3600);
  return `${kilometersPerHour.toFixed(1)} km/h`;
}

function formatTripCode(trip: FlowTrip) {
  return trip.tripId.slice(-8).toUpperCase();
}

function normalizeSearchText(value: string) {
  return value
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function parseSearchDate(query: string, availableDates: string[]) {
  const isoDate = query.match(/\b20\d{2}-\d{2}-\d{2}\b/)?.[0];
  if (isoDate && availableDates.includes(isoDate)) {
    return isoDate;
  }
  return null;
}

function parseSearchTime(query: string) {
  const match = query.match(/\b(\d{1,2})(?::(\d{2}))?(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?)?\b/i);
  if (!match) {
    return null;
  }

  let hours = Number(match[1]);
  const minutes = Number(match[2] ?? 0);
  const seconds = Number(match[3] ?? 0);
  const meridiem = match[4]?.toLowerCase().replaceAll(".", "");

  if (!Number.isInteger(hours) || !Number.isInteger(minutes) || !Number.isInteger(seconds) || minutes > 59 || seconds > 59) {
    return null;
  }
  if (meridiem) {
    if (hours < 1 || hours > 12) {
      return null;
    }
    hours = hours % 12;
    if (meridiem === "pm") {
      hours += 12;
    }
  } else if (hours > 23) {
    return null;
  }

  return hours * 3600 + minutes * 60 + seconds;
}

function stationSearchTokens(query: string) {
  const ignored = new Set(["am", "pm", "at", "to", "from", "ride", "trip", "start", "started", "when", "did", "this"]);
  return normalizeSearchText(query)
    .split(" ")
    .filter((token) => token.length > 1 && !ignored.has(token) && !/^\d+$/.test(token));
}

function tokenHits(tokens: string[], text: string) {
  const normalized = normalizeSearchText(text);
  return tokens.reduce((score, token) => score + (normalized.includes(token) ? 1 : 0), 0);
}

function rideSearchMatches(query: string, trips: FlowTrip[], currentTime: number): RideSearchMatch[] {
  const tokens = stationSearchTokens(query);
  const searchedTime = parseSearchTime(query);
  if (!tokens.length && searchedTime === null) {
    return [];
  }

  return trips
    .map((trip) => {
      const stationScore = tokenHits(tokens, `${trip.startStationName} ${trip.endStationName}`);
      const timeDistance = Math.abs((searchedTime ?? currentTime) - trip.startSeconds);
      const timeScore = searchedTime === null ? 0 : Math.max(0, 4 - timeDistance / 900);
      return {
        trip,
        score: stationScore * 2 + timeScore
      };
    })
    .filter((match) => match.score > 0)
    .sort((a, b) => b.score - a.score || Math.abs(a.trip.startSeconds - currentTime) - Math.abs(b.trip.startSeconds - currentTime))
    .slice(0, 6);
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

function focusTripPath(map: maplibregl.Map, trip: FlowTrip) {
  if (trip.path.length < 2) {
    return;
  }

  const bounds = trip.path.reduce(
    (tripBounds, coordinate) => tripBounds.extend(coordinate),
    new maplibregl.LngLatBounds(trip.path[0], trip.path[0])
  );
  const width = map.getCanvas().clientWidth;
  const height = map.getCanvas().clientHeight;
  const compact = width < 760 || height < 620;

  map.fitBounds(bounds, {
    duration: 900,
    maxZoom: 15.4,
    padding: compact
      ? { top: 132, right: 36, bottom: 160, left: 36 }
      : { top: 140, right: 520, bottom: 112, left: 280 }
  });
}

function resetMapView(map: maplibregl.Map | null) {
  map?.easeTo({
    center: [TORONTO_VIEW_STATE.longitude, TORONTO_VIEW_STATE.latitude],
    zoom: TORONTO_VIEW_STATE.zoom,
    pitch: TORONTO_VIEW_STATE.pitch,
    bearing: TORONTO_VIEW_STATE.bearing,
    duration: 900
  });
}

function createFinishPulses(bursts: FinishBurst[]): FinishPulse[] {
  const now = performance.now();
  const pulses: FinishPulse[] = [];

  for (const burst of bursts) {
    const rawProgress = (now - burst.startedAt) / FINISH_BURST_MS;
    if (rawProgress < 0 || rawProgress > 1) {
      continue;
    }

    const progress = 1 - (1 - rawProgress) ** 2;
    pulses.push({
      id: burst.id,
      position: burst.position,
      color: [burst.color[0], burst.color[1], burst.color[2], Math.round(120 * (1 - rawProgress))],
      radius: 3 + 15 * progress
    });
  }

  return pulses;
}

export default function App() {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const deckRef = useRef<Deck | null>(null);
  const dataClientRef = useRef<TripDataClient | null>(null);
  const popupRef = useRef<maplibregl.Popup | null>(null);
  const parkingStationsRef = useRef<ParkingStation[]>([]);
  const searchInputRef = useRef<HTMLInputElement | null>(null);
  const tripClickHandledRef = useRef(false);
  const stationSelectHandlerRef = useRef<(payload: Omit<StationPopup, "type">) => void>(() => {});
  const stationClearHandlerRef = useRef<() => void>(() => {});
  const animationRef = useRef<number | null>(null);
  const lastFrameRef = useRef<number | null>(null);
  const previousTimeRef = useRef(DEFAULT_START_SECONDS);

  const [manifest, setManifest] = useState<PublishedManifest | null>(null);
  const [selectedDate, setSelectedDate] = useState(DEFAULT_SELECTED_DATE);
  const [filters, setFilters] = useState<FilterState>(DEFAULT_FILTERS);
  const [trips, setTrips] = useState<FlowTrip[]>([]);
  const [loadState, setLoadState] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState("");
  const [currentTime, setCurrentTime] = useState(DEFAULT_START_SECONDS);
  const [isPlaying, setIsPlaying] = useState(false);
  const [speed, setSpeed] = useState(30);
  const [isFiltersOpen, setIsFiltersOpen] = useState(false);
  const [parkingStations, setParkingStations] = useState<ParkingStation[]>([]);
  const [mapPopup, setMapPopup] = useState<MapPopup | null>(null);
  const [selectedTrip, setSelectedTrip] = useState<FlowTrip | null>(null);
  const [finishBursts, setFinishBursts] = useState<FinishBurst[]>([]);
  const [isSearchOpen, setIsSearchOpen] = useState(false);
  const [searchMode, setSearchMode] = useState<SearchMode>("time");
  const [searchQuery, setSearchQuery] = useState("");

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
  const detailedClock = useMemo(() => formatClockWithSeconds(currentTime), [currentTime]);
  const formattedDateLabel = useMemo(() => formatDateLabel(selectedDate), [selectedDate]);
  const timelineClock = useMemo(() => formatClockWithSeconds(currentTime), [currentTime]);
  const parsedSearchTime = useMemo(() => parseSearchTime(searchQuery), [searchQuery]);
  const parsedSearchDate = useMemo(() => parseSearchDate(searchQuery, availableDates), [availableDates, searchQuery]);
  const rideMatches = useMemo(() => rideSearchMatches(searchQuery, trips, currentTime), [currentTime, searchQuery, trips]);

  const activeTrips = useMemo(
    () => trips.filter((trip) => trip.startSeconds <= currentTime && trip.endSeconds >= currentTime - TRIP_TRAIL_LENGTH_SECONDS),
    [currentTime, trips]
  );
  const currentTripMarkers = useMemo(
    () => activeTrips.flatMap((trip) => {
      const marker = markerForTrip(trip, currentTime);
      return marker ? [marker] : [];
    }),
    [activeTrips, currentTime]
  );
  const finishPulses = useMemo(() => createFinishPulses(finishBursts), [currentTime, finishBursts]);

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
        layerIds: ["bike-share-trip-arrows", "bike-share-selected-trip", "bike-share-trips"]
      });
      const trip = pickedFlowTrip(pick?.object);
      if (!trip) {
        tripClickHandledRef.current = false;
        setSelectedTrip((selected) => {
          if (selected) {
            resetMapView(map);
          }
          return null;
        });
        setMapPopup(null);
        return;
      }

      tripClickHandledRef.current = true;
      setMapPopup(null);
      setSelectedTrip(trip);
      focusTripPath(map, trip);
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

    const title = document.createElement("div");
    title.className = "station-popup-title";
    title.textContent = mapPopup.name;
    content.appendChild(title);

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
      trailLength: TRIP_TRAIL_LENGTH_SECONDS,
      currentTime,
      capRounded: true,
      jointRounded: true,
      pickable: true
    });
    const selectedTripLayer = new PathLayer<FlowTrip>({
      id: "bike-share-selected-trip",
      data: selectedTrip ? [selectedTrip] : [],
      getPath: (trip) => trip.path,
      getColor: () => SELECTED_ROUTE_COLOR,
      opacity: 0.92,
      widthMinPixels: 3,
      widthMaxPixels: 4,
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
    const finishBurstLayer = new ScatterplotLayer<FinishPulse>({
      id: "bike-share-finish-bursts",
      data: finishPulses,
      getPosition: (pulse) => pulse.position,
      getFillColor: (pulse) => pulse.color,
      getLineColor: (pulse) => pulse.color,
      getRadius: (pulse) => pulse.radius,
      getLineWidth: 1.6,
      radiusUnits: "pixels",
      radiusMinPixels: 1,
      radiusMaxPixels: 20,
      lineWidthUnits: "pixels",
      stroked: true,
      filled: false,
      pickable: false
    });

    deckRef.current.setProps({ layers: [layer, selectedTripLayer, arrowLayer, finishBurstLayer] });
  }, [activeTrips, currentTime, currentTripMarkers, finishPulses, selectedTrip]);

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
    setSelectedTrip((trip) => {
      if (!trip) {
        return null;
      }
      return trips.some((candidate) => candidate.tripId === trip.tripId) ? trip : null;
    });
  }, [trips]);

  useEffect(() => {
    setFinishBursts([]);
    previousTimeRef.current = currentTime;
  }, [filters, selectedDate]);

  useEffect(() => {
    const previousTime = previousTimeRef.current;
    previousTimeRef.current = currentTime;

    if (!isPlaying || currentTime <= previousTime || currentTime - previousTime > 600) {
      return;
    }

    const now = performance.now();
    const completedTrips = trips
      .filter((trip) => trip.endSeconds > previousTime && trip.endSeconds <= currentTime && trip.path.length)
      .slice(0, 80);

    if (!completedTrips.length) {
      return;
    }

    setFinishBursts((bursts) => [
      ...bursts.filter((burst) => now - burst.startedAt < FINISH_BURST_MS),
      ...completedTrips.map((trip) => ({
        id: `${trip.tripId}-${now}`,
        position: trip.path[trip.path.length - 1],
        color: tripColor(trip),
        startedAt: now
      }))
    ]);
  }, [currentTime, isPlaying, trips]);

  useEffect(() => {
    if (!finishBursts.length) {
      return;
    }

    const timeout = window.setTimeout(() => {
      const now = performance.now();
      setFinishBursts((bursts) => bursts.filter((burst) => now - burst.startedAt < FINISH_BURST_MS));
    }, FINISH_BURST_MS);

    return () => window.clearTimeout(timeout);
  }, [finishBursts]);

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
    if (!isSearchOpen) {
      return;
    }
    window.setTimeout(() => searchInputRef.current?.focus(), 0);
  }, [isSearchOpen]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setIsSearchOpen((value) => !value);
        return;
      }
      if (event.key === "Escape" && isSearchOpen) {
        event.preventDefault();
        setIsSearchOpen(false);
        return;
      }
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
      if (event.key === "Escape") {
        setSelectedTrip((trip) => {
          if (trip) {
            resetMapView(mapRef.current);
          }
          return null;
        });
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [isSearchOpen]);

  const setUserType = (userType: UserType) => setFilters((value) => ({ ...value, userTypes: toggleValue(value.userTypes, userType) }));
  const setBikeCategory = (bikeCategory: BikeCategory) =>
    setFilters((value) => ({ ...value, bikeCategories: toggleValue(value.bikeCategories, bikeCategory) }));
  const deselectSelectedTrip = useCallback(() => {
    setSelectedTrip(null);
    resetMapView(mapRef.current);
  }, []);
  const openSearch = useCallback((mode: SearchMode = "time") => {
    setSearchMode(mode);
    setIsSearchOpen(true);
  }, []);
  const applyTimeSearch = useCallback(() => {
    if (parsedSearchTime === null) {
      return;
    }
    if (parsedSearchDate) {
      setSelectedDate(parsedSearchDate);
    }
    setCurrentTime(parsedSearchTime);
    setIsPlaying(false);
    setIsSearchOpen(false);
  }, [parsedSearchDate, parsedSearchTime]);
  const selectSearchTrip = useCallback((trip: FlowTrip) => {
    setCurrentTime(trip.startSeconds);
    setIsPlaying(false);
    setMapPopup(null);
    setSelectedTrip(trip);
    if (mapRef.current) {
      focusTripPath(mapRef.current, trip);
    }
    setIsSearchOpen(false);
  }, []);

  return (
    <main className={selectedTrip ? "app-shell selected-mode" : "app-shell"}>
      <div ref={mapContainerRef} className="map" />

      <aside className="action-rail" aria-label="Bike Share flow controls">
        <div className="rail-meta">
          <span className="rail-brand">Toronto Bike Share</span>
          <span className={`status-pill status-${loadState}`}>{formatLoadState(loadState)}</span>
        </div>

        <button type="button" className="rail-control rail-search" onClick={() => openSearch("ride")}>
          <span className="rail-main">
            <Search size={18} />
            Find ride
          </span>
          <kbd>⌘K</kbd>
        </button>

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

      <section className="timeline-dock" aria-label="Playback timeline">
        <div className="timeline-header">
          <div>
            <span className="time-date">{formattedDateLabel}</span>
            <strong>{detailedClock}</strong>
          </div>
          <span className="timeline-live-pill">
            {currentTripMarkers.length.toLocaleString()} live
          </span>
        </div>
        <input
          type="range"
          min={0}
          max={DAY_SECONDS - 1}
          step={60}
          value={Math.floor(currentTime)}
          onChange={(event) => setCurrentTime(Number(event.target.value))}
        />
        <div className="timeline-scale" aria-hidden="true">
          <span>00:00</span>
          <span>{timelineClock}</span>
          <span>23:59</span>
        </div>
      </section>

      {isSearchOpen && (
        <div className="command-backdrop" role="presentation" onMouseDown={() => setIsSearchOpen(false)}>
          <section className="command-panel" aria-label="Search rides and time" onMouseDown={(event) => event.stopPropagation()}>
            <div className="command-tabs">
              <button
                type="button"
                className={searchMode === "time" ? "command-tab active" : "command-tab"}
                onClick={() => setSearchMode("time")}
              >
                Search time/date
              </button>
              <button
                type="button"
                className={searchMode === "ride" ? "command-tab active" : "command-tab"}
                onClick={() => setSearchMode("ride")}
              >
                Find ride
              </button>
              <button type="button" className="command-close" aria-label="Close search" onClick={() => setIsSearchOpen(false)}>
                <X size={20} aria-hidden="true" />
              </button>
            </div>

            <label className="command-input-row">
              <Search size={24} aria-hidden="true" />
              <input
                ref={searchInputRef}
                value={searchQuery}
                placeholder={searchMode === "time" ? "Try 8:20am or 2026-01-01 4pm" : "Try start station, end station, or 8:05am"}
                onChange={(event) => setSearchQuery(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Tab") {
                    event.preventDefault();
                    setSearchMode((value) => (value === "time" ? "ride" : "time"));
                  }
                  if (event.key === "Enter" && searchMode === "time") {
                    applyTimeSearch();
                  }
                  if (event.key === "Enter" && searchMode === "ride" && rideMatches[0]) {
                    selectSearchTrip(rideMatches[0].trip);
                  }
                }}
              />
            </label>

            <div className="command-results">
              {searchMode === "time" ? (
                <>
                  <button type="button" className="command-primary-result" disabled={parsedSearchTime === null} onClick={applyTimeSearch}>
                    <span>Jump to time</span>
                    <strong>
                      {parsedSearchTime === null
                        ? "Enter a time"
                        : `${parsedSearchDate ?? selectedDate} · ${formatClockCompact(parsedSearchTime)}`}
                    </strong>
                  </button>
                </>
              ) : (
                <>
                  {rideMatches.length ? (
                    rideMatches.map(({ trip }) => (
                      <button key={trip.tripId} type="button" className="command-ride-result" onClick={() => selectSearchTrip(trip)}>
                        <span>{formatClockCompact(trip.startSeconds)}</span>
                        <strong>
                          {trip.startStationName} → {trip.endStationName}
                        </strong>
                        <em>{formatBikeLabel(trip)} · {formatDuration(trip.durationSeconds)}</em>
                      </button>
                    ))
                  ) : (
                    <p className="command-empty">Search the loaded day by station name or start time.</p>
                  )}
                </>
              )}
            </div>
          </section>
        </div>
      )}

      {selectedTrip && (
        <aside className="selected-trip-card" aria-label="Selected trip details">
          <div className="selected-trip-header">
            <div className="selected-trip-title">
              <Bike size={24} aria-hidden="true" />
              <strong>{formatBikeLabel(selectedTrip)}</strong>
            </div>
            <div className="selected-trip-actions">
              <span>{formatTripCode(selectedTrip)}</span>
              <button type="button" aria-label="Deselect trip" onClick={deselectSelectedTrip}>
                <X size={16} aria-hidden="true" />
              </button>
            </div>
          </div>

          <div className="selected-trip-route">
            <p>{selectedTrip.startStationName}</p>
            <span>to</span>
            <p>{selectedTrip.endStationName}</p>
          </div>

          <div className="selected-trip-time">
            <strong>
              {formatClockShort(selectedTrip.startSeconds)} – {formatClockShort(selectedTrip.endSeconds)}
            </strong>
          </div>

          <div className="selected-trip-stats">
            <span>{formatDuration(selectedTrip.durationSeconds)}</span>
            <span>{formatDistance(selectedTrip.distanceMeters)}</span>
            <span>{formatAverageSpeed(selectedTrip)}</span>
          </div>

          <div className="selected-trip-hint">
            <kbd>Esc</kbd>
            <span>to deselect</span>
          </div>
        </aside>
      )}

      {loadState === "error" && (
        <p className="error-toast" role="alert">
          {errorMessage}
        </p>
      )}
    </main>
  );
}
