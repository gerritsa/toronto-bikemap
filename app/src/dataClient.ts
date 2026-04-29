import type {
  AnalyticsLoadRequest,
  AnalyticsLoadResponse,
  FilterState,
  ParkingStation,
  PublishedManifest,
  StationLoadRequest,
  StationLoadResponse,
  TripLoadRequest,
  TripLoadResponse
} from "./types";

export async function loadManifest(): Promise<PublishedManifest> {
  const url = import.meta.env.VITE_MANIFEST_URL || "/sample/latest.json";
  const response = await fetch(url, { cache: "no-cache" });
  if (!response.ok) {
    throw new Error(`Could not load manifest from ${url}`);
  }
  return response.json();
}

export function tripPartitionUrl(manifest: PublishedManifest, date: string) {
  if (manifest.assets.tripsBaseUrl.endsWith(".json")) {
    return manifest.assets.tripsBaseUrl;
  }

  const [year, month, day] = date.split("-");
  const base = manifest.assets.tripsBaseUrl.replace(/\/$/, "");
  return `${base}/year=${year}/month=${month}/day=${day}/trips.parquet`;
}

export class TripDataClient {
  private worker = new Worker(new URL("./dataWorker.ts", import.meta.url), { type: "module" });
  private pendingTrips = new Map<string, (response: TripLoadResponse) => void>();
  private pendingStations = new Map<string, (response: StationLoadResponse) => void>();
  private pendingAnalytics = new Map<string, (response: AnalyticsLoadResponse) => void>();

  constructor() {
    this.worker.onmessage = (event: MessageEvent<TripLoadResponse | StationLoadResponse | AnalyticsLoadResponse>) => {
      const data = event.data;
      if (data.type === "tripsLoaded" || data.type === "loadError") {
        const resolver = this.pendingTrips.get(data.requestId);
        if (resolver) {
          this.pendingTrips.delete(data.requestId);
          resolver(data);
        }
        return;
      }
      if (data.type === "stationsLoaded" || data.type === "stationsLoadError") {
        const resolver = this.pendingStations.get(data.requestId);
        if (resolver) {
          this.pendingStations.delete(data.requestId);
          resolver(data);
        }
        return;
      }
      if (data.type === "analyticsLoaded" || data.type === "analyticsLoadError") {
        const resolver = this.pendingAnalytics.get(data.requestId);
        if (resolver) {
          this.pendingAnalytics.delete(data.requestId);
          resolver(data);
        }
      }
    };
  }

  loadTrips(params: {
    date: string;
    tripsUrl: string;
    routesUrl: string;
    filters: FilterState;
    maxTrips: number;
  }): Promise<TripLoadResponse> {
    const requestId = crypto.randomUUID();
    const request: TripLoadRequest = {
      type: "loadTrips",
      requestId,
      ...params
    };

    return new Promise((resolve) => {
      this.pendingTrips.set(requestId, resolve);
      this.worker.postMessage(request);
    });
  }

  loadStations(stationsUrl: string): Promise<ParkingStation[]> {
    const requestId = crypto.randomUUID();
    const request: StationLoadRequest = {
      type: "loadStations",
      requestId,
      stationsUrl
    };

    return new Promise((resolve, reject) => {
      this.pendingStations.set(requestId, (response) => {
        if (response.type === "stationsLoaded") {
          resolve(response.stations);
        } else if (response.type === "stationsLoadError") {
          reject(new Error(response.message));
        } else {
          reject(new Error("Unexpected worker response while loading stations"));
        }
      });
      this.worker.postMessage(request);
    });
  }

  loadAnalytics(params: {
    dailyUrl: string;
    hourlyUrl: string;
    routesDailyUrl?: string;
    dateStart: string;
    dateEnd: string;
    filters: FilterState;
    topRouteLimit?: number;
  }): Promise<AnalyticsLoadResponse> {
    const requestId = crypto.randomUUID();
    const request: AnalyticsLoadRequest = {
      type: "loadAnalytics",
      requestId,
      topRouteLimit: 10,
      ...params
    };

    return new Promise((resolve) => {
      this.pendingAnalytics.set(requestId, resolve);
      this.worker.postMessage(request);
    });
  }

  dispose() {
    this.worker.terminate();
    this.pendingTrips.clear();
    this.pendingStations.clear();
    this.pendingAnalytics.clear();
  }
}
