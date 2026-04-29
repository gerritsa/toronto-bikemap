export type BikeModel = "ICONIC" | "EFIT" | "ASTRO";
export type UserType = "Member" | "Casual";
export type BikeCategory = "Classic" | "E-bike";

export type PublishedManifest = {
  runId: string;
  generatedAt: string;
  source: {
    ckanPackageId: "bike-share-toronto-ridership-data";
    ridershipResourceName: string;
    ridershipUrl: string;
    ridershipLastModified: string;
    ridershipSizeBytes?: number;
    ridershipResources?: Array<{
      year: number;
      name: string;
      url: string;
      lastModified: string;
      sizeBytes: number;
    }>;
    dateMin: string;
    dateMax: string;
    tripCount: number;
  };
  assets: {
    tripsBaseUrl: string;
    routesUrl: string;
    stationsUrl: string;
    basemapUrl: string;
  };
  analytics?: {
    dailyUrl: string;
    hourlyUrl: string;
    routesDailyUrl?: string;
  };
  filters: {
    userTypes: UserType[];
    bikeModels: BikeModel[];
    bikeCategories: Record<BikeCategory, BikeModel[]>;
  };
  dates: string[];
};

export type FlowTrip = {
  tripId: string;
  routeId: string;
  userType: UserType;
  bikeModel: BikeModel;
  bikeCategory: BikeCategory;
  startStationName: string;
  endStationName: string;
  startSeconds: number;
  endSeconds: number;
  durationSeconds: number;
  distanceMeters: number;
  isSameStation: boolean;
  routeStatus: string;
  path: [number, number][];
  timestamps: number[];
};

export type FilterState = {
  userTypes: UserType[];
  bikeModels: BikeModel[];
  bikeCategories: BikeCategory[];
};

export type TripLoadRequest = {
  type: "loadTrips";
  requestId: string;
  date: string;
  tripsUrl: string;
  routesUrl: string;
  filters: FilterState;
  maxTrips: number;
};

export type TripLoadResponse =
  | {
      type: "tripsLoaded";
      requestId: string;
      trips: FlowTrip[];
      totalMatchingTrips: number;
      capped: boolean;
    }
  | {
      type: "loadError";
      requestId: string;
      message: string;
    };

export type AnalyticsOverview = {
  tripCount: number;
  distanceMeters: number;
  durationSeconds: number;
  avgTripsPerDay: number;
  dayCount: number;
};

export type AnalyticsDailyPoint = {
  serviceDate: string;
  tripCount: number;
};

export type AnalyticsHourlyPoint = {
  hour: number;
  tripCount: number;
};

export type AnalyticsTopRoute = {
  routeId: string;
  startStationName: string;
  endStationName: string;
  tripCount: number;
  distanceMeters: number;
};

export type AnalyticsLoadRequest = {
  type: "loadAnalytics";
  requestId: string;
  dailyUrl: string;
  hourlyUrl: string;
  routesDailyUrl?: string;
  dateStart: string;
  dateEnd: string;
  filters: FilterState;
  topRouteLimit: number;
};

export type AnalyticsLoadResponse =
  | {
      type: "analyticsLoaded";
      requestId: string;
      overview: AnalyticsOverview;
      daily: AnalyticsDailyPoint[];
      hourly: AnalyticsHourlyPoint[];
      topRoutes: AnalyticsTopRoute[];
    }
  | {
      type: "analyticsLoadError";
      requestId: string;
      message: string;
    };

export type ParkingStation = {
  stationId: string;
  name: string;
  lat: number;
  lon: number;
};

export type StationLoadRequest = {
  type: "loadStations";
  requestId: string;
  stationsUrl: string;
};

export type StationLoadResponse =
  | {
      type: "stationsLoaded";
      requestId: string;
      stations: ParkingStation[];
    }
  | {
      type: "stationsLoadError";
      requestId: string;
      message: string;
    };

export type WorkerInboundMessage = TripLoadRequest | StationLoadRequest | AnalyticsLoadRequest;
export type WorkerOutboundMessage = TripLoadResponse | StationLoadResponse | AnalyticsLoadResponse;
