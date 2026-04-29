import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbMvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbEhWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import type {
  AnalyticsDailyPoint,
  AnalyticsHourlyPoint,
  AnalyticsLoadRequest,
  AnalyticsLoadResponse,
  AnalyticsOverview,
  AnalyticsTopRoute,
  FlowTrip,
  ParkingStation,
  StationLoadRequest,
  StationLoadResponse,
  TripLoadRequest,
  TripLoadResponse,
  WorkerInboundMessage
} from "./types";
import { decodePolyline, timestampsForPath } from "./polyline";

let dbPromise: Promise<duckdb.AsyncDuckDB> | null = null;
const DUCKDB_BUNDLES: duckdb.DuckDBBundles = {
  mvp: {
    mainModule: duckdbMvpWasm,
    mainWorker: duckdbMvpWorker
  },
  eh: {
    mainModule: duckdbEhWasm,
    mainWorker: duckdbEhWorker
  }
};

async function getDuckDb() {
  if (!dbPromise) {
    dbPromise = (async () => {
      const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);
      if (!bundle.mainWorker) {
        throw new Error("DuckDB WASM worker bundle is unavailable");
      }
      const worker = new Worker(bundle.mainWorker);
      const logger = new duckdb.ConsoleLogger();
      const db = new duckdb.AsyncDuckDB(logger, worker);
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      return db;
    })();
  }
  return dbPromise;
}

async function configureHttpfs(connection: duckdb.AsyncDuckDBConnection) {
  await connection.query(`
    INSTALL httpfs;
    LOAD httpfs;
    SET enable_http_metadata_cache=true;
  `);
}

function sqlString(value: string) {
  return `'${value.replaceAll("'", "''")}'`;
}

function sqlList(values: string[]) {
  return values.length ? values.map(sqlString).join(",") : "''";
}

function numberField(value: unknown) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function parkingStationFromRow(row: Record<string, unknown>): ParkingStation | null {
  const lat = Number(row.lat);
  const lon = Number(row.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }
  return {
    stationId: String(row.station_id ?? ""),
    name: String(row.name ?? row.station_id ?? "Station"),
    lat,
    lon
  };
}

function normalizeDateRange(dateStart: string, dateEnd: string) {
  return dateStart <= dateEnd ? { dateStart, dateEnd } : { dateStart: dateEnd, dateEnd: dateStart };
}

function analyticsFiltersMatch(row: Record<string, unknown>, request: AnalyticsLoadRequest) {
  const { dateStart, dateEnd } = normalizeDateRange(request.dateStart, request.dateEnd);
  const serviceDate = String(row.service_date ?? "");
  const userType = String(row.user_type ?? "");
  const bikeCategory = String(row.bike_category ?? "");
  return (
    serviceDate >= dateStart &&
    serviceDate <= dateEnd &&
    request.filters.userTypes.includes(userType as never) &&
    request.filters.bikeCategories.includes(bikeCategory as never)
  );
}

function buildAnalyticsOverview(daily: AnalyticsDailyPoint[], routeCount: number, totals: { distanceMeters: number; durationSeconds: number }) {
  const tripCount = daily.reduce((sum, row) => sum + row.tripCount, 0);
  const dayCount = daily.length;
  return {
    tripCount,
    routeCount,
    distanceMeters: totals.distanceMeters,
    durationSeconds: totals.durationSeconds,
    avgTripsPerDay: dayCount ? tripCount / dayCount : 0,
    dayCount
  } satisfies AnalyticsOverview;
}

async function loadStationsFromJson(url: string): Promise<ParkingStation[]> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error("Station list could not be loaded");
  }
  const rows = (await response.json()) as Array<Record<string, unknown>>;
  return rows.flatMap((row) => {
    const station = parkingStationFromRow(row);
    return station ? [station] : [];
  });
}

async function loadStationsFromParquet(url: string): Promise<ParkingStation[]> {
  const db = await getDuckDb();
  const connection = await db.connect();
  try {
    await configureHttpfs(connection);
    const table = await connection.query(`
      SELECT station_id, name, lat, lon
      FROM read_parquet(${sqlString(url)})
    `);
    return table.toArray().flatMap((row) => {
      const record = row as unknown as Record<string, unknown>;
      const station = parkingStationFromRow(record);
      return station ? [station] : [];
    });
  } finally {
    await connection.close();
  }
}

async function loadStations(request: StationLoadRequest): Promise<StationLoadResponse> {
  const stations = request.stationsUrl.endsWith(".json")
    ? await loadStationsFromJson(request.stationsUrl)
    : await loadStationsFromParquet(request.stationsUrl);
  return {
    type: "stationsLoaded",
    requestId: request.requestId,
    stations
  };
}

async function loadJsonFallback(request: TripLoadRequest): Promise<TripLoadResponse> {
  const [tripsResponse, routesResponse] = await Promise.all([fetch(request.tripsUrl), fetch(request.routesUrl)]);
  if (!tripsResponse.ok || !routesResponse.ok) {
    throw new Error("Sample data files could not be loaded");
  }
  const tripRows = (await tripsResponse.json()) as Array<Record<string, unknown>>;
  const routeRows = (await routesResponse.json()) as Array<Record<string, unknown>>;
  const routes = new Map(routeRows.map((row) => [String(row.route_id), row]));

  const rows = tripRows.filter((row) => {
    const userType = String(row.user_type);
    const bikeModel = String(row.bike_model);
    const bikeCategory = String(row.bike_category);
    return (
      request.filters.userTypes.includes(userType as never) &&
      request.filters.bikeModels.includes(bikeModel as never) &&
      request.filters.bikeCategories.includes(bikeCategory as never)
    );
  });

  const cappedRows = rows.slice(0, request.maxTrips);
  const trips = cappedRows.flatMap((row): FlowTrip[] => {
    const route = routes.get(String(row.route_id));
    const encoded = String(route?.encoded_polyline ?? "");
    if (!encoded) {
      return [];
    }
    const path = decodePolyline(encoded);
    const startSeconds = Number(row.start_seconds);
    const endSeconds = Number(row.end_seconds);
    return [
      {
        tripId: String(row.trip_id),
        routeId: String(row.route_id),
        userType: String(row.user_type) as FlowTrip["userType"],
        bikeModel: String(row.bike_model) as FlowTrip["bikeModel"],
        bikeCategory: String(row.bike_category) as FlowTrip["bikeCategory"],
        startStationName: String(row.start_station_name ?? row.start_station_id ?? "Unknown station"),
        endStationName: String(row.end_station_name ?? row.end_station_id ?? "Unknown station"),
        startSeconds,
        endSeconds,
        durationSeconds: Number(row.duration_seconds ?? Math.max(0, endSeconds - startSeconds)),
        distanceMeters: Number(route?.distance_meters ?? 0),
        isSameStation: Boolean(row.is_same_station),
        routeStatus: String(route?.route_status ?? "ok"),
        path,
        timestamps: timestampsForPath(path, startSeconds, endSeconds)
      }
    ];
  });

  return {
    type: "tripsLoaded",
    requestId: request.requestId,
    trips,
    totalMatchingTrips: rows.length,
    capped: rows.length > cappedRows.length
  };
}

async function loadTrips(request: TripLoadRequest): Promise<TripLoadResponse> {
  if (request.tripsUrl.endsWith(".json") || request.routesUrl.endsWith(".json")) {
    return loadJsonFallback(request);
  }

  const db = await getDuckDb();
  const connection = await db.connect();
  try {
    await configureHttpfs(connection);

    const where = `
      t.user_type IN (${sqlList(request.filters.userTypes)})
      AND t.bike_model IN (${sqlList(request.filters.bikeModels)})
      AND t.bike_category IN (${sqlList(request.filters.bikeCategories)})
    `;

    const countTable = await connection.query(`
      SELECT COUNT(*)::INTEGER AS count
      FROM read_parquet(${sqlString(request.tripsUrl)}) t
      WHERE ${where}
    `);
    const totalMatchingTrips = Number(countTable.toArray()[0]?.count ?? 0);

    const table = await connection.query(`
      SELECT
        t.trip_id,
        t.route_id,
        t.user_type,
        t.bike_model,
        t.bike_category,
        t.start_station_name,
        t.end_station_name,
        t.start_seconds,
        t.end_seconds,
        t.duration_seconds,
        t.is_same_station,
        r.distance_meters,
        r.encoded_polyline,
        r.route_status
      FROM read_parquet(${sqlString(request.tripsUrl)}) t
      INNER JOIN read_parquet(${sqlString(request.routesUrl)}) r
        ON t.route_id = r.route_id
      WHERE ${where}
      ORDER BY t.start_seconds
      LIMIT ${request.maxTrips}
    `);

    const trips = table.toArray().flatMap((row): FlowTrip[] => {
      const encoded = String(row.encoded_polyline ?? "");
      if (!encoded) {
        return [];
      }
      const path = decodePolyline(encoded);
      const startSeconds = Number(row.start_seconds);
      const endSeconds = Number(row.end_seconds);
      return [
        {
          tripId: String(row.trip_id),
          routeId: String(row.route_id),
          userType: String(row.user_type) as FlowTrip["userType"],
          bikeModel: String(row.bike_model) as FlowTrip["bikeModel"],
          bikeCategory: String(row.bike_category) as FlowTrip["bikeCategory"],
          startStationName: String(row.start_station_name ?? "Unknown station"),
          endStationName: String(row.end_station_name ?? "Unknown station"),
          startSeconds,
          endSeconds,
          durationSeconds: Number(row.duration_seconds ?? Math.max(0, endSeconds - startSeconds)),
          distanceMeters: Number(row.distance_meters ?? 0),
          isSameStation: Boolean(row.is_same_station),
          routeStatus: String(row.route_status),
          path,
          timestamps: timestampsForPath(path, startSeconds, endSeconds)
        }
      ];
    });

    return {
      type: "tripsLoaded",
      requestId: request.requestId,
      trips,
      totalMatchingTrips,
      capped: totalMatchingTrips > trips.length
    };
  } finally {
    await connection.close();
  }
}

async function loadAnalyticsFromJson(request: AnalyticsLoadRequest): Promise<AnalyticsLoadResponse> {
  const [dailyResponse, hourlyResponse, routesResponse] = await Promise.all([
    fetch(request.dailyUrl),
    fetch(request.hourlyUrl),
    fetch(request.routesDailyUrl)
  ]);
  if (!dailyResponse.ok || !hourlyResponse.ok || !routesResponse.ok) {
    throw new Error("Analytics files could not be loaded");
  }

  const dailyRows = ((await dailyResponse.json()) as Array<Record<string, unknown>>).filter((row) => analyticsFiltersMatch(row, request));
  const dailyByDate = new Map<string, AnalyticsDailyPoint>();
  let distanceMeters = 0;
  let durationSeconds = 0;
  for (const row of dailyRows) {
    const serviceDate = String(row.service_date ?? "");
    const tripCount = numberField(row.trip_count);
    distanceMeters += numberField(row.distance_meters_sum);
    durationSeconds += numberField(row.duration_seconds_sum);
    dailyByDate.set(serviceDate, {
      serviceDate,
      tripCount: (dailyByDate.get(serviceDate)?.tripCount ?? 0) + tripCount
    });
  }
  const daily = [...dailyByDate.values()].sort((first, second) => first.serviceDate.localeCompare(second.serviceDate));

  const hourlyRows = ((await hourlyResponse.json()) as Array<Record<string, unknown>>).filter((row) => analyticsFiltersMatch(row, request));
  const hourlyByHour = new Map<number, number>();
  for (const row of hourlyRows) {
    const hour = numberField(row.hour);
    hourlyByHour.set(hour, (hourlyByHour.get(hour) ?? 0) + numberField(row.trip_count));
  }
  const hourly = [...hourlyByHour.entries()]
    .sort((first, second) => first[0] - second[0])
    .map(([hour, tripCount]) => ({ hour, tripCount }));

  const routesRows = ((await routesResponse.json()) as Array<Record<string, unknown>>).filter((row) => analyticsFiltersMatch(row, request));
  const routeMap = new Map<string, AnalyticsTopRoute>();
  for (const row of routesRows) {
    const routeId = String(row.route_id ?? "");
    const existing = routeMap.get(routeId);
    routeMap.set(routeId, {
      routeId,
      startStationName: String(row.start_station_name ?? ""),
      endStationName: String(row.end_station_name ?? ""),
      tripCount: (existing?.tripCount ?? 0) + numberField(row.trip_count),
      distanceMeters: existing?.distanceMeters ?? numberField(row.distance_meters)
    });
  }
  const topRoutes = [...routeMap.values()]
    .sort((first, second) => second.tripCount - first.tripCount || first.routeId.localeCompare(second.routeId))
    .slice(0, request.topRouteLimit);

  return {
    type: "analyticsLoaded",
    requestId: request.requestId,
    overview: buildAnalyticsOverview(daily, routeMap.size, { distanceMeters, durationSeconds }),
    daily,
    hourly,
    topRoutes
  };
}

async function loadAnalyticsFromParquet(request: AnalyticsLoadRequest): Promise<AnalyticsLoadResponse> {
  const db = await getDuckDb();
  const connection = await db.connect();
  try {
    await configureHttpfs(connection);
    const { dateStart, dateEnd } = normalizeDateRange(request.dateStart, request.dateEnd);
    const where = `
      service_date BETWEEN ${sqlString(dateStart)} AND ${sqlString(dateEnd)}
      AND user_type IN (${sqlList(request.filters.userTypes)})
      AND bike_category IN (${sqlList(request.filters.bikeCategories)})
    `;

    const dailyTable = await connection.query(`
      SELECT
        service_date,
        SUM(trip_count)::INTEGER AS trip_count,
        SUM(distance_meters_sum)::DOUBLE AS distance_meters_sum,
        SUM(duration_seconds_sum)::DOUBLE AS duration_seconds_sum
      FROM read_parquet(${sqlString(request.dailyUrl)})
      WHERE ${where}
      GROUP BY service_date
      ORDER BY service_date
    `);
    const dailyRows = dailyTable.toArray();
    const daily = dailyRows.map((row) => ({
      serviceDate: String(row.service_date ?? ""),
      tripCount: numberField(row.trip_count)
    }));
    const distanceMeters = dailyRows.reduce((sum, row) => sum + numberField(row.distance_meters_sum), 0);
    const durationSeconds = dailyRows.reduce((sum, row) => sum + numberField(row.duration_seconds_sum), 0);

    const hourlyTable = await connection.query(`
      SELECT
        hour,
        SUM(trip_count)::INTEGER AS trip_count
      FROM read_parquet(${sqlString(request.hourlyUrl)})
      WHERE ${where}
      GROUP BY hour
      ORDER BY hour
    `);
    const hourly = hourlyTable.toArray().map((row) => ({
      hour: numberField(row.hour),
      tripCount: numberField(row.trip_count)
    }));

    const routesCountTable = await connection.query(`
      SELECT COUNT(DISTINCT route_id)::INTEGER AS route_count
      FROM read_parquet(${sqlString(request.routesDailyUrl)})
      WHERE ${where}
    `);
    const routeCount = numberField(routesCountTable.toArray()[0]?.route_count);

    const routesTable = await connection.query(`
      SELECT
        route_id,
        MIN(start_station_name) AS start_station_name,
        MIN(end_station_name) AS end_station_name,
        SUM(trip_count)::INTEGER AS trip_count,
        MAX(distance_meters)::DOUBLE AS distance_meters
      FROM read_parquet(${sqlString(request.routesDailyUrl)})
      WHERE ${where}
      GROUP BY route_id
      ORDER BY trip_count DESC, route_id
      LIMIT ${request.topRouteLimit}
    `);
    const topRoutes = routesTable.toArray().map((row) => ({
      routeId: String(row.route_id ?? ""),
      startStationName: String(row.start_station_name ?? ""),
      endStationName: String(row.end_station_name ?? ""),
      tripCount: numberField(row.trip_count),
      distanceMeters: numberField(row.distance_meters)
    }));

    return {
      type: "analyticsLoaded",
      requestId: request.requestId,
      overview: buildAnalyticsOverview(daily, routeCount, { distanceMeters, durationSeconds }),
      daily,
      hourly,
      topRoutes
    };
  } finally {
    await connection.close();
  }
}

async function loadAnalytics(request: AnalyticsLoadRequest): Promise<AnalyticsLoadResponse> {
  if (request.dailyUrl.endsWith(".json") || request.hourlyUrl.endsWith(".json") || request.routesDailyUrl.endsWith(".json")) {
    return loadAnalyticsFromJson(request);
  }
  return loadAnalyticsFromParquet(request);
}

self.onmessage = async (event: MessageEvent<WorkerInboundMessage>) => {
  const message = event.data;
  try {
    if (message.type === "loadTrips") {
      self.postMessage(await loadTrips(message));
      return;
    }
    if (message.type === "loadStations") {
      self.postMessage(await loadStations(message));
      return;
    }
    if (message.type === "loadAnalytics") {
      self.postMessage(await loadAnalytics(message));
    }
  } catch (error) {
    const messageText = error instanceof Error ? error.message : "Unknown worker error";
    if (message.type === "loadTrips") {
      const response: TripLoadResponse = {
        type: "loadError",
        requestId: message.requestId,
        message: messageText
      };
      self.postMessage(response);
      return;
    }
    if (message.type === "loadStations") {
      const response: StationLoadResponse = {
        type: "stationsLoadError",
        requestId: message.requestId,
        message: messageText
      };
      self.postMessage(response);
      return;
    }
    const response: AnalyticsLoadResponse = {
      type: "analyticsLoadError",
      requestId: message.requestId,
      message: messageText
    };
    self.postMessage(response);
  }
};
