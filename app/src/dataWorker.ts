import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbMvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbEhWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import type {
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

function sqlString(value: string) {
  return `'${value.replaceAll("'", "''")}'`;
}

function sqlList(values: string[]) {
  return values.length ? values.map(sqlString).join(",") : "''";
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
    await connection.query(`
      INSTALL httpfs;
      LOAD httpfs;
      SET enable_http_metadata_cache=true;
    `);
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
    await connection.query(`
      INSTALL httpfs;
      LOAD httpfs;
      SET enable_http_metadata_cache=true;
    `);

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

self.onmessage = async (event: MessageEvent<WorkerInboundMessage>) => {
  const message = event.data;
  if (message.type === "loadTrips") {
    try {
      self.postMessage(await loadTrips(message));
    } catch (error) {
      const response: TripLoadResponse = {
        type: "loadError",
        requestId: message.requestId,
        message: error instanceof Error ? error.message : "Unknown data loading error"
      };
      self.postMessage(response);
    }
    return;
  }

  if (message.type === "loadStations") {
    try {
      self.postMessage(await loadStations(message));
    } catch (error) {
      const response: StationLoadResponse = {
        type: "stationsLoadError",
        requestId: message.requestId,
        message: error instanceof Error ? error.message : "Unknown station loading error"
      };
      self.postMessage(response);
    }
  }
};
