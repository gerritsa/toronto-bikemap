import { BarChart3, CalendarRange, Clock3, Route, Timer } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { TripDataClient } from "./dataClient";
import type {
  AnalyticsDailyPoint,
  AnalyticsHourlyPoint,
  AnalyticsOverview,
  AnalyticsTopRoute,
  FilterState,
  PublishedManifest
} from "./types";

type AnalyticsPageProps = {
  manifest: PublishedManifest | null;
  filters: FilterState;
  dataClient: TripDataClient | null;
};

type AnalyticsState = {
  overview: AnalyticsOverview;
  daily: AnalyticsDailyPoint[];
  hourly: AnalyticsHourlyPoint[];
  topRoutes: AnalyticsTopRoute[];
};

const EMPTY_ANALYTICS: AnalyticsState = {
  overview: {
    tripCount: 0,
    routeCount: 0,
    distanceMeters: 0,
    durationSeconds: 0,
    avgTripsPerDay: 0,
    dayCount: 0
  },
  daily: [],
  hourly: [],
  topRoutes: []
};

function normalizedRange(start: string, end: string) {
  return start <= end ? { dateStart: start, dateEnd: end } : { dateStart: end, dateEnd: start };
}

function formatDateLabel(dateString: string) {
  const [year, month, day] = dateString.split("-").map(Number);
  return new Intl.DateTimeFormat("en-CA", {
    month: "short",
    day: "numeric",
    year: "numeric"
  }).format(new Date(year, (month || 1) - 1, day || 1));
}

function formatHourLabel(hour: number) {
  const suffix = hour >= 12 ? "PM" : "AM";
  const hour12 = hour % 12 || 12;
  return `${hour12}${suffix}`;
}

function formatDistance(distanceMeters: number) {
  return `${(distanceMeters / 1000).toFixed(1)} km`;
}

function formatDuration(durationSeconds: number) {
  const totalMinutes = Math.round(durationSeconds / 60);
  if (totalMinutes >= 60) {
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  }
  return `${totalMinutes} min`;
}

function chartBarWidth(value: number, maxValue: number) {
  if (!maxValue) {
    return "0%";
  }
  return `${Math.max(6, Math.round((value / maxValue) * 100))}%`;
}

export function AnalyticsPage({ manifest, filters, dataClient }: AnalyticsPageProps) {
  const [dateStart, setDateStart] = useState(manifest?.source.dateMin ?? "");
  const [dateEnd, setDateEnd] = useState(manifest?.source.dateMax ?? "");
  const [analytics, setAnalytics] = useState<AnalyticsState>(EMPTY_ANALYTICS);
  const [loadState, setLoadState] = useState<"idle" | "loading" | "error">("idle");
  const [errorMessage, setErrorMessage] = useState("");

  useEffect(() => {
    setDateStart(manifest?.source.dateMin ?? "");
    setDateEnd(manifest?.source.dateMax ?? "");
  }, [manifest?.source.dateMin, manifest?.source.dateMax]);

  const range = useMemo(() => normalizedRange(dateStart, dateEnd), [dateEnd, dateStart]);
  const dailyMax = useMemo(() => analytics.daily.reduce((max, row) => Math.max(max, row.tripCount), 0), [analytics.daily]);
  const hourlyMax = useMemo(() => analytics.hourly.reduce((max, row) => Math.max(max, row.tripCount), 0), [analytics.hourly]);

  useEffect(() => {
    if (!manifest?.analytics || !dataClient || !range.dateStart || !range.dateEnd) {
      setAnalytics(EMPTY_ANALYTICS);
      return;
    }
    let cancelled = false;
    setLoadState("loading");
    setErrorMessage("");

    void dataClient
      .loadAnalytics({
        dailyUrl: manifest.analytics.dailyUrl,
        hourlyUrl: manifest.analytics.hourlyUrl,
        routesDailyUrl: manifest.analytics.routesDailyUrl,
        dateStart: range.dateStart,
        dateEnd: range.dateEnd,
        filters,
        topRouteLimit: 8
      })
      .then((response) => {
        if (cancelled) {
          return;
        }
        if (response.type === "analyticsLoadError") {
          setAnalytics(EMPTY_ANALYTICS);
          setErrorMessage(response.message);
          setLoadState("error");
          return;
        }
        setAnalytics({
          overview: response.overview,
          daily: response.daily,
          hourly: response.hourly,
          topRoutes: response.topRoutes
        });
        setLoadState("idle");
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setAnalytics(EMPTY_ANALYTICS);
        setErrorMessage(error instanceof Error ? error.message : "Could not load analytics");
        setLoadState("error");
      });

    return () => {
      cancelled = true;
    };
  }, [dataClient, filters, manifest, range.dateEnd, range.dateStart]);

  if (!manifest?.analytics) {
    return (
      <section className="analytics-shell analytics-empty-state">
        <div className="analytics-empty-copy">
          <BarChart3 size={28} />
          <strong>Analytics unavailable</strong>
          <p>This manifest does not advertise precomputed analytics assets yet.</p>
        </div>
      </section>
    );
  }

  return (
    <section className="analytics-shell" aria-label="Analytics dashboard">
      <header className="analytics-header">
        <div>
          <span className="analytics-eyebrow">Overview</span>
          <h1>Trip analytics</h1>
        </div>
        <div className="analytics-range">
          <label>
            <span>From</span>
            <input
              type="date"
              min={manifest.source.dateMin}
              max={manifest.source.dateMax}
              value={dateStart}
              onChange={(event) => setDateStart(event.target.value)}
            />
          </label>
          <label>
            <span>To</span>
            <input
              type="date"
              min={manifest.source.dateMin}
              max={manifest.source.dateMax}
              value={dateEnd}
              onChange={(event) => setDateEnd(event.target.value)}
            />
          </label>
        </div>
      </header>

      <div className="analytics-summary-grid">
        <article className="analytics-card">
          <span className="analytics-card-label">
            <CalendarRange size={16} />
            Trips
          </span>
          <strong>{analytics.overview.tripCount.toLocaleString()}</strong>
          <p>{analytics.overview.dayCount.toLocaleString()} days in range</p>
        </article>
        <article className="analytics-card">
          <span className="analytics-card-label">
            <Route size={16} />
            Routes
          </span>
          <strong>{analytics.overview.routeCount.toLocaleString()}</strong>
          <p>Unique routes in range</p>
        </article>
        <article className="analytics-card">
          <span className="analytics-card-label">
            <BarChart3 size={16} />
            Distance
          </span>
          <strong>{formatDistance(analytics.overview.distanceMeters)}</strong>
          <p>Total ridden distance</p>
        </article>
        <article className="analytics-card">
          <span className="analytics-card-label">
            <Timer size={16} />
            Duration
          </span>
          <strong>{formatDuration(analytics.overview.durationSeconds)}</strong>
          <p>Total ride time</p>
        </article>
        <article className="analytics-card">
          <span className="analytics-card-label">
            <Clock3 size={16} />
            Avg/day
          </span>
          <strong>{analytics.overview.avgTripsPerDay.toFixed(1)}</strong>
          <p>Trips per day</p>
        </article>
      </div>

      <div className="analytics-panels">
        <section className="analytics-panel">
          <div className="analytics-panel-header">
            <h2>Trips per day</h2>
            <span>
              {formatDateLabel(range.dateStart)} to {formatDateLabel(range.dateEnd)}
            </span>
          </div>
          <div className="analytics-bar-list">
            {analytics.daily.length ? (
              analytics.daily.map((row) => (
                <div key={row.serviceDate} className="analytics-bar-row">
                  <span>{formatDateLabel(row.serviceDate)}</span>
                  <div className="analytics-bar-track">
                    <div className="analytics-bar-fill" style={{ width: chartBarWidth(row.tripCount, dailyMax) }} />
                  </div>
                  <strong>{row.tripCount.toLocaleString()}</strong>
                </div>
              ))
            ) : (
              <p className="analytics-empty">No daily analytics for this range.</p>
            )}
          </div>
        </section>

        <section className="analytics-panel">
          <div className="analytics-panel-header">
            <h2>Most popular times</h2>
            <span>Starts by hour</span>
          </div>
          <div className="analytics-bar-list">
            {analytics.hourly.length ? (
              analytics.hourly.map((row) => (
                <div key={row.hour} className="analytics-bar-row">
                  <span>{formatHourLabel(row.hour)}</span>
                  <div className="analytics-bar-track">
                    <div className="analytics-bar-fill analytics-bar-fill-accent" style={{ width: chartBarWidth(row.tripCount, hourlyMax) }} />
                  </div>
                  <strong>{row.tripCount.toLocaleString()}</strong>
                </div>
              ))
            ) : (
              <p className="analytics-empty">No hourly analytics for this range.</p>
            )}
          </div>
        </section>
      </div>

      <section className="analytics-panel analytics-routes-panel">
        <div className="analytics-panel-header">
          <h2>Most popular routes</h2>
          <span>Top {analytics.topRoutes.length || 0}</span>
        </div>
        {analytics.topRoutes.length ? (
          <div className="analytics-route-table">
            {analytics.topRoutes.map((route, index) => (
              <div key={route.routeId} className="analytics-route-row">
                <span className="analytics-route-rank">{index + 1}</span>
                <div className="analytics-route-main">
                  <strong>
                    {route.startStationName} to {route.endStationName}
                  </strong>
                  <span>{formatDistance(route.distanceMeters)}</span>
                </div>
                <span className="analytics-route-count">{route.tripCount.toLocaleString()} trips</span>
              </div>
            ))}
          </div>
        ) : (
          <p className="analytics-empty">No route analytics for this range.</p>
        )}
      </section>

      {loadState === "loading" && <p className="analytics-status">Loading analytics…</p>}
      {loadState === "error" && (
        <p className="analytics-status analytics-status-error" role="alert">
          {errorMessage}
        </p>
      )}
    </section>
  );
}
