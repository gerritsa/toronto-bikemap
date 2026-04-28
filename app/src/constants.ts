import type { FilterState } from "./types";

export const TORONTO_VIEW_STATE = {
  longitude: -79.3832,
  latitude: 43.6532,
  zoom: 12,
  bearing: 0,
  pitch: 0
};

export const DEFAULT_FILTERS: FilterState = {
  userTypes: ["Member", "Casual"],
  bikeModels: ["ICONIC", "EFIT", "ASTRO"],
  bikeCategories: ["Classic", "E-bike"]
};

export const SPEEDS = [1, 5, 15, 30, 60, 120];
export const DAY_SECONDS = 24 * 60 * 60;
export const DEFAULT_MAX_TRIPS = 25000;
