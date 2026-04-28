from __future__ import annotations

import pandas as pd


def route_qa_summary(routes: pd.DataFrame) -> dict:
    by_status = routes["route_status"].value_counts().sort_index().to_dict()
    by_engine = routes["routing_engine"].value_counts().sort_index().to_dict()
    return {
        "routeCount": int(len(routes)),
        "byStatus": {str(key): int(value) for key, value in by_status.items()},
        "byEngine": {str(key): int(value) for key, value in by_engine.items()},
        "fallbackStraightLineCount": int(by_status.get("fallback_straight_line", 0)),
        "syntheticLoopCount": int(routes["is_synthetic_loop"].sum()),
        "missingStationRouteCount": int(by_status.get("missing_station", 0)),
    }
