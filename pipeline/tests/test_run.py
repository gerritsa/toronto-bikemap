import pandas as pd

from bikeshare_pipeline.run import build_manifest, find_year_resources, previous_manifest_is_current


def sample_package() -> dict:
    return {
        "result": {
            "resources": [
                {
                    "name": "bikeshare-ridership-2024.zip",
                    "url": "https://example.com/2024.zip",
                    "last_modified": "2024-10-25T00:00:00",
                    "size": 100,
                },
                {
                    "name": "bikeshare-ridership-2025.zip",
                    "url": "https://example.com/2025.zip",
                    "last_modified": "2025-12-31T00:00:00",
                    "size": 200,
                },
            ]
        }
    }


def test_find_year_resources_returns_requested_years_in_order():
    resources = find_year_resources(sample_package(), [2024, 2025])
    assert [resource["name"] for resource in resources] == [
        "bikeshare-ridership-2024.zip",
        "bikeshare-ridership-2025.zip",
    ]


def test_build_manifest_includes_multi_year_resources_and_analytics():
    manifest = build_manifest(
        find_year_resources(sample_package(), [2024, 2025]),
        trips=pd.DataFrame(
            [
                {"service_date": "2024-01-01", "user_type": "Member", "bike_model": "ICONIC"},
                {"service_date": "2025-12-31", "user_type": "Casual", "bike_model": "ASTRO"},
            ]
        ),
        run_id="20260428T000000Z",
        public_base_url="https://example.com",
        has_basemap=False,
    )
    assert manifest["source"]["dateMin"] == "2024-01-01"
    assert manifest["source"]["dateMax"] == "2025-12-31"
    assert len(manifest["source"]["ridershipResources"]) == 2
    assert manifest["analytics"]["dailyUrl"].endswith("/analytics/daily.parquet")


def test_previous_manifest_is_current_uses_multi_year_resource_list(monkeypatch):
    class Response:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "source": {
                    "ridershipResources": [
                        {
                            "year": 2024,
                            "name": "bikeshare-ridership-2024.zip",
                            "url": "https://example.com/2024.zip",
                            "lastModified": "2024-10-25T00:00:00",
                            "sizeBytes": 100,
                        },
                        {
                            "year": 2025,
                            "name": "bikeshare-ridership-2025.zip",
                            "url": "https://example.com/2025.zip",
                            "lastModified": "2025-12-31T00:00:00",
                            "sizeBytes": 200,
                        },
                    ]
                }
            }

    monkeypatch.setattr("bikeshare_pipeline.run.requests.get", lambda *_args, **_kwargs: Response())
    assert previous_manifest_is_current("https://example.com/manifest/latest.json", find_year_resources(sample_package(), [2024, 2025]))
