import json

import requests_mock

from tap_rest_api_msdk.tap import TapRestApiMsdk
from tap_rest_api_msdk.utils import (
    apply_incremental_search_params,
    ensure_schema_has_replication_key,
    format_replication_bookmark,
)

from tests.test_streams import config, json_resp, url_path


def test_schema_inference_uses_post_body(requests_mock):
    def matcher(request):
        body = json.loads(request.body)
        return (
            request.method == "POST"
            and body["key"] == "secret"
            and body["date_from"] == "2024-09-12"
        )

    requests_mock.post(
        url_path(),
        additional_matcher=matcher,
        json=json_resp(),
    )

    configs = config(
        {
            "rest_method": "POST",
            "use_request_body_not_params": True,
            "start_date": "2024-09-12",
            "source_search_field": "date_from",
            "source_search_query": "$last_run_date",
            "replication_key": "key3",
        }
    )
    configs["streams"][0]["params"] = {"key": "secret"}

    stream0 = TapRestApiMsdk(config=configs, parse_env_config=True).discover_streams()[0]

    assert stream0.schema["properties"]["key3"]["type"] == "string"


def test_schema_inference_empty_response_includes_replication_key(requests_mock):
    requests_mock.get(url_path(), json=[])

    configs = config(
        {
            "replication_key": "@timestamp",
            "start_date": "2024-09-12",
            "source_search_field": "date_from",
            "source_search_query": "$last_run_date",
        }
    )
    configs["streams"][0]["primary_keys"] = ["_id"]

    stream0 = TapRestApiMsdk(config=configs, parse_env_config=True).discover_streams()[0]

    assert "@timestamp" in stream0.schema["properties"]
    assert stream0.schema["properties"]["@timestamp"]["format"] == "date-time"
    assert "_id" in stream0.schema["properties"]


def test_format_replication_bookmark():
    assert (
        format_replication_bookmark(
            "2026-06-11T07:47:18.194936103Z",
            "%Y-%m-%d %H:%M:%S",
        )
        == "2026-06-11 07:47:18"
    )


def test_apply_incremental_search_params():
    params = apply_incremental_search_params(
        {"query": "state:bounced"},
        "date_from",
        "$last_run_date",
        "2024-09-12",
    )

    assert params == {
        "query": "state:bounced",
        "date_from": "2024-09-12",
    }


def test_ensure_schema_has_replication_key():
    schema = ensure_schema_has_replication_key({}, "@timestamp", ["_id"])

    assert schema["properties"]["@timestamp"]["format"] == "date-time"
    assert schema["properties"]["_id"]["type"] == ["string", "null"]
