import json

from tap_rest_api_msdk.utils import (
    apply_incremental_search_params,
    flatten_json,
    format_replication_bookmark,
    replication_key_schema_property,
)


def test_flatten_json():
    d = {
        "a": 1,
        "b": {"a": 2, "b": {"a": 3}, "c": {"a": "bacon", "b": "yum"}},
        "c": [{"foo": "bar"}, {"eggs": "spam"}],
        "d": [4, 5],
        "e.-f": 6,
    }
    ret = flatten_json(d, except_keys=["b_c"])
    assert ret["a"] == 1
    assert ret["b_a"] == 2
    assert ret["b_b_a"] == 3
    assert ret["b_c"] == json.dumps({"a": "bacon", "b": "yum"})
    assert ret["c"] == json.dumps([{"foo": "bar"}, {"eggs": "spam"}])
    assert ret["d"] == json.dumps([4, 5])
    assert ret["e__f"] == 6


def test_replication_key_schema_property():
    assert replication_key_schema_property("ts") == {
        "type": ["integer", "string", "null"]
    }
    assert replication_key_schema_property("@timestamp") == {
        "type": ["string", "null"],
        "format": "date-time",
    }


def test_format_replication_bookmark_preserves_unparseable_value():
    assert format_replication_bookmark("not-a-date", "%Y-%m-%d") == "not-a-date"


def test_apply_incremental_search_params_noop_without_last_run_date():
    params = {"query": "state:bounced"}
    assert apply_incremental_search_params(params, "date_from", "$last_run_date", "") == params
