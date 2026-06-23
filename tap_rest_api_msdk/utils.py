"""Basic utility functions."""

import json
from datetime import datetime
from string import Template
from typing import Any, Dict, Optional

import requests


def flatten_json(
    obj: dict,
    except_keys: Optional[list] = None,
    store_raw_json_message: Optional[bool] = False,
) -> dict:
    """Flattens a json object by appending the patch as a key in the returned object.

    Automatically converts arrays and any provided keys into json strings to prevent
    flattening further into those branches.

    Args:
        obj: the json object to be flattened.
        except_keys: list of the keys of the nodes that should be converted to json
            strings.
        store_raw_json_message: Additionally adds the raw JSON message to a field
        named _sdc_raw_json. Note: The field is a JSON type of object.

    Returns:
        A flattened json object.

    """
    out = {}
    if not except_keys:
        except_keys = []

    def t(s: str) -> str:
        """Translate a string to db friendly column names.

        Args:
            s: required - string to make a translation table from.

        Returns:
            Translation table.

        """
        translation_table = s.maketrans("-.", "__")
        return s.translate(translation_table)

    def flatten(o: Any, exception_keys: list, name: str = "") -> None:
        """Recursive flattening of the json object in place.

        Args:
            o: the json object to be flattened.
            exception_keys: list of the keys of the nodes that should
                be converted to json strings.
            name: the prefix for the exception_keys

        """
        if type(o) is dict:
            for k in o:
                # the key is in the list of keys to skip, convert to json string
                if name + k in exception_keys:
                    out[t(name + k)] = json.dumps(o[k])
                else:
                    flatten(o[k], exception_keys, name + k + "_")

        # if the object is an array, convert to a json string
        elif type(o) is list:
            out[t(name[:-1])] = json.dumps(o)

        # otherwise, translate the key to be database friendly
        else:
            out[t(name[:-1])] = o

    flatten(obj, exception_keys=except_keys)
    # Optional store the whole row in the _sdc_raw_json field.
    if store_raw_json_message:
        out["_sdc_raw_json"] = obj  # type: ignore[assignment]
    return out


def unnest_dict(d):
    """Flattens a dict object by create a new object with the key value pairs.

    Recursive flattening any nested dicts to a single level.

    Args:
        obj: the dict object to be flattened.

    Returns:
        A flattened dict object.

    """
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            result.update(unnest_dict(v))
        else:
            result[k] = v
    return result


def format_replication_bookmark(raw: Any, fmt: str) -> Any:
    """Format a bookmark value for API query parameters.

    Args:
        raw: Bookmark value from state or config.
        fmt: ``strftime`` format string.

    Returns:
        Formatted bookmark, or the original value if parsing fails.

    """
    if raw is None or raw == "":
        return raw

    raw_str = str(raw)
    try:
        normalized = raw_str.replace("Z", "+00:00")
        if "." in normalized:
            date_part, remainder = normalized.split(".", 1)
            tz_index = max(remainder.find("+"), remainder.find("-"))
            if tz_index == -1:
                fraction = remainder[:6].ljust(6, "0")[:6]
                normalized = f"{date_part}.{fraction}"
            else:
                fraction = remainder[:tz_index][:6].ljust(6, "0")[:6]
                normalized = f"{date_part}.{fraction}{remainder[tz_index:]}"

        return datetime.fromisoformat(normalized).strftime(fmt)
    except ValueError:
        return raw_str


def get_last_run_date_format(config: dict) -> str:
    """Return the configured bookmark date format for API requests."""
    return config.get("last_run_date_format", "%Y-%m-%dT%H:%M:%S")


def apply_incremental_search_params(
    params: dict,
    source_search_field: Optional[str],
    source_search_query: Optional[str],
    last_run_date: Any,
) -> dict:
    """Apply incremental search parameters to a request payload.

    Args:
        params: Base request parameters/body fields.
        source_search_field: API field used for incremental filtering.
        source_search_query: Template containing ``$last_run_date``.
        last_run_date: Value substituted into the template.

    Returns:
        Updated request parameters.

    """
    if not source_search_field or not source_search_query or not last_run_date:
        return params

    request_params = dict(params)
    query_template = Template(source_search_query)
    request_params[source_search_field] = query_template.substitute(
        last_run_date=last_run_date
    )
    return request_params


def replication_key_schema_property(replication_key: str) -> dict:
    """Build a Singer schema property for a replication key."""
    if replication_key in ("ts",):
        return {"type": ["integer", "string", "null"]}
    if replication_key.startswith("@") or "timestamp" in replication_key.lower():
        return {"type": ["string", "null"], "format": "date-time"}
    if replication_key.endswith("_at"):
        return {"type": ["string", "null"], "format": "date-time"}
    return {"type": ["string", "null"]}


def ensure_schema_has_replication_key(
    schema: dict,
    replication_key: Optional[str],
    primary_keys: Optional[list] = None,
) -> dict:
    """Ensure inferred schema includes replication and primary key fields."""
    properties = schema.setdefault("properties", {})

    if replication_key and replication_key not in properties:
        properties[replication_key] = replication_key_schema_property(replication_key)

    for primary_key in primary_keys or []:
        if primary_key not in properties:
            properties[primary_key] = {"type": ["string", "null"]}

    return schema


def normalize_null_only_schema_types(schema: dict) -> dict:
    """Convert null-only inferred properties to nullable strings."""
    for _, details in schema.get("properties", {}).items():
        if details.get("type") == "null":
            details["type"] = ["string", "null"]
    return schema


def issue_api_request(
    *,
    api_url: str,
    path: str,
    params: dict,
    headers: dict,
    rest_method: str = "GET",
    use_request_body_not_params: bool = False,
    http_auth: Any = None,
) -> requests.Response:
    """Issue an API request using the tap's configured HTTP semantics."""
    url = api_url + path
    method = rest_method.upper()

    if method == "POST":
        if use_request_body_not_params:
            return requests.post(
                url,
                json=params,
                auth=http_auth,
                headers=headers,
            )
        return requests.post(
            url,
            params=params,
            auth=http_auth,
            headers=headers,
        )

    return requests.get(
        url,
        params=params,
        auth=http_auth,
        headers=headers,
    )


def get_start_date(self, context: Optional[dict]) -> Any:
    """Return a start date if a DateTime bookmark is available.

    Otherwise it returns the starting date as defined in
    the start_date parameter.

    Args:
        context: - the singer context object.

    Returns:
        An start date else and empty string.

    """
    fmt = get_last_run_date_format(self.config)
    try:
        return self.get_starting_timestamp(context).strftime(fmt)
    except (ValueError, AttributeError):
        return format_replication_bookmark(
            self.get_starting_replication_key_value(context),
            fmt,
        )
