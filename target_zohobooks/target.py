"""Zohobooks target class."""

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_zohobooks.sinks import ZohobooksSink


class TargetZohobooks(Target):
    """Sample target for Zohobooks."""

    name = "target-zohobooks"
    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("refresh_token", th.StringType, required=True),
    ).to_dict()
    default_sink_class = ZohobooksSink


if __name__ == "__main__":
    TargetZohobooks.cli()
