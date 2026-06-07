"""Minimal Airflow plugin shim to ensure the DataHub plugin package is loaded."""

from airflow.plugins_manager import AirflowPlugin

try:
    import datahub_airflow_plugin  # noqa: F401
except Exception:  # pragma: no cover - optional in non-DataHub environments
    datahub_airflow_plugin = None


class DataHubPlugin(AirflowPlugin):
    name = "datahub_plugin"
