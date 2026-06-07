from pathlib import Path

from src.common.config import load_config

BASE_DIR = Path(__file__).resolve().parents[2]
PROJECT_DIR = BASE_DIR
CONFIG_PATH = PROJECT_DIR / "config.yaml"
DATA_ROOT_DIR = PROJECT_DIR / "data"
DATA_DIR = DATA_ROOT_DIR / "raw"
OFFLINE_DIR = DATA_DIR / "offline"
STREAM_DIR = DATA_ROOT_DIR / "stream"
STREAM_EVENTS_PATH = STREAM_DIR / "trading_events" / "events.jsonl"
OUTPUTS_DIR = PROJECT_DIR / "outputs"
REPORTS_DIR = OUTPUTS_DIR / "reports"
SAMPLES_DIR = OUTPUTS_DIR / "samples"
CHARTS_DIR = OUTPUTS_DIR / "charts"
RUN_LOGS_DIR = OUTPUTS_DIR / "run_logs"


def _normalize_prefix(prefix: str) -> str:
    return prefix.strip("/")


def get_storage_config():
    return load_config(CONFIG_PATH)["storage"]


def get_storage_uri(layer: str, table_name: str | None = None) -> str:
    storage_cfg = get_storage_config()
    bucket = storage_cfg["bucket"]
    prefix = _normalize_prefix(storage_cfg[f"{layer}_prefix"])
    base_uri = f"s3a://{bucket}/{prefix}"
    if table_name:
        return f"{base_uri}/{table_name}"
    return base_uri
