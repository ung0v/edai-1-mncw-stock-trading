from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parents[1]
CONFIG_PATH = BASE_DIR / "config.yaml"
DATA_DIR = PROJECT_DIR / "data" / "raw"
OFFLINE_DIR = DATA_DIR / "offline"
STREAM_DIR = DATA_DIR / "stream"
STREAM_EVENTS_PATH = STREAM_DIR / "trading_events" / "events.jsonl"
OUTPUTS_DIR = PROJECT_DIR / "outputs"
REPORTS_DIR = OUTPUTS_DIR / "reports"
SAMPLES_DIR = OUTPUTS_DIR / "samples"
CHARTS_DIR = OUTPUTS_DIR / "charts"
