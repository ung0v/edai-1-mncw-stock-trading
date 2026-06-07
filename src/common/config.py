from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config.yaml"


def load_config(path: str | Path = DEFAULT_CONFIG_PATH):
    with open(path, "r") as f:
        return yaml.safe_load(f)
