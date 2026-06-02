# config.py
from pathlib import Path

import yaml


def load_config(path: str = "config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)
