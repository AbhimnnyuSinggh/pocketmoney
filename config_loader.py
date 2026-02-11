"""
config_loader.py — Loads and validates YAML configuration.
"""
import yaml
import os
import sys


def load_config(path: str = "config.yaml") -> dict:
    """Load config from YAML file, with env-var overrides for secrets."""
    if not os.path.exists(path):
        print(f"[ERROR] Config file not found: {path}")
        sys.exit(1)

    with open(path, "r") as f:
        cfg = yaml.safe_load(f)

    # Allow environment variable overrides for sensitive fields
    cfg["wallet"]["private_key"] = os.environ.get(
        "POLY_PRIVATE_KEY", cfg["wallet"].get("private_key", "")
    )
    cfg["wallet"]["funder_address"] = os.environ.get(
        "POLY_FUNDER_ADDRESS", cfg["wallet"].get("funder_address", "")
    )
    cfg["telegram"]["bot_token"] = os.environ.get(
        "TELEGRAM_BOT_TOKEN", cfg["telegram"].get("bot_token", "")
    )
    cfg["telegram"]["chat_id"] = os.environ.get(
        "TELEGRAM_CHAT_ID", cfg["telegram"].get("chat_id", "")
    )
    return cfg
