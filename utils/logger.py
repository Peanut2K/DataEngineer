"""
utils/logger.py
───────────────
Centralised logging factory.
Each module gets its own named logger with:
  - Console handler  (human-readable output)
  - File handler     (daily log file in logs/)
"""

import logging
import os
from datetime import datetime

from config.settings import LOG_LEVEL, LOG_DIR


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger instance.

    Usage:
        from utils.logger import get_logger
        logger = get_logger("my_module")
        logger.info("Hello, pipeline!")
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    # Avoid adding duplicate handlers if the logger is reused
    if logger.handlers:
        return logger

    # ── Formatter ─────────────────────────────────────────────────────────────
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(name)-25s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Console Handler ───────────────────────────────────────────────────────
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # ── File Handler (daily rotation by naming convention) ────────────────────
    today       = datetime.now().strftime("%Y-%m-%d")
    log_file    = os.path.join(LOG_DIR, f"{today}_{name}.log")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
