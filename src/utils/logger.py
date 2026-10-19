"""Logging utility for RetailFlow Data Engineering pipeline."""

import logging
import sys
from pathlib import Path


def get_logger(name: str = "retailflow", log_level: str = "INFO") -> logging.Logger:
    """Configures and returns a standardized logger with console and optional file handlers.

    Args:
        name: Name of the logger, typically __name__ or module identifier.
        log_level: Minimum severity level to log (DEBUG, INFO, WARNING, ERROR, CRITICAL).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        # Format with timestamp, level, module, line number and message
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Standard console handler (stdout)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File handler (logs directory)
        log_dir = Path("logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "retailflow.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
