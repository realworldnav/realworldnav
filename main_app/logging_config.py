"""
Logging configuration for RealWorldNAV.
Supports normal mode (concise) and debug mode (verbose with file output).
"""
import logging
import sys
import os
from pathlib import Path

# Debug mode: set DECODER_DEBUG=1 to enable verbose decoder logging
DECODER_DEBUG = os.getenv('DECODER_DEBUG', '').lower() in ('1', 'true', 'yes')

# Debug log file path
DEBUG_LOG_PATH = Path(__file__).parent.parent / 'decoder_debug.log'


# Custom formatter for concise output
class ConciseFormatter(logging.Formatter):
    """Single-line, concise log format."""

    FORMATS = {
        logging.DEBUG: "\033[90m[D]\033[0m %(name)s: %(message)s",
        logging.INFO: "\033[32m[I]\033[0m %(message)s",
        logging.WARNING: "\033[33m[W]\033[0m %(message)s",
        logging.ERROR: "\033[31m[E]\033[0m %(name)s: %(message)s",
        logging.CRITICAL: "\033[31;1m[!]\033[0m %(name)s: %(message)s",
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno, self.FORMATS[logging.INFO])
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class VerboseFormatter(logging.Formatter):
    """Detailed format for debug file logging."""
    def __init__(self):
        super().__init__(
            fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )


def setup_logging(level=logging.INFO):
    """
    Configure logging for the entire application.
    Call this once at startup before any other imports.

    Set DECODER_DEBUG=1 to enable verbose decoder logging to file.
    """
    # Silence noisy third-party loggers
    noisy_loggers = [
        'botocore', 'boto3', 'urllib3', 's3transfer',
        'websockets', 'asyncio', 'httpcore', 'httpx',
        'web3', 'web3.providers', 'web3.RequestManager',
    ]
    for logger_name in noisy_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # Configure root logger
    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers
    root.handlers.clear()

    # Add concise console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ConciseFormatter())
    root.addHandler(handler)

    # App logger at INFO level
    app_logger = logging.getLogger('realworldnav')
    app_logger.setLevel(level)

    # === DEBUG MODE: Enable verbose decoder logging ===
    if DECODER_DEBUG:
        setup_decoder_debug_logging()
        app_logger.info(f"DECODER_DEBUG enabled - verbose logs written to {DEBUG_LOG_PATH}")

    return app_logger


def setup_decoder_debug_logging():
    """
    Set up verbose debug logging for decoder modules.
    Writes detailed logs to decoder_debug.log file.
    """
    # Create file handler for decoder debug logs
    file_handler = logging.FileHandler(DEBUG_LOG_PATH, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(VerboseFormatter())
    file_handler.name = 'decoder_debug_file'  # Tag for identification

    # Only add to the parent logger to avoid duplicates
    # The parent 'main_app.services.decoders' will catch all child logger messages
    parent_logger = logging.getLogger('main_app.services.decoders')
    parent_logger.setLevel(logging.DEBUG)
    if not any(getattr(h, 'name', None) == 'decoder_debug_file' for h in parent_logger.handlers):
        parent_logger.addHandler(file_handler)

    # Also add to __main__ for explore_tx.py
    main_logger = logging.getLogger('__main__')
    main_logger.setLevel(logging.DEBUG)
    if not any(getattr(h, 'name', None) == 'decoder_debug_file' for h in main_logger.handlers):
        main_logger.addHandler(file_handler)


def get_debug_logger(name: str = 'decoder') -> logging.Logger:
    """
    Get a debug-enabled logger for a decoder/debug script.

    Always logs to console AND to decoder_debug.log if DECODER_DEBUG is set.

    Usage:
        from main_app.logging_config import get_debug_logger
        logger = get_debug_logger(__name__)
        logger.debug("Detailed info...")  # Goes to file
        logger.info("Summary info...")    # Goes to console + file
    """
    logger = logging.getLogger(name)

    # Ensure we have at least a console handler (check by name)
    has_console = any(getattr(h, 'name', None) == 'console' for h in logger.handlers)
    if not has_console and not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(ConciseFormatter())
        console_handler.name = 'console'
        logger.addHandler(console_handler)

    # If debug mode, add file handler (only if not already added)
    if DECODER_DEBUG:
        has_file = any(getattr(h, 'name', None) == 'debug_file' for h in logger.handlers)
        if not has_file:
            file_handler = logging.FileHandler(DEBUG_LOG_PATH, mode='a', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(VerboseFormatter())
            file_handler.name = 'debug_file'
            logger.addHandler(file_handler)
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    return logger


# Convenience function for modules to get their logger
def get_logger(name: str) -> logging.Logger:
    """Get a logger for a module. Use: logger = get_logger(__name__)"""
    return logging.getLogger(f'realworldnav.{name}')
