"""
Concise logging configuration for RealWorldNAV.
Only shows important messages, not verbose debug spam.
"""
import logging
import sys

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


def setup_logging(level=logging.INFO):
    """
    Configure logging for the entire application.
    Call this once at startup before any other imports.
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

    return app_logger


# Convenience function for modules to get their logger
def get_logger(name: str) -> logging.Logger:
    """Get a logger for a module. Use: logger = get_logger(__name__)"""
    return logging.getLogger(f'realworldnav.{name}')
