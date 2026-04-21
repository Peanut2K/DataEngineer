"""
utils/retry.py
──────────────
Decorator-based retry mechanism with exponential back-off logging.

Usage:
    from utils.retry import with_retry

    @with_retry(max_retries=3, delay=5)
    def call_external_api():
        ...
"""

import time
import functools
from typing import Tuple, Type

from utils.logger import get_logger

logger = get_logger("retry")


def with_retry(
    max_retries: int = 3,
    delay: int = 5,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """
    Decorator that retries the wrapped function on failure.

    Args:
        max_retries:  Maximum number of attempts (including the first).
        delay:        Seconds to wait between attempts.
        exceptions:   Tuple of exception types that trigger a retry.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_retries:
                        logger.error(
                            f"[{func.__name__}] All {max_retries} attempts failed. "
                            f"Last error: {exc}"
                        )
                        raise
                    logger.warning(
                        f"[{func.__name__}] Attempt {attempt}/{max_retries} failed: {exc}. "
                        f"Retrying in {delay}s …"
                    )
                    time.sleep(delay)

        return wrapper

    return decorator
