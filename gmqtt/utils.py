import asyncio
import logging

logger = logging.getLogger(__name__)


def cancelable(func):
    async def wrapper(*args, **kwargs):
        try:
            result = await func(*args, **kwargs)
        except asyncio.CancelledError as exc:
            logger.debug('[CANCEL TASK] %s', func.__name__)
            return None
        return result

    return wrapper


def catch_exceptions(func):
    async def _wrapper(*args, **kwargs):
        try:
            result = await func(*args, **kwargs)
        except Exception as exc:
            logger.warning('[EXCEPTION] %s', func.__name__, exc_info=exc)
            result = None
        return result
    return _wrapper