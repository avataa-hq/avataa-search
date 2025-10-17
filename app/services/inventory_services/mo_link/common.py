import time
from functools import wraps


def async_timing_decorator(enable: bool = False):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if enable:
                start_time = time.perf_counter()
                result = await func(*args, **kwargs)
                end_time = time.perf_counter()
                print(
                    f"Execution time: {func.__name__}: {end_time - start_time:.4f} seconds"
                )
                return result
            return await func(*args, **kwargs)

        return wrapper

    return decorator


def timing_decorator(enable: bool = False):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if enable:
                start_time = time.perf_counter()
                result = func(*args, **kwargs)
                end_time = time.perf_counter()
                print(
                    f"Execution time: {func.__name__}: {end_time - start_time:.4f} seconds"
                )
                return result
            return func(*args, **kwargs)

        return wrapper

    return decorator
