import newrelic.agent
from functools import wraps

app = newrelic.agent.register_application(timeout=10.0)


def with_logging(name=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if app:
                task_name = name or func.__qualname__

                with newrelic.agent.BackgroundTask(app, name=task_name):
                    return func(*args, **kwargs)

            return func(*args, **kwargs)

        return wrapper

    return decorator
