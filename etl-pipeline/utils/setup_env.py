from functools import wraps
import os

from load_env import load_env_file


def setup_env(parser):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            parsed_args = parser.parse_args()

            load_env_file(parsed_args.environment, "./config/{}.yaml")
            os.environ["ENVIRONMENT"] = (
                os.environ.get("ENVIRONMENT") or parsed_args.environment
            )

            return func(*args, **kwargs)

        return wrapper

    return decorator
