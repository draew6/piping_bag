from typing import Callable, get_type_hints
from functools import wraps
import inspect
from .interfaces import Database
from .models import Query, Data


def query[T](sql_query: str):
    def decorator(func: Callable[..., list[T]]):
        @wraps(func)
        async def wrapper(*args, **kwargs) -> list[T]:
            function_parameters = inspect.signature(func).parameters
            data_parameters = {
                name: value
                for name, value in function_parameters.items()
                if value.annotation is Data
            }
            function_parameter_names = [
                parameter
                for parameter in function_parameters.keys()
                if parameter != "self" and parameter not in kwargs
            ]
            function_parameter_values = args[1:]
            parameters = (
                dict(zip(function_parameter_names, function_parameter_values)) | kwargs
            )
            database: Database = args[0].db
            if data_parameters:
                data = parameters.get(list(data_parameters.keys())[0], None)
            else:
                data = None
            q = Query(
                sql_query,
                get_type_hints(func).get("return"),
                {k: v for k, v in parameters.items() if k not in data_parameters},
                args[0].schema,
                data,
            )
            result = await q.send(database)
            return result

        return wrapper

    return decorator
