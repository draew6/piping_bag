from typing import Union, Type, get_origin, Any
from types import UnionType
from pydantic import BaseModel, validate_call
import re
from .interfaces import Database


class Operation: ...


class Select(Operation): ...


class Insert(Operation): ...


class InsertOne(Insert): ...


class InsertMany(Insert): ...


class Update(Operation): ...

class UpdateMany(Update): ...

class Delete(Operation): ...


class In:
    usable_in = [Select, Update, Delete]


class Returning:
    usable_in = [Insert, Update, Delete]


class ReturnType:
    model: Type
    is_adaptive: bool
    is_list: bool
    is_optional: bool
    ADAPTIVE_CLASSES = [BaseModel, dict]

    def __init__(self, type_hint: Type):
        model = type_hint
        self.is_list = model is list or get_origin(model) is list
        if self.is_list:
            model = model.__args__[0]
        if (
            get_origin(model) is Union or get_origin(model) is UnionType
        ) and isinstance(None, model.__args__[1]):
            model = model.__args__[0]
            self.is_optional = True
        else:
            self.is_optional = False
        self.is_adaptive = (
            any(issubclass(model, c) for c in self.ADAPTIVE_CLASSES) if model else False
        )
        self.model = model


class Parameter:
    name: str
    clean_name: str
    value: Any

    def __init__(self, name: str, value: Any):
        if name.startswith("$"):
            self.name = name
            self.clean_name = name.strip("$")
        else:
            self.name = f"${name}"
            self.clean_name = name
        self.value = value

    def __eq__(self, other):
        if isinstance(other, str):
            if other.startswith("$"):
                return self.name == other
            else:
                return self.clean_name == other
        elif isinstance(other, Parameter):
            return self.name == other.name
        else:
            return False


Array = list[int]
Data = list[tuple]


class ArrayParameter(Parameter):
    value: Array

    @validate_call
    def __init__(self, name: str, value: Array):
        super().__init__(name, value)


class Query:
    operation: Type[Operation]
    options: list[Type[In] | Type[Returning]]
    return_type: ReturnType
    raw: str
    modified: str
    schema: str | None
    all_params: list[Parameter] = []
    params: list[Parameter] = []
    in_params: list[Parameter] = []
    values: dict = {}
    data: list = []

    def __init__(
        self, sql_query: str, type_hint: Type, values: dict, schema=None, data=None
    ):
        self.raw = sql_query.strip()
        self.modified = self.raw
        self.values = values
        self.schema = schema
        self.in_params = []
        if data:
            self.data = data
        self.extract_operation()
        self.parse_all_params()
        self.extract_options()
        self.parse_params()
        self.return_type = ReturnType(type_hint)
        self.standardize()
        self.set_schema()

    def extract_operation(self):
        raw_lower = self.raw.lower()
        # Extracting the operation
        if raw_lower.startswith("select"):
            self.operation = Select
        elif raw_lower.startswith("insert") and self.data:
            self.operation = InsertMany
        elif raw_lower.startswith("insert"):
            self.operation = Insert
        elif raw_lower.startswith("update") and self.data:
            self.operation = UpdateMany
        elif raw_lower.startswith("update"):
            self.operation = Update
        elif raw_lower.startswith("delete"):
            self.operation = Delete
        else:
            raise ValueError(f"Invalid SQL query: {self.raw}")

    def extract_options(self):
        raw_lower = self.raw.lower()

        self.options = []
        if "returning" in raw_lower:
            assert self.operation in Returning.usable_in
            self.options.append(Returning)
        in_sentences = re.findall(r"\b\w+\s+in\s+\$\w+\b", raw_lower)
        if in_sentences:
            assert self.operation in In.usable_in
            self.options.append(In)
            for in_sentence in in_sentences:
                param = in_sentence.split("$")[1].strip()
                in_param = ArrayParameter(param, self.values[param])
                assert in_param in self.all_params
                self.in_params.append(in_param)

    def set_schema(self):
        if self.schema is not None:
            self.modified = (
                self.modified.replace("FROM ", f'FROM "{self.schema}".')
                .replace("JOIN ", f'JOIN "{self.schema}".')
                .replace("INSERT INTO ", f'INSERT INTO "{self.schema}".')
            )
            if self.operation is Update:
                self.modified = self.modified.replace(
                    "UPDATE ", f'UPDATE "{self.schema}".'
                )

    def parse_all_params(self):
        self.all_params = [
            Parameter(param, self.values[param.strip("$")])
            for param in dict.fromkeys(re.findall(r"\$\w+", self.modified))
            if param.strip("$") in self.values
        ]

    def parse_params(self):
        self.params = [
            param for param in self.all_params if param not in self.in_params
        ]

    def standardize(self):
        if self.operation in [Select, Update, Delete]:
            for in_param in self.in_params:
                self.modified = self.modified.replace(
                    in_param.name,
                    f"({','.join([str(value) for value in in_param.value])})",
                )

        for index, param in enumerate(self.params, start=1):
            self.modified = self.modified.replace(param.name, f"${index}")

    async def send(self, db: Database):
        if self.operation in [InsertMany, UpdateMany]:
            result = await db.execute_many(self.modified, self.data)
        elif self.return_type.is_list and self.operation is Select:
            rows = await db.fetch_many(self.modified, *[p.value for p in self.params])
            if rows is None:
                result = None
            elif not self.return_type.is_adaptive:
                result = [row[0] for row in rows]
            else:
                result = [self.return_type.model(**dict(row)) for row in rows]
        elif not self.return_type.is_list and (
            self.operation is Select or Returning in self.options
        ):
            rows = await db.fetch_one(self.modified, *[p.value for p in self.params])
            if rows is None:
                result = None
            elif not self.return_type.is_adaptive:
                result = rows[0]
            else:
                result = self.return_type.model(**dict(rows))
        elif self.operation not in [Select, Update, Delete] and In in self.options:
            result = await db.execute_many(
                self.modified, *[p.value for p in self.params]
            )
            result = None
        else:
            result = await db.execute(self.modified, *[p.value for p in self.params])
            result = None
        return result
