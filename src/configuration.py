from enum import Enum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, computed_field


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class Endpoint(BaseModel):
    endpoint: str
    columns: list[str] = Field(default_factory=list)
    table_name: str = ""
    primary_key: list[str] = ["id"]
    incremental_field: str = "id"
    initial_since: str = ""


class Destination(BaseModel):
    load_type: LoadType = Field(default=LoadType.incremental_load)

    @computed_field
    @property
    def incremental(self) -> bool:
        return self.load_type == LoadType.incremental_load


class Configuration(BaseModel):
    endpoints: list[Endpoint] = Field(default_factory=list)
    destination: Destination = Field(default_factory=Destination)
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
