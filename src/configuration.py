from enum import Enum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field, ValidationError, computed_field


class Authorization(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    client_id: str = Field(alias="#client_id", default="")
    client_secret: str = Field(alias="#client_secret", default="")
    username: str = ""


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
    authorization: Authorization = Field(default_factory=Authorization)
    endpoint: str = ""
    columns: list[str] = Field(default_factory=list)
    table_name: str = ""
    primary_key: list[str] = ["id"]
    incremental_field: str = "id"
    initial_since: str = ""
    locations: list[str] = Field(default_factory=list)
    destination: Destination = Field(default_factory=Destination)
    batch_size: int = 1000
    debug: bool = False

    @computed_field
    @property
    def endpoints(self) -> list[Endpoint]:
        if not self.endpoint:
            return []
        return [Endpoint(
            endpoint=self.endpoint,
            columns=self.columns,
            table_name=self.table_name,
            primary_key=self.primary_key,
            incremental_field=self.incremental_field,
            initial_since=self.initial_since,
        )]

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
