from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


# identifier for Database
@dataclass(frozen=True)
class DatabaseIdentifier:
    database_name: Optional[str]


# identifier for Schema
@dataclass(frozen=True)
class SchemaIdentifier:
    schema_name: str
    database_id: DatabaseIdentifier


# identifier for Table
@dataclass(frozen=True)
class TableIdentifier:
    table_name: str
    schema_id: SchemaIdentifier

    @classmethod
    def from_names(
        cls, table_name: str, schema_name: str, database_name: Optional[str]
    ) -> "TableIdentifier":
        return TableIdentifier(
            table_name=table_name,
            schema_id=SchemaIdentifier(
                schema_name=schema_name,
                database_id=DatabaseIdentifier(database_name=database_name),
            ),
        )


# dataclass containing schema basic details
@dataclass
class BaseSchema:
    name: str
    comment: Optional[str] = None
    created: Optional[datetime] = None
    last_altered: Optional[datetime] = None

    def id(self, database_id: DatabaseIdentifier) -> SchemaIdentifier:
        return SchemaIdentifier(schema_name=self.name, database_id=database_id)


# dataclass containing database basic details
@dataclass
class BaseDatabase:
    name: str
    created: Optional[datetime] = None
    comment: Optional[str] = None
    last_altered: Optional[datetime] = None

    def id(self) -> DatabaseIdentifier:
        return DatabaseIdentifier(database_name=self.name)


# dataclass containing column basic details
@dataclass
class BaseColumn:
    name: str
    ordinal_position: int
    is_nullable: bool
    data_type: str
    comment: Optional[str]
    sqlalchemy_type: Any  # Not clean


# dataclass containing table basic details
@dataclass
class BaseTable:
    name: str
    comment: Optional[str]
    created: Optional[datetime] = None
    last_altered: Optional[datetime] = None
    size_in_bytes: Optional[int] = None
    rows_count: Optional[int] = None
    column_count: Optional[int] = None
    ddl: Optional[str] = None

    def id(self, schema_id: SchemaIdentifier) -> TableIdentifier:
        return TableIdentifier(table_name=self.name, schema_id=schema_id)


# dataclass containing view basic details
@dataclass
class BaseView:
    name: str
    comment: Optional[str]
    created: Optional[datetime] = None
    last_altered: Optional[datetime] = None
    view_definition: Optional[str] = None
    size_in_bytes: Optional[int] = None
    rows_count: Optional[int] = None
    column_count: Optional[int] = None

    def id(self, schema_id: SchemaIdentifier) -> TableIdentifier:
        return TableIdentifier(table_name=self.name, schema_id=schema_id)
