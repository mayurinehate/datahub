from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List, Optional

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler

from datahub.ingestion.source.sql.common.models import (
    BaseColumn,
    BaseSchema,
    BaseTable,
    BaseView,
    DatabaseIdentifier,
    SchemaIdentifier,
    TableIdentifier,
)


@dataclass
class TableDetail:
    comment: Optional[str] = None
    properties: Optional[dict] = None
    location: Optional[str] = None


class ExtractionInterface(ABCMeta):
    """Sql common metadata extraction interface"""

    @abstractmethod
    def get_databases(self) -> List[DatabaseIdentifier]:
        pass

    @abstractmethod
    def get_schemas(self, db: DatabaseIdentifier) -> Iterable[BaseSchema]:
        pass

    @abstractmethod
    def get_tables(self, schema: SchemaIdentifier) -> Iterable[BaseTable]:
        pass

    @abstractmethod
    def get_views(self, schema: SchemaIdentifier) -> Iterable[BaseView]:
        pass

    @abstractmethod
    def get_table_columns(self, table: TableIdentifier) -> Iterable[BaseColumn]:
        pass

    @abstractmethod
    def get_profiler_instance(self, db: DatabaseIdentifier) -> "DatahubGEProfiler":
        pass

    @abstractmethod
    def get_table_details(self, table: TableIdentifier) -> TableDetail:
        pass

    @abstractmethod
    def get_pk_constraint(self, table: TableIdentifier) -> dict:
        pass

    @abstractmethod
    def get_foreign_keys(self, table: TableIdentifier) -> List[dict]:
        pass

    @abstractmethod
    def get_view_definition(self, view: TableIdentifier) -> Optional[str]:
        pass
