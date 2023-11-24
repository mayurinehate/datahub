from abc import ABCMeta, abstractmethod
from typing import List

from datahub.ingestion.source.sql.common.models import (
    BaseColumn,
    BaseDatabase,
    BaseSchema,
    BaseTable,
    BaseView,
    DatabaseIdentifier,
    SchemaIdentifier,
    TableIdentifier,
)


class ExtractionInterface(ABCMeta):
    """Sql common metadata extraction interface"""

    @abstractmethod
    def get_databases(self) -> List[BaseDatabase]:
        pass

    @abstractmethod
    def get_schemas(self, db: DatabaseIdentifier) -> List[BaseSchema]:
        pass

    @abstractmethod
    def get_tables(self, schema: SchemaIdentifier) -> List[BaseTable]:
        pass

    @abstractmethod
    def get_view(self, schema: SchemaIdentifier) -> List[BaseView]:
        pass

    @abstractmethod
    def get_table_columns(self, table: TableIdentifier) -> List[BaseColumn]:
        pass
