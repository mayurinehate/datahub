import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, cast

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine.row import LegacyRow
from sqlalchemy.exc import ProgrammingError

from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.source.sql.common.api import ExtractionInterface, TableDetail
from datahub.ingestion.source.sql.common.models import (
    BaseColumn,
    BaseSchema,
    BaseTable,
    BaseView,
    DatabaseIdentifier,
    SchemaIdentifier,
    TableIdentifier,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLCommonConfig,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler

logger: logging.Logger = logging.getLogger(__name__)


TWO_TIER_PLACEHOLDER_DB = DatabaseIdentifier(None)


class BaseExtractionApi(ExtractionInterface, Closeable):
    def __init__(
        self, config: SQLCommonConfig, is_two_tier_hierachy: bool = False
    ) -> None:
        self.config = config
        self.IS_TWO_TIER_PLATFORM = is_two_tier_hierachy

        # non-default db inspectors cache
        # TODO: max cache size = 1 ?
        self._inspector_cache: Dict[DatabaseIdentifier, Inspector] = {}

    def get_databases(self) -> List[DatabaseIdentifier]:
        if self.IS_TWO_TIER_PLATFORM:
            # For a platform with 2 tier representation, it is okay to not implement this,
            # as inspector.get_schema_names can be used to get list of top-level containers (i.e. schemas).
            return [TWO_TIER_PLACEHOLDER_DB]

        if isinstance(self.config, BasicSQLAlchemyConfig):
            config = cast(BasicSQLAlchemyConfig, self.config)
            if config.database:
                return [DatabaseIdentifier(config.database)]

        # This method must be implemented for a platform with 3 tier representation,
        # unless ingestion is for a particular database only.

        raise NotImplementedError()

    def get_schemas(self, db: DatabaseIdentifier) -> Iterable[BaseSchema]:
        if self.IS_TWO_TIER_PLATFORM and isinstance(self.config, BasicSQLAlchemyConfig):
            config = cast(BasicSQLAlchemyConfig, self.config)
            if config.database:
                yield from [BaseSchema(config.database)]
                return

        inspector = self._get_inspector(db)
        for schema in inspector.get_schema_names():
            yield self._get_schema(SchemaIdentifier(schema, db))

    def get_tables(self, schema: SchemaIdentifier) -> Iterable[BaseTable]:
        inspector = self._get_inspector(schema.database_id)
        for table_name in inspector.get_table_names(schema.schema_name):
            yield self._get_table(TableIdentifier(table_name, schema))

    def get_views(self, schema: SchemaIdentifier) -> Iterable[BaseView]:
        inspector = self._get_inspector(schema.database_id)
        for table_name in inspector.get_view_names(schema.schema_name):
            yield self._get_view(TableIdentifier(table_name, schema))

    def get_table_columns(self, table: TableIdentifier) -> Iterable[BaseColumn]:
        inspector = self._get_inspector(table.schema_id.database_id)
        for column in inspector.get_columns(
            table.table_name, table.schema_id.schema_name
        ):
            yield self._get_column(column)

    def get_table_details(self, table: TableIdentifier) -> TableDetail:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            inspector = self._get_inspector(table.schema_id.database_id)
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_table_comment(table.table_name, table.schema_id.schema_name)  # type: ignore
        except NotImplementedError:
            return TableDetail()
        except ProgrammingError as pe:
            # Snowflake needs schema names quoted when fetching table comments.
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {table.schema_id.schema_name} and table {table.table_name}",
                pe,
            )
            table_info: dict = inspector.get_table_comment(table, f'"{table.schema_id.schema_name}"')  # type: ignore

        description = table_info.get("text")
        if isinstance(description, LegacyRow):
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return TableDetail(description, properties, location)

    def _get_column(self, column: dict) -> BaseColumn:
        return BaseColumn(
            name=column["name"],
            ordinal_position=-1,  # TODO
            data_type=column.get("full_type", repr(column["type"])),
            comment=column.get("comment", None),
            is_nullable=column["nullable"],
            sqlalchemy_type=column["type"],
        )

    def _get_inspector(self, db: DatabaseIdentifier) -> Inspector:
        if db in self._inspector_cache:
            return self._inspector_cache[db]
        else:
            url = self.config.get_sql_alchemy_url(database=db.database_name)
            logger.debug(f"sql_alchemy_url={url}")
            engine = create_engine(url, **self.config.options)
            conn = engine.connect()
            inspector = inspect(conn)
            self._inspector_cache[db] = inspector
            return inspector

    def _get_schema(self, schema: SchemaIdentifier) -> BaseSchema:
        return BaseSchema(schema.schema_name)

    def _get_table(self, table: TableIdentifier) -> BaseTable:
        detail = self.get_table_details(table)
        return BaseTable(name=table.table_name, comment=detail.comment)

    def _get_view(self, table: TableIdentifier) -> BaseView:
        detail = self.get_table_details(table)
        return BaseView(name=table.table_name, comment=detail.comment)

    def close(self) -> None:
        super().close()
        for inspector in self._inspector_cache.values():
            inspector.engine.dispose()

    def get_profiler_instance(
        self, db: DatabaseIdentifier, report: SQLSourceReport, platform: str
    ) -> "DatahubGEProfiler":
        from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler

        return DatahubGEProfiler(
            conn=self._get_inspector(db).bind,
            report=report,
            config=self.config.profiling,
            platform=platform,
        )

    def get_pk_constraint(self, table: TableIdentifier) -> dict:
        inspector = self._get_inspector(table.schema_id.database_id)
        return inspector.get_pk_constraint(
            table.table_name, table.schema_id.schema_name
        )

    def get_foreign_keys(self, table: TableIdentifier) -> List[Dict[str, Any]]:
        inspector = self._get_inspector(table.schema_id.database_id)
        return inspector.get_foreign_keys(table.table_name, table.schema_id.schema_name)

    def get_view_definition(self, view: TableIdentifier) -> Optional[str]:
        inspector = self._get_inspector(view.schema_id.database_id)
        return inspector.get_view_definition(
            view.table_name, view.schema_id.schema_name
        )
