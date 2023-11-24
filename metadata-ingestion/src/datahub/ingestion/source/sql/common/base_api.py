import logging
from typing import Dict, Iterable, List

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.source.sql.common.api import ExtractionInterface
from datahub.ingestion.source.sql.common.models import (
    BaseDatabase,
    BaseSchema,
    DatabaseIdentifier,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig

logger: logging.Logger = logging.getLogger(__name__)
TWO_TIER_PLACEHOLDER_DB = DatabaseIdentifier(None)


class BaseExtractionApi(ExtractionInterface, Closeable):
    def __init__(
        self, config: SQLAlchemyConnectionConfig, is_two_tier_hierachy: bool = False
    ) -> None:
        self.config = config
        self.IS_TWO_TIER_PLATFORM = is_two_tier_hierachy

        # non-default db inspectors cache
        self._inspector_cache: Dict[DatabaseIdentifier, Inspector] = {}

    def get_databases(self) -> List[BaseDatabase]:
        if not self.IS_TWO_TIER_PLATFORM and self.config.database:
            return [BaseDatabase(self.config.database)]
        raise NotImplementedError()

    def get_schemas(self, db: DatabaseIdentifier) -> List[BaseSchema]:
        inspector = self._get_inspector(db)
        return list(self._get_schemas(inspector))

    def _get_inspector(self, db: DatabaseIdentifier) -> Inspector:
        if db in self._inspector_cache:
            return self._inspector_cache[db]
        else:
            url = self.config.get_sql_alchemy_url(database=db.database_name)
            logger.debug(f"sql_alchemy_url={url}")
            engine = create_engine(url, **self.config.options)
            with engine.connect() as conn:
                inspector = inspect(conn)
                return inspector

    def _get_schemas(self, inspector: Inspector) -> Iterable[BaseSchema]:
        for schema_name in inspector.get_schema_names():
            yield BaseSchema(schema_name)
