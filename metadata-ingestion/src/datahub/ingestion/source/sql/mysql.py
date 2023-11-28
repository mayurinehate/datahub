# This import verifies that the dependencies are available.

import logging
from typing import Iterable

import pymysql  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import create_engine, util
from sqlalchemy.dialects.mysql import base
from sqlalchemy.dialects.mysql.enumerated import SET
from sqlalchemy.engine.row import LegacyRow

from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import (
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)

SET.__repr__ = util.generic_repr  # type:ignore

GEOMETRY = make_sqlalchemy_type("GEOMETRY")
POINT = make_sqlalchemy_type("POINT")
LINESTRING = make_sqlalchemy_type("LINESTRING")
POLYGON = make_sqlalchemy_type("POLYGON")
DECIMAL128 = make_sqlalchemy_type("DECIMAL128")

register_custom_type(GEOMETRY)
register_custom_type(POINT)
register_custom_type(LINESTRING)
register_custom_type(POLYGON)
register_custom_type(DECIMAL128)

base.ischema_names["geometry"] = GEOMETRY
base.ischema_names["point"] = POINT
base.ischema_names["linestring"] = LINESTRING
base.ischema_names["polygon"] = POLYGON
base.ischema_names["decimal128"] = DECIMAL128

logger: logging.Logger = logging.getLogger(__name__)


class MySQLConnectionConfig(SQLAlchemyConnectionConfig):
    # defaults
    host_port: str = Field(default="localhost:3306", description="MySQL host URL.")
    scheme: str = "mysql+pymysql"


class MySQLConfig(MySQLConnectionConfig, TwoTierSQLAlchemyConfig):
    def get_identifier(self, *, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.database_alias:
            return f"{self.database_alias}.{table}"
        else:
            return regular


@platform_name("MySQL")
@config_class(MySQLConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class MySQLSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    Metadata for databases, schemas, and tables
    Column types and schema associated with each table
    Table, row, and column statistics via optional SQL profiling
    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, self.get_platform())

    def get_platform(self):
        return "mysql"

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def global_workunits_start_hook(self) -> Iterable[MetadataWorkUnit]:
        if self.config.is_profiling_enabled():
            url = self.config.get_sql_alchemy_url()
            logger.debug(f"sql_alchemy_url={url}")
            engine = create_engine(url, **self.config.options)
            with engine.connect() as conn:
                for row in conn.execute(
                    "SELECT table_schema, table_name, data_length from information_schema.tables"
                ):
                    if isinstance(row, LegacyRow):
                        schema = row[0]
                        table = row[1]
                        data_length = row[2]
                    else:
                        schema = row.table_schema
                        table = row.table_name
                        data_length = row.data_length
                    self.profile_metadata_info.dataset_name_to_storage_bytes[
                        f"{schema}.{table}"
                    ] = data_length
            yield from []
