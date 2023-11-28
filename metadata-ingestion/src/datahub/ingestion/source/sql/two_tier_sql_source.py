from typing import Optional, Tuple

from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig


class TwoTierSQLAlchemyConfig(BasicSQLAlchemyConfig):
    schema_pattern: AllowDenyPattern = Field(
        # The superclass contains a `schema_pattern` field, so we need this here
        # to override the documentation.
        default=AllowDenyPattern.allow_all(),
        description="Deprecated in favour of database_pattern.",
    )
    # TODO: Let's allow using schema_pattern and database_pattern interchangeably for a 2-tier source

    _schema_pattern_deprecated = pydantic_renamed_field(
        "schema_pattern", "database_pattern"
    )


class TwoTierSQLAlchemySource(SQLAlchemySource):
    def __init__(self, config, ctx, platform):
        super().__init__(config, ctx, platform)
        self.config: TwoTierSQLAlchemyConfig = config

    def get_db_schema(self, dataset_identifier: str) -> Tuple[Optional[str], str]:
        schema, _view = dataset_identifier.split(".", 1)
        return None, schema
