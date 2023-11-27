import logging
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set, cast

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pydantic import Field, SecretStr, root_validator, validator
from snowflake.connector.network import (
    DEFAULT_AUTHENTICATOR,
    EXTERNAL_BROWSER_AUTHENTICATOR,
    KEY_PAIR_AUTHENTICATOR,
    OAUTH_AUTHENTICATOR,
)

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.oauth import OAuthConfiguration, OAuthIdentityProvider
from datahub.configuration.pattern_utils import UUID_REGEX
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.source.snowflake.constants import (
    CLIENT_PREFETCH_THREADS,
    CLIENT_SESSION_KEEP_ALIVE,
)
from datahub.ingestion.source.sql.oauth_generator import OAuthTokenGenerator
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig, make_sqlalchemy_uri
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulLineageConfigMixin,
    StatefulProfilingConfigMixin,
    StatefulUsageConfigMixin,
)
from datahub.ingestion.source_config.usage.snowflake_usage import SnowflakeUsageConfig
from datahub.utilities.config_clean import (
    remove_protocol,
    remove_suffix,
    remove_trailing_slashes,
)
from datahub.utilities.global_warning_util import add_global_warning

logger = logging.Logger(__name__)

# FIVETRAN creates temporary tables in schema named FIVETRAN_xxx_STAGING.
# Ref - https://support.fivetran.com/hc/en-us/articles/1500003507122-Why-Is-There-an-Empty-Schema-Named-Fivetran-staging-in-the-Destination-
#
# DBT incremental models create temporary tables ending with __dbt_tmp
# Ref - https://discourse.getdbt.com/t/handling-bigquery-incremental-dbt-tmp-tables/7540
DEFAULT_TABLES_DENY_LIST = [
    r".*\.FIVETRAN_.*_STAGING\..*",  # fivetran
    r".*__DBT_TMP$",  # dbt
    rf".*\.SEGMENT_{UUID_REGEX}",  # segment
    rf".*\.STAGING_.*_{UUID_REGEX}",  # stitch
]

APPLICATION_NAME: str = "acryl_datahub"

VALID_AUTH_TYPES: Dict[str, str] = {
    "DEFAULT_AUTHENTICATOR": DEFAULT_AUTHENTICATOR,
    "EXTERNAL_BROWSER_AUTHENTICATOR": EXTERNAL_BROWSER_AUTHENTICATOR,
    "KEY_PAIR_AUTHENTICATOR": KEY_PAIR_AUTHENTICATOR,
    "OAUTH_AUTHENTICATOR": OAUTH_AUTHENTICATOR,
}

SNOWFLAKE_HOST_SUFFIX = ".snowflakecomputing.com"


class TagOption(str, Enum):
    with_lineage = "with_lineage"
    without_lineage = "without_lineage"
    skip = "skip"


@dataclass(frozen=True)
class DatabaseId:
    database: str = Field(
        description="Database created from share in consumer account."
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance of consumer snowflake account.",
    )


class SnowflakeShareConfig(ConfigModel):
    database: str = Field(description="Database from which share is created.")
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance for snowflake account in which share is created.",
    )

    consumers: Set[DatabaseId] = Field(
        description="List of databases created in consumer accounts."
    )

    @property
    def source_database(self) -> DatabaseId:
        return DatabaseId(self.database, self.platform_instance)


class SnowflakeConnectionConfig(ConfigModel):
    # Note: this config model is also used by the snowflake-usage source.

    options: dict = Field(
        default_factory=dict,
        description="Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.",
    )

    scheme: str = "snowflake"
    username: Optional[str] = Field(default=None, description="Snowflake username.")
    password: Optional[SecretStr] = Field(
        default=None, exclude=True, description="Snowflake password."
    )
    private_key: Optional[str] = Field(
        default=None,
        description="Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n' if using key pair authentication. Encrypted version of private key will be in a form of '-----BEGIN ENCRYPTED PRIVATE KEY-----\\nencrypted-private-key\\n-----END ENCRYPTED PRIVATE KEY-----\\n' See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html",
    )

    private_key_path: Optional[str] = Field(
        default=None,
        description="The path to the private key if using key pair authentication. Ignored if `private_key` is set. See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html",
    )
    private_key_password: Optional[SecretStr] = Field(
        default=None,
        exclude=True,
        description="Password for your private key. Required if using key pair authentication with encrypted private key.",
    )

    oauth_config: Optional[OAuthConfiguration] = Field(
        default=None,
        description="oauth configuration - https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth",
    )
    authentication_type: str = Field(
        default="DEFAULT_AUTHENTICATOR",
        description='The type of authenticator to use when connecting to Snowflake. Supports "DEFAULT_AUTHENTICATOR", "OAUTH_AUTHENTICATOR", "EXTERNAL_BROWSER_AUTHENTICATOR" and "KEY_PAIR_AUTHENTICATOR".',
    )
    account_id: str = Field(
        description="Snowflake account identifier. e.g. xy12345,  xy12345.us-east-2.aws, xy12345.us-central1.gcp, xy12345.central-us.azure, xy12345.us-west-2.privatelink. Refer [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#format-2-legacy-account-locator-in-a-region) for more details.",
    )
    warehouse: Optional[str] = Field(default=None, description="Snowflake warehouse.")
    role: Optional[str] = Field(default=None, description="Snowflake role.")
    connect_args: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Connect args to pass to Snowflake SqlAlchemy driver",
        exclude=True,
    )

    def get_account(self) -> str:
        assert self.account_id
        return self.account_id

    rename_host_port_to_account_id = pydantic_renamed_field("host_port", "account_id")

    @validator("account_id")
    def validate_account_id(cls, account_id: str) -> str:
        account_id = remove_protocol(account_id)
        account_id = remove_trailing_slashes(account_id)
        account_id = remove_suffix(account_id, SNOWFLAKE_HOST_SUFFIX)
        return account_id

    @validator("authentication_type", always=True)
    def authenticator_type_is_valid(cls, v, values, field):
        if v not in VALID_AUTH_TYPES.keys():
            raise ValueError(
                f"unsupported authenticator type '{v}' was provided,"
                f" use one of {list(VALID_AUTH_TYPES.keys())}"
            )
        if (
            values.get("private_key") is not None
            or values.get("private_key_path") is not None
        ) and v != "KEY_PAIR_AUTHENTICATOR":
            raise ValueError(
                f"Either `private_key` and `private_key_path` is set but `authentication_type` is {v}. "
                f"Should be set to 'KEY_PAIR_AUTHENTICATOR' when using key pair authentication"
            )
        if v == "KEY_PAIR_AUTHENTICATOR":
            # If we are using key pair auth, we need the private key path and password to be set
            if (
                values.get("private_key") is None
                and values.get("private_key_path") is None
            ):
                raise ValueError(
                    f"Both `private_key` and `private_key_path` are none. "
                    f"At least one should be set when using {v} authentication"
                )
        elif v == "OAUTH_AUTHENTICATOR":
            cls._check_oauth_config(values.get("oauth_config"))
        logger.info(f"using authenticator type '{v}'")
        return v

    @staticmethod
    def _check_oauth_config(oauth_config: Optional[OAuthConfiguration]) -> None:
        if oauth_config is None:
            raise ValueError(
                "'oauth_config' is none but should be set when using OAUTH_AUTHENTICATOR authentication"
            )
        if oauth_config.use_certificate is True:
            if oauth_config.provider == OAuthIdentityProvider.OKTA.value:
                raise ValueError(
                    "Certificate authentication is not supported for Okta."
                )
            if oauth_config.encoded_oauth_private_key is None:
                raise ValueError(
                    "'base64_encoded_oauth_private_key' was none "
                    "but should be set when using certificate for oauth_config"
                )
            if oauth_config.encoded_oauth_public_key is None:
                raise ValueError(
                    "'base64_encoded_oauth_public_key' was none"
                    "but should be set when using use_certificate true for oauth_config"
                )
        elif oauth_config.client_secret is None:
            raise ValueError(
                "'oauth_config.client_secret' was none "
                "but should be set when using use_certificate false for oauth_config"
            )

    def get_sql_alchemy_url(
        self,
        uri_opts: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[SecretStr] = None,
        role: Optional[str] = None,
    ) -> str:
        if username is None:
            username = self.username
        if password is None:
            password = self.password
        if role is None:
            role = self.role
        return make_sqlalchemy_uri(
            self.scheme,
            username,
            password.get_secret_value() if password else None,
            self.account_id,
            f'"{database}"' if database is not None else database,
            uri_opts={
                # Drop the options if value is None.
                key: value
                for (key, value) in {
                    "authenticator": VALID_AUTH_TYPES.get(self.authentication_type),
                    "warehouse": self.warehouse,
                    "role": role,
                    "application": APPLICATION_NAME,
                }.items()
                if value
            },
        )

    _computed_connect_args: Optional[dict] = None

    def get_connect_args(self) -> dict:
        """
        Builds connect args, adding defaults and reading a private key from the file if needed.
        Caches the results in a private instance variable to avoid reading the file multiple times.
        """

        if self._computed_connect_args is not None:
            return self._computed_connect_args

        connect_args: Dict[str, Any] = {
            # Improves performance and avoids timeout errors for larger query result
            CLIENT_PREFETCH_THREADS: 10,
            CLIENT_SESSION_KEEP_ALIVE: True,
            # Let user override the default config values
            **(self.connect_args or {}),
        }

        if (
            "private_key" not in connect_args
            and self.authentication_type == "KEY_PAIR_AUTHENTICATOR"
        ):
            if self.private_key is not None:
                pkey_bytes = self.private_key.replace("\\n", "\n").encode()
            else:
                assert (
                    self.private_key_path
                ), "missing required private key path to read key from"
                with open(self.private_key_path, "rb") as key:
                    pkey_bytes = key.read()

            p_key = serialization.load_pem_private_key(
                pkey_bytes,
                password=self.private_key_password.get_secret_value().encode()
                if self.private_key_password is not None
                else None,
                backend=default_backend(),
            )

            pkb: bytes = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            connect_args["private_key"] = pkb

        self._computed_connect_args = connect_args
        return connect_args

    def get_options(self) -> dict:
        options_connect_args: Dict = self.get_connect_args()
        options_connect_args.update(self.options.get("connect_args", {}))
        self.options["connect_args"] = options_connect_args
        return self.options

    def get_oauth_connection(self) -> snowflake.connector.SnowflakeConnection:
        assert (
            self.oauth_config
        ), "oauth_config should be provided if using oauth based authentication"
        generator = OAuthTokenGenerator(
            client_id=self.oauth_config.client_id,
            authority_url=self.oauth_config.authority_url,
            provider=self.oauth_config.provider,
            username=self.username,
            password=self.password,
        )
        if self.oauth_config.use_certificate:
            response = generator.get_token_with_certificate(
                private_key_content=str(self.oauth_config.encoded_oauth_public_key),
                public_key_content=str(self.oauth_config.encoded_oauth_private_key),
                scopes=self.oauth_config.scopes,
            )
        else:
            assert self.oauth_config.client_secret
            response = generator.get_token_with_secret(
                secret=str(self.oauth_config.client_secret.get_secret_value()),
                scopes=self.oauth_config.scopes,
            )
        try:
            token = response["access_token"]
        except KeyError:
            raise ValueError(
                f"access_token not found in response {response}. "
                "Please check your OAuth configuration."
            )
        connect_args = self.get_options()["connect_args"]
        return snowflake.connector.connect(
            user=self.username,
            account=self.account_id,
            token=token,
            role=self.role,
            warehouse=self.warehouse,
            authenticator=VALID_AUTH_TYPES.get(self.authentication_type),
            application=APPLICATION_NAME,
            **connect_args,
        )

    def get_key_pair_connection(self) -> snowflake.connector.SnowflakeConnection:
        connect_args = self.get_options()["connect_args"]

        return snowflake.connector.connect(
            user=self.username,
            account=self.account_id,
            warehouse=self.warehouse,
            role=self.role,
            authenticator=VALID_AUTH_TYPES.get(self.authentication_type),
            application=APPLICATION_NAME,
            **connect_args,
        )

    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        connect_args = self.get_options()["connect_args"]
        if self.authentication_type == "DEFAULT_AUTHENTICATOR":
            return snowflake.connector.connect(
                user=self.username,
                password=self.password.get_secret_value() if self.password else None,
                account=self.account_id,
                warehouse=self.warehouse,
                role=self.role,
                application=APPLICATION_NAME,
                **connect_args,
            )
        elif self.authentication_type == "OAUTH_AUTHENTICATOR":
            return self.get_oauth_connection()
        elif self.authentication_type == "KEY_PAIR_AUTHENTICATOR":
            return self.get_key_pair_connection()
        elif self.authentication_type == "EXTERNAL_BROWSER_AUTHENTICATOR":
            return snowflake.connector.connect(
                user=self.username,
                password=self.password.get_secret_value() if self.password else None,
                account=self.account_id,
                warehouse=self.warehouse,
                role=self.role,
                authenticator=VALID_AUTH_TYPES.get(self.authentication_type),
                application=APPLICATION_NAME,
                **connect_args,
            )
        else:
            # not expected to be here
            raise Exception("Not expected to be here.")


class SnowflakeV2Config(
    SQLCommonConfig,
    SnowflakeConnectionConfig,
    SnowflakeUsageConfig,
    StatefulLineageConfigMixin,
    StatefulUsageConfigMixin,
    StatefulProfilingConfigMixin,
    ClassificationSourceConfigMixin,
):
    include_table_lineage: bool = Field(
        default=True,
        description="If enabled, populates the snowflake table-to-table and s3-to-snowflake table lineage. Requires appropriate grants given to the role and Snowflake Enterprise Edition or above.",
    )
    include_view_lineage: bool = Field(
        default=True,
        description="If enabled, populates the snowflake view->table and table->view lineages. Requires appropriate grants given to the role, and include_table_lineage to be True. view->table lineage requires Snowflake Enterprise Edition or above.",
    )

    database_pattern: AllowDenyPattern = AllowDenyPattern(
        deny=[r"^UTIL_DB$", r"^SNOWFLAKE$", r"^SNOWFLAKE_SAMPLE_DATA$"]
    )

    ignore_start_time_lineage: bool = False
    upstream_lineage_in_report: bool = False

    convert_urns_to_lowercase: bool = Field(
        default=True,
    )

    include_usage_stats: bool = Field(
        default=True,
        description="If enabled, populates the snowflake usage statistics. Requires appropriate grants given to the role.",
    )

    include_technical_schema: bool = Field(
        default=True,
        description="If enabled, populates the snowflake technical schema and descriptions.",
    )

    include_column_lineage: bool = Field(
        default=True,
        description="Populates table->table and view->table column lineage. Requires appropriate grants given to the role and the Snowflake Enterprise Edition or above.",
    )

    include_view_column_lineage: bool = Field(
        default=True,
        description="Populates view->view and table->view column lineage using DataHub's sql parser.",
    )

    _check_role_grants_removed = pydantic_removed_field("check_role_grants")
    _provision_role_removed = pydantic_removed_field("provision_role")

    extract_tags: TagOption = Field(
        default=TagOption.skip,
        description="""Optional. Allowed values are `without_lineage`, `with_lineage`, and `skip` (default). `without_lineage` only extracts tags that have been applied directly to the given entity. `with_lineage` extracts both directly applied and propagated tags, but will be significantly slower. See the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/object-tagging.html#tag-lineage) for information about tag lineage/propagation. """,
    )

    include_external_url: bool = Field(
        default=True,
        description="Whether to populate Snowsight url for Snowflake Objects",
    )

    match_fully_qualified_names: bool = Field(
        default=False,
        description="Whether `schema_pattern` is matched against fully qualified schema name `<catalog>.<schema>`.",
    )

    _use_legacy_lineage_method_removed = pydantic_removed_field(
        "use_legacy_lineage_method"
    )

    validate_upstreams_against_patterns: bool = Field(
        default=True,
        description="Whether to validate upstream snowflake tables against allow-deny patterns",
    )

    tag_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="List of regex patterns for tags to include in ingestion. Only used if `extract_tags` is enabled.",
    )

    # This is required since access_history table does not capture whether the table was temporary table.
    temporary_tables_pattern: List[str] = Field(
        default=DEFAULT_TABLES_DENY_LIST,
        description="[Advanced] Regex patterns for temporary tables to filter in lineage ingestion. Specify regex to match the entire table name in database.schema.table format. Defaults are to set in such a way to ignore the temporary staging tables created by known ETL tools.",
    )

    rename_upstreams_deny_pattern_to_temporary_table_pattern = pydantic_renamed_field(
        "upstreams_deny_pattern", "temporary_tables_pattern"
    )

    shares: Optional[Dict[str, SnowflakeShareConfig]] = Field(
        default=None,
        description="Required if current account owns or consumes snowflake share."
        " If specified, connector creates lineage and siblings relationship between current account's database tables and consumer/producer account's database tables."
        " Map of share name -> details of share.",
    )

    email_as_user_identifier: bool = Field(
        default=True,
        description="Format user urns as an email, if the snowflake user's email is set. If `email_domain` is provided, generates email addresses for snowflake users with unset emails, based on their username.",
    )

    def get_sql_alchemy_url(
        self,
        uri_opts: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[SecretStr] = None,
        role: Optional[str] = None,
    ) -> str:
        return SnowflakeConnectionConfig.get_sql_alchemy_url(
            self,
            uri_opts=uri_opts,
            database=database,
            username=username,
            password=password,
            role=role,
        )

    @root_validator(skip_on_failure=True)
    def validate_include_view_lineage(cls, values):
        if (
            "include_table_lineage" in values
            and not values.get("include_table_lineage")
            and values.get("include_view_lineage")
        ):
            raise ValueError(
                "include_table_lineage must be True for include_view_lineage to be set."
            )
        return values

    @validator("convert_urns_to_lowercase")
    def validate_convert_urns_to_lowercase(cls, v):
        if not v:
            add_global_warning(
                "Please use `convert_urns_to_lowercase: True`, otherwise lineage to other sources may not work correctly."
            )

        return v

    @validator("include_column_lineage")
    def validate_include_column_lineage(cls, v, values):
        if not values.get("include_table_lineage") and v:
            raise ValueError(
                "include_table_lineage must be True for include_column_lineage to be set."
            )
        return v

    @root_validator(pre=False)
    def validate_unsupported_configs(cls, values: Dict) -> Dict:
        value = values.get("include_read_operational_stats")
        if value is not None and value:
            raise ValueError(
                "include_read_operational_stats is not supported. Set `include_read_operational_stats` to False.",
            )

        match_fully_qualified_names = values.get("match_fully_qualified_names")

        schema_pattern: Optional[AllowDenyPattern] = values.get("schema_pattern")

        if (
            schema_pattern is not None
            and schema_pattern != AllowDenyPattern.allow_all()
            and match_fully_qualified_names is not None
            and not match_fully_qualified_names
        ):
            logger.warning(
                "Please update `schema_pattern` to match against fully qualified schema name `<catalog_name>.<schema_name>` and set config `match_fully_qualified_names : True`."
                "Current default `match_fully_qualified_names: False` is only to maintain backward compatibility. "
                "The config option `match_fully_qualified_names` will be deprecated in future and the default behavior will assume `match_fully_qualified_names: True`."
            )

        # Always exclude reporting metadata for INFORMATION_SCHEMA schema
        if schema_pattern is not None and schema_pattern:
            logger.debug("Adding deny for INFORMATION_SCHEMA to schema_pattern.")
            cast(AllowDenyPattern, schema_pattern).deny.append(r".*INFORMATION_SCHEMA$")

        include_technical_schema = values.get("include_technical_schema")
        include_profiles = (
            values.get("profiling") is not None and values["profiling"].enabled
        )
        delete_detection_enabled = (
            values.get("stateful_ingestion") is not None
            and values["stateful_ingestion"].enabled
            and values["stateful_ingestion"].remove_stale_metadata
        )

        # TODO: Allow lineage extraction and profiling irrespective of basic schema extraction,
        # as it seems possible with some refactor
        if not include_technical_schema and any(
            [include_profiles, delete_detection_enabled]
        ):
            raise ValueError(
                "Cannot perform Deletion Detection or Profiling without extracting snowflake technical schema. Set `include_technical_schema` to True or disable Deletion Detection and Profiling."
            )

        return values

    @property
    def parse_view_ddl(self) -> bool:
        return self.include_view_column_lineage

    @validator("shares")
    def validate_shares(
        cls, shares: Optional[Dict[str, SnowflakeShareConfig]], values: Dict
    ) -> Optional[Dict[str, SnowflakeShareConfig]]:
        current_platform_instance = values.get("platform_instance")

        if shares:
            # Check: platform_instance should be present
            if current_platform_instance is None:
                logger.info(
                    "It is advisable to use `platform_instance` when ingesting from multiple snowflake accounts, if they contain databases with same name. "
                    "Setting `platform_instance` allows distinguishing such databases without conflict and correctly ingest their metadata."
                )

            databases_included_in_share: List[DatabaseId] = []
            databases_created_from_share: List[DatabaseId] = []

            for share_details in shares.values():
                shared_db = DatabaseId(
                    share_details.database, share_details.platform_instance
                )
                if current_platform_instance:
                    assert all(
                        consumer.platform_instance != share_details.platform_instance
                        for consumer in share_details.consumers
                    ), "Share's platform_instance can not be same as consumer's platform instance. Self-sharing not supported in Snowflake."

                databases_included_in_share.append(shared_db)
                databases_created_from_share.extend(share_details.consumers)

            for db_from_share in databases_created_from_share:
                assert (
                    db_from_share not in databases_included_in_share
                ), "Database included in a share can not be present as consumer in any share."
                assert (
                    databases_created_from_share.count(db_from_share) == 1
                ), "Same database can not be present as consumer in more than one share."

        return shares

    def outbounds(self) -> Dict[str, Set[DatabaseId]]:
        """
        Returns mapping of
            database included in current account's outbound share -> all databases created from this share in other accounts
        """
        outbounds: Dict[str, Set[DatabaseId]] = defaultdict(set)
        if self.shares:
            for share_name, share_details in self.shares.items():
                if share_details.platform_instance == self.platform_instance:
                    logger.debug(
                        f"database {share_details.database} is included in outbound share(s) {share_name}."
                    )
                    outbounds[share_details.database].update(share_details.consumers)
        return outbounds

    def inbounds(self) -> Dict[str, DatabaseId]:
        """
        Returns mapping of
            database created from an current account's inbound share -> other-account database from which this share was created
        """
        inbounds: Dict[str, DatabaseId] = {}
        if self.shares:
            for share_name, share_details in self.shares.items():
                for consumer in share_details.consumers:
                    if consumer.platform_instance == self.platform_instance:
                        logger.debug(
                            f"database {consumer.database} is created from inbound share {share_name}."
                        )
                        inbounds[consumer.database] = share_details.source_database
                        if self.platform_instance:
                            break
                        # If not using platform_instance, any one of consumer databases
                        # can be the database from this instance. so we include all relevant
                        # databases in inbounds.
                else:
                    logger.info(
                        f"Skipping Share {share_name}, as it does not include current platform instance {self.platform_instance}",
                    )
        return inbounds
