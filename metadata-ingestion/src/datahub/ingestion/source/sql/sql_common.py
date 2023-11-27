import datetime
import logging
import traceback
from dataclasses import dataclass, field
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

import sqlalchemy.dialects.postgresql.base
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes as types
from sqlalchemy.types import TypeDecorator, TypeEngine

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.incremental_lineage_helper import auto_incremental_lineage
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.common.api import ExtractionInterface
from datahub.ingestion.source.sql.common.base_api import (
    TWO_TIER_PLACEHOLDER_DB,
    BaseExtractionApi,
)
from datahub.ingestion.source.sql.common.models import (
    DatabaseIdentifier,
    SchemaIdentifier,
    TableIdentifier,
)
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    downgrade_schema_from_v2,
    gen_database_container,
    gen_database_key,
    gen_schema_container,
    gen_schema_key,
    get_domain_wu,
    schema_requires_v2,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    ForeignKeyConstraint,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    ViewPropertiesClass,
)
from datahub.telemetry import telemetry
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.sqlalchemy_query_combiner import SQLAlchemyQueryCombinerReport
from datahub.utilities.sqlglot_lineage import (
    SchemaResolver,
    SqlParsingResult,
    sqlglot_lineage,
    view_definition_lineage_helper,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import (
        DatahubGEProfiler,
        GEProfilerRequest,
    )

logger: logging.Logger = logging.getLogger(__name__)

MISSING_COLUMN_INFO = "missing column information"


@dataclass
class SQLSourceReport(StaleEntityRemovalSourceReport):
    tables_scanned: int = 0
    views_scanned: int = 0
    entities_profiled: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    query_combiner: Optional[SQLAlchemyQueryCombinerReport] = None

    num_view_definitions_parsed: int = 0
    num_view_definitions_failed_parsing: int = 0
    num_view_definitions_failed_column_parsing: int = 0
    view_definitions_parsing_failures: LossyList[str] = field(default_factory=LossyList)

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a view or a table
        """
        if ent_type == "table":
            self.tables_scanned += 1
        elif ent_type == "view":
            self.views_scanned += 1
        else:
            raise KeyError(f"Unknown entity {ent_type}.")

    def report_entity_profiled(self, name: str) -> None:
        self.entities_profiled += 1

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)

    def report_from_query_combiner(
        self, query_combiner_report: SQLAlchemyQueryCombinerReport
    ) -> None:
        self.query_combiner = query_combiner_report


# TODO: Remove SqlWorkUnit
SqlWorkUnit = MetadataWorkUnit


_field_type_mapping: Dict[Type[TypeEngine], Type] = {
    # Note: to add dialect-specific types to this mapping, use the `register_custom_type` function.
    types.Integer: NumberTypeClass,
    types.Numeric: NumberTypeClass,
    types.Boolean: BooleanTypeClass,
    types.Enum: EnumTypeClass,
    types._Binary: BytesTypeClass,
    types.LargeBinary: BytesTypeClass,
    types.PickleType: BytesTypeClass,
    types.ARRAY: ArrayTypeClass,
    types.String: StringTypeClass,
    types.Date: DateTypeClass,
    types.DATE: DateTypeClass,
    types.Time: TimeTypeClass,
    types.DateTime: TimeTypeClass,
    types.DATETIME: TimeTypeClass,
    types.TIMESTAMP: TimeTypeClass,
    types.JSON: RecordTypeClass,
    # Because the postgresql dialect is used internally by many other dialects,
    # we add some postgres types here. This is ok to do because the postgresql
    # dialect is built-in to sqlalchemy.
    sqlalchemy.dialects.postgresql.base.BYTEA: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.DOUBLE_PRECISION: NumberTypeClass,
    sqlalchemy.dialects.postgresql.base.INET: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.MACADDR: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.MONEY: NumberTypeClass,
    sqlalchemy.dialects.postgresql.base.OID: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.REGCLASS: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.TIMESTAMP: TimeTypeClass,
    sqlalchemy.dialects.postgresql.base.TIME: TimeTypeClass,
    sqlalchemy.dialects.postgresql.base.INTERVAL: TimeTypeClass,
    sqlalchemy.dialects.postgresql.base.BIT: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.UUID: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.TSVECTOR: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.ENUM: EnumTypeClass,
    # When SQLAlchemy is unable to map a type into its internal hierarchy, it
    # assigns the NullType by default. We want to carry this warning through.
    types.NullType: NullTypeClass,
}
_known_unknown_field_types: Set[Type[TypeEngine]] = {
    types.Interval,
    types.CLOB,
}


def register_custom_type(tp: Type[TypeEngine], output: Optional[Type] = None) -> None:
    if output:
        _field_type_mapping[tp] = output
    else:
        _known_unknown_field_types.add(tp)


class _CustomSQLAlchemyDummyType(TypeDecorator):
    impl = types.LargeBinary


def make_sqlalchemy_type(name: str) -> Type[TypeEngine]:
    # This usage of type() dynamically constructs a class.
    # See https://stackoverflow.com/a/15247202/5004662 and
    # https://docs.python.org/3/library/functions.html#type.
    sqlalchemy_type: Type[TypeEngine] = type(
        name,
        (_CustomSQLAlchemyDummyType,),
        {
            "__repr__": lambda self: f"{name}()",
        },
    )
    return sqlalchemy_type


def get_column_type(
    sql_report: SQLSourceReport, dataset_name: str, column_type: Any
) -> SchemaFieldDataType:
    """
    Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
    """

    TypeClass: Optional[Type] = None
    for sql_type in _field_type_mapping.keys():
        if isinstance(column_type, sql_type):
            TypeClass = _field_type_mapping[sql_type]
            break
    if TypeClass is None:
        for sql_type in _known_unknown_field_types:
            if isinstance(column_type, sql_type):
                TypeClass = NullTypeClass
                break

    if TypeClass is None:
        sql_report.report_warning(
            dataset_name, f"unable to map type {column_type!r} to metadata schema"
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def get_schema_metadata(
    sql_report: SQLSourceReport,
    dataset_name: str,
    platform: str,
    columns: List[dict],
    pk_constraints: Optional[dict] = None,
    foreign_keys: Optional[List[ForeignKeyConstraint]] = None,
    canonical_schema: Optional[List[SchemaField]] = None,
    simplify_nested_field_paths: bool = False,
) -> SchemaMetadata:
    if (
        simplify_nested_field_paths
        and canonical_schema is not None
        and not schema_requires_v2(canonical_schema)
    ):
        canonical_schema = downgrade_schema_from_v2(canonical_schema)

    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=make_data_platform_urn(platform),
        version=0,
        hash="",
        platformSchema=MySqlDDL(tableSchema=""),
        fields=canonical_schema or [],
    )
    if foreign_keys is not None and foreign_keys != []:
        schema_metadata.foreignKeys = foreign_keys

    return schema_metadata


# config flags to emit telemetry for
config_options_to_report = [
    "include_views",
    "include_tables",
]


@dataclass
class ProfileMetadata:
    """
    A class to hold information about the table for profile enrichment
    """

    dataset_name_to_storage_bytes: Dict[str, int] = field(default_factory=dict)


class SQLAlchemySource(StatefulIngestionSourceBase):
    """A Base class for all SQL Sources that use SQLAlchemy to extend"""

    def __init__(self, config: SQLCommonConfig, ctx: PipelineContext, platform: str):
        super(SQLAlchemySource, self).__init__(config, ctx)
        self.config = config
        self.platform = platform
        self.report: SQLSourceReport = SQLSourceReport()
        self.profile_metadata_info: ProfileMetadata = ProfileMetadata()

        config_report = {
            config_option: config.dict().get(config_option)
            for config_option in config_options_to_report
        }

        config_report = {
            **config_report,
            "profiling_enabled": config.is_profiling_enabled(),
            "platform": platform,
        }

        telemetry.telemetry_instance.ping(
            "sql_config",
            config_report,
        )

        if config.is_profiling_enabled():
            telemetry.telemetry_instance.ping(
                "sql_profiling_config",
                config.profiling.config_for_telemetry(),
            )

        self.domain_registry: Optional[DomainRegistry] = None
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        self.views_failed_parsing: Set[str] = set()
        self.schema_resolver: SchemaResolver = SchemaResolver(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        self._view_definition_cache: MutableMapping[str, str]
        if self.config.use_file_backed_cache:
            self._view_definition_cache = FileBackedDict[str]()
        else:
            self._view_definition_cache = {}

        self.api: ExtractionInterface = BaseExtractionApi(
            config, self.IS_TWO_TIER_PLATFORM
        )

    @property
    def IS_TWO_TIER_PLATFORM(self) -> bool:
        TWO_TIER_PLATFORMS = ["mysql", "hive", "clickhouse", "teradata"]
        return self.platform in TWO_TIER_PLATFORMS

    def warn(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_warning(key, reason[:100])
        log.warning(f"{key} => {reason}")

    def error(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_failure(key, reason[:100])
        log.error(f"{key} => {reason}\n{traceback.format_exc()}")

    # TODO - override for sources get_databases
    def get_inspectors(self) -> Iterable[Inspector]:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)
            yield inspector

    def gen_database_containers(
        self,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = gen_database_key(
            database,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_database_container(
            database=database,
            database_container_key=database_container_key,
            sub_types=[DatasetContainerSubTypes.DATABASE],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            extra_properties=extra_properties,
        )

    def gen_schema_containers(
        self,
        schema: SchemaIdentifier,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if self.IS_TWO_TIER_PLATFORM:
            # For 2-tier platforms, we generate schema containers as database containers
            self.gen_database_containers(schema.schema_name)
        else:
            database_container_key = gen_database_key(
                schema.database_id.database_name,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            schema_container_key = gen_schema_key(
                db_name=schema.database_id.database_name,
                schema=schema.schema_name,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield from gen_schema_container(
                database=schema.database_id.database_name,
                schema=schema.schema_name,
                schema_container_key=schema_container_key,
                database_container_key=database_container_key,
                sub_types=[DatasetContainerSubTypes.SCHEMA],
                domain_registry=self.domain_registry,
                domain_config=self.config.domain,
                extra_properties=extra_properties,
            )

    def add_table_to_schema_container(
        self,
        dataset_urn: str,
        schema: SchemaIdentifier,
    ) -> Iterable[MetadataWorkUnit]:
        if self.IS_TWO_TIER_PLATFORM:
            schema_container_key = gen_database_key(
                db_name=schema.schema_name,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield from add_table_to_schema_container(
                dataset_urn=dataset_urn,
                parent_container_key=schema_container_key,
            )
        else:
            schema_container_key = gen_schema_key(
                db_name=schema.database_id.database_name,
                schema=schema.schema_name,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield from add_table_to_schema_container(
                dataset_urn=dataset_urn,
                parent_container_key=schema_container_key,
            )

    def get_database_level_workunits(
        self,
        database: DatabaseIdentifier,
    ) -> Iterable[MetadataWorkUnit]:
        if database == TWO_TIER_PLACEHOLDER_DB and self.IS_TWO_TIER_PLATFORM:
            # If we are using 2-tier platform, we don't need to generate database containers here.
            return
        yield from self.gen_database_containers(database=database.database_name)

    def get_schema_level_workunits(
        self, schema: SchemaIdentifier
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from self.gen_schema_containers(schema=schema)

        if self.config.include_tables:
            yield from self.loop_tables(schema, self.config)

        if self.config.include_views:
            yield from self.loop_views(schema, self.config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            partial(
                auto_incremental_lineage,
                self.ctx.graph,
                self.config.incremental_lineage,
            ),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        if logger.isEnabledFor(logging.DEBUG):
            # If debug logging is enabled, we also want to echo each SQL query issued.
            self.config.options.setdefault("echo", True)

        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        if self.config.is_profiling_enabled():
            self.config.options.setdefault(
                "max_overflow", self.config.profiling.max_workers
            )

        try:
            databases = self.api.get_databases()
        except NotImplementedError:
            # Method `get_databases` must be overridden in three tier sources
            # Or `database` config should be specified in recipe.
            raise

        for database in databases:
            yield from self._process_database(database)

        if self.config.include_view_lineage:
            yield from self.get_view_lineage()

    def _process_database(
        self, database: DatabaseIdentifier
    ) -> Iterable[MetadataWorkUnit]:
        profiler = None
        profile_requests: List["GEProfilerRequest"] = []
        if self.config.is_profiling_enabled():
            profiler = self.api.get_profiler_instance(database)
            try:
                self.add_profile_metadata(None)  # TODO
            except Exception as e:
                self.warn(
                    logger,
                    "profile_metadata",
                    f"Failed to get enrichment data for profile {e}",
                )

        yield from self.get_database_level_workunits(database)

        for schema in self.api.get_schemas(database):
            if not self.config.schema_pattern.allowed(schema.name):
                self.report.report_dropped(f"{schema}.*")
                continue
            else:
                yield from self._process_schema(schema, profiler, profile_requests)

        if profiler and profile_requests:
            yield from self.loop_profiler(
                profile_requests, profiler, platform=self.platform
            )

    def _process_schema(
        self,
        schema: SchemaIdentifier,
        profiler: Optional[DatahubGEProfiler],
        profile_requests: List[GEProfilerRequest],
    ) -> Iterable[MetadataWorkUnit]:
        yield from self.get_schema_level_workunits(schema=schema)

        if profiler:
            profile_requests += list(self.loop_profiler_requests(schema, self.config))

    def get_view_lineage(self) -> Iterable[MetadataWorkUnit]:
        builder = SqlParsingBuilder(
            generate_lineage=True,
            generate_usage_statistics=False,
            generate_operations=False,
        )
        for dataset_name in self._view_definition_cache.keys():
            view_definition = self._view_definition_cache[dataset_name]
            result = self._run_sql_parser(
                dataset_name,
                view_definition,
                self.schema_resolver,
            )
            if result and result.out_tables:
                # This does not yield any workunits but we use
                # yield here to execute this method
                yield from builder.process_sql_parsing_result(
                    result=result,
                    query=view_definition,
                    is_view_ddl=True,
                    include_column_lineage=self.config.include_view_column_lineage,
                )
            else:
                self.views_failed_parsing.add(dataset_name)
        yield from builder.gen_workunits()

    def get_identifier(self, table: TableIdentifier) -> str:
        # Many SQLAlchemy dialects have three-level hierarchies. This method, which
        # subclasses can override, enables them to modify the identifiers as needed.
        if hasattr(self.config, "get_identifier"):
            # This path is deprecated and will eventually be removed.
            return self.config.get_identifier(schema=table.schema_id.schema_name, table=table.table_name)  # type: ignore
        if self.IS_TWO_TIER_PLATFORM:
            return f"{table.schema_id.schema_name}.{table.table_name}"
        else:
            return f"{table.schema_id.database_id.database_name}.{table.schema_id.schema_name}.{table.table_name}"

    def get_foreign_key_metadata(
        self,
        dataset_urn: str,
        schema: SchemaIdentifier,
        fk_dict: Dict[str, str],
    ) -> ForeignKeyConstraint:
        referred_schema: Optional[str] = fk_dict.get("referred_schema")

        if not referred_schema:
            referred_schema = schema.schema_name

        referred_dataset_name = self.get_identifier(
            TableIdentifier(fk_dict["referred_table"], schema_id=schema)
        )

        source_fields = [
            f"urn:li:schemaField:({dataset_urn},{f})"
            for f in fk_dict["constrained_columns"]
        ]
        foreign_dataset = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=referred_dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        foreign_fields = [
            f"urn:li:schemaField:({foreign_dataset},{f})"
            for f in fk_dict["referred_columns"]
        ]

        return ForeignKeyConstraint(
            fk_dict["name"], foreign_fields, source_fields, foreign_dataset
        )

    def loop_tables(  # noqa: C901
        self,
        schema: SchemaIdentifier,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        tables_seen: Set[str] = set()
        try:
            for table in self.api.get_tables(schema):
                dataset_name = self.get_identifier(table)

                if dataset_name not in tables_seen:
                    tables_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue

                self.report.report_entity_scanned(dataset_name, ent_type="table")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                try:
                    yield from self._process_table(dataset_name, table, sql_config)
                except Exception as e:
                    self.warn(logger, f"{schema}.{table}", f"Ingestion error: {e}")
        except Exception as e:
            self.error(logger, f"{schema}", f"Tables error: {e}")

    def _process_table(
        self,
        dataset_name: str,
        table: TableIdentifier,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        columns = self.api.get_table_columns(table)
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],
        )

        detail = self.api.get_table_details(table)

        dataset_properties = DatasetPropertiesClass(
            name=table,
            description=detail.comment,
            customProperties=detail.properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        if self.config.include_table_location_lineage and detail.location:
            external_upstream_table = UpstreamClass(
                dataset=detail.location,
                type=DatasetLineageTypeClass.COPY,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_snapshot.urn,
                aspect=UpstreamLineage(upstreams=[external_upstream_table]),
            ).as_workunit()

        pk_constraints: dict = self.api.get_pk_constraint(table)
        foreign_keys = self._get_foreign_keys(dataset_urn, table)
        schema_fields = self.get_schema_fields(
            dataset_name, columns, pk_constraints, tags=None
        )
        schema_metadata = get_schema_metadata(
            self.report,
            dataset_name,
            self.platform,
            columns,
            pk_constraints,
            foreign_keys,
            schema_fields,
        )
        dataset_snapshot.aspects.append(schema_metadata)
        if self.config.include_view_lineage:
            self.schema_resolver.add_schema_metadata(dataset_urn, schema_metadata)

        yield from self.add_table_to_schema_container(
            dataset_urn=dataset_urn, schema=table.schema_id
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield SqlWorkUnit(id=dataset_name, mce=mce)
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        yield MetadataWorkUnit(
            id=f"{dataset_name}-subtypes",
            mcp=MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[DatasetSubTypes.TABLE]),
            ),
        )

        if self.config.domain:
            assert self.domain_registry
            yield from get_domain_wu(
                dataset_name=dataset_name,
                entity_urn=dataset_urn,
                domain_config=sql_config.domain,
                domain_registry=self.domain_registry,
            )

    # TODO
    def get_database_properties(
        self, inspector: Inspector, database: str
    ) -> Optional[Dict[str, str]]:
        return None

    # TODO
    def get_schema_properties(
        self, inspector: Inspector, database: str, schema: str
    ) -> Optional[Dict[str, str]]:
        return None

    # TODO
    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        return None, {}, None

    def get_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[MetadataWorkUnit]:
        # If we are a platform instance based source, emit the instance aspect
        if self.config.platform_instance:
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            ).as_workunit()
        else:
            return None

    def _get_foreign_keys(
        self, dataset_urn: str, table: TableIdentifier
    ) -> List[ForeignKeyConstraint]:
        try:
            foreign_keys = [
                self.get_foreign_key_metadata(dataset_urn, table.schema_id, fk_rec)
                for fk_rec in self.api.get_foreign_keys(table)
            ]
        except KeyError:
            # certain databases like MySQL cause issues due to lower-case/upper-case irregularities
            logger.debug(
                f"{dataset_urn}: failure in foreign key extraction... skipping"
            )
            foreign_keys = []
        return foreign_keys

    def get_schema_fields(
        self,
        dataset_name: str,
        columns: List[dict],
        pk_constraints: Optional[dict] = None,
        tags: Optional[Dict[str, List[str]]] = None,
    ) -> List[SchemaField]:
        canonical_schema = []
        for column in columns:
            column_tags: Optional[List[str]] = None
            if tags:
                column_tags = tags.get(column["name"], [])
            fields = self.get_schema_fields_for_column(
                dataset_name, column, pk_constraints, tags=column_tags
            )
            canonical_schema.extend(fields)
        return canonical_schema

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: dict,
        pk_constraints: Optional[dict] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:
        gtc: Optional[GlobalTagsClass] = None
        if tags:
            tags_str = [make_tag_urn(t) for t in tags]
            tags_tac = [TagAssociationClass(t) for t in tags_str]
            gtc = GlobalTagsClass(tags_tac)
        field = SchemaField(
            fieldPath=column["name"],
            type=get_column_type(self.report, dataset_name, column["type"]),
            nativeDataType=column.get("full_type", repr(column["type"])),
            description=column.get("comment", None),
            nullable=column["nullable"],
            recursive=False,
            globalTags=gtc,
        )
        if (
            pk_constraints is not None
            and isinstance(pk_constraints, dict)  # some dialects (hive) return list
            and column["name"] in pk_constraints.get("constrained_columns", [])
        ):
            field.isPartOfKey = True
        return [field]

    def loop_views(
        self,
        schema: SchemaIdentifier,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        try:
            for view in self.api.get_views(schema):
                dataset_name = self.get_identifier(table=view)
                self.report.report_entity_scanned(dataset_name, ent_type="view")

                if not sql_config.view_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                try:
                    yield from self._process_view(
                        dataset_name=dataset_name,
                        view=view,
                        sql_config=sql_config,
                    )
                except Exception as e:
                    self.warn(logger, f"{schema}.{view}", f"Ingestion error: {e}")
        except Exception as e:
            self.error(logger, f"{schema}", f"Views error: {e}")

    def _process_view(
        self,
        dataset_name: str,
        view: TableIdentifier,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        try:
            columns = self.api.get_table_columns(view)
        except KeyError:
            # For certain types of views, we are unable to fetch the list of columns.
            self.warn(logger, dataset_name, "unable to get schema for this view")
            schema_metadata = None
        else:
            schema_fields = self.get_schema_fields(dataset_name, columns)
            schema_metadata = get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                canonical_schema=schema_fields,
            )
            if self.config.include_view_lineage:
                self.schema_resolver.add_schema_metadata(dataset_urn, schema_metadata)
        description, properties, _ = self.api.get_table_details(view)
        try:
            view_definition = self.api.get_view_definition(view)
            if view_definition is None:
                view_definition = ""
            else:
                # Some dialects return a TextClause instead of a raw string,
                # so we need to convert them to a string.
                view_definition = str(view_definition)
        except NotImplementedError:
            view_definition = ""
        properties["view_definition"] = view_definition
        properties["is_view"] = "True"
        if view_definition and self.config.include_view_lineage:
            self._view_definition_cache[dataset_name] = view_definition

        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],
        )

        yield from self.add_table_to_schema_container(
            dataset_urn=dataset_urn,
            schema=view.schema_id,
        )

        dataset_properties = DatasetPropertiesClass(
            name=view,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)
        if schema_metadata:
            dataset_snapshot.aspects.append(schema_metadata)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield SqlWorkUnit(id=dataset_name, mce=mce)
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
        ).as_workunit()
        if "view_definition" in properties:
            view_definition_string = properties["view_definition"]
            view_properties_aspect = ViewPropertiesClass(
                materialized=False, viewLanguage="SQL", viewLogic=view_definition_string
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_properties_aspect,
            ).as_workunit()

        if self.config.domain and self.domain_registry:
            yield from get_domain_wu(
                dataset_name=dataset_name,
                entity_urn=dataset_urn,
                domain_config=sql_config.domain,
                domain_registry=self.domain_registry,
            )

    def _run_sql_parser(
        self, view_identifier: str, query: str, schema_resolver: SchemaResolver
    ) -> Optional[SqlParsingResult]:
        try:
            database, schema = self.get_db_schema(view_identifier)
        except ValueError:
            logger.warning(f"Invalid view identifier: {view_identifier}")
            return None
        raw_lineage = sqlglot_lineage(
            query,
            schema_resolver=schema_resolver,
            default_db=database,
            default_schema=schema,
        )
        view_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            view_identifier,
            self.config.platform_instance,
            self.config.env,
        )

        if raw_lineage.debug_info.table_error:
            logger.debug(
                f"Failed to parse lineage for view {view_identifier}: "
                f"{raw_lineage.debug_info.table_error}"
            )
            self.report.num_view_definitions_failed_parsing += 1
            self.report.view_definitions_parsing_failures.append(
                f"Table-level sql parsing error for view {view_identifier}: {raw_lineage.debug_info.table_error}"
            )
            return None

        elif raw_lineage.debug_info.column_error:
            self.report.num_view_definitions_failed_column_parsing += 1
            self.report.view_definitions_parsing_failures.append(
                f"Column-level sql parsing error for view {view_identifier}: {raw_lineage.debug_info.column_error}"
            )
        else:
            self.report.num_view_definitions_parsed += 1
        return view_definition_lineage_helper(raw_lineage, view_urn)

    def get_db_schema(self, dataset_identifier: str) -> Tuple[Optional[str], str]:
        database, schema, _view = dataset_identifier.split(".")
        return database, schema

    def get_profile_args(self) -> Dict:
        """Passed down to GE profiler"""
        return {}

    # Override if needed
    def generate_partition_profiler_query(
        self, schema: str, table: str, partition_datetime: Optional[datetime.datetime]
    ) -> Tuple[Optional[str], Optional[str]]:
        return None, None

    def is_table_partitioned(
        self, database: Optional[str], schema: str, table: str
    ) -> Optional[bool]:
        return None

    # Override if needed
    # TODO
    def generate_profile_candidates(
        self,
        inspector: Inspector,
        threshold_time: Optional[datetime.datetime],
        schema: str,
    ) -> Optional[List[str]]:
        raise NotImplementedError()

    # Override if you want to do additional checks
    def is_dataset_eligible_for_profiling(
        self,
        dataset_name: str,
        sql_config: SQLCommonConfig,
        profile_candidates: Optional[List[str]],
    ) -> bool:
        return (
            sql_config.table_pattern.allowed(dataset_name)
            and sql_config.profile_pattern.allowed(dataset_name)
        ) and (
            profile_candidates is None
            or (profile_candidates is not None and dataset_name in profile_candidates)
        )

    def loop_profiler_requests(
        self,
        schema: SchemaIdentifier,
        sql_config: SQLCommonConfig,
    ) -> Iterable["GEProfilerRequest"]:
        from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest

        tables_seen: Set[str] = set()
        profile_candidates = None  # Default value if profile candidates not available.
        if (
            sql_config.profiling.profile_if_updated_since_days is not None
            or sql_config.profiling.profile_table_size_limit is not None
            or sql_config.profiling.profile_table_row_limit is None
        ):
            try:
                threshold_time: Optional[datetime.datetime] = None
                if sql_config.profiling.profile_if_updated_since_days is not None:
                    threshold_time = datetime.datetime.now(
                        datetime.timezone.utc
                    ) - datetime.timedelta(
                        sql_config.profiling.profile_if_updated_since_days
                    )
                    # TODO
                # profile_candidates = self.generate_profile_candidates(
                #    inspector, threshold_time, schema
                # )
                print(threshold_time)
            except NotImplementedError:
                logger.debug("Source does not support generating profile candidates.")

        for table in self.api.get_tables(schema):
            dataset_name = self.get_identifier(table=table)
            if not self.is_dataset_eligible_for_profiling(
                dataset_name, sql_config, profile_candidates
            ):
                if self.config.profiling.report_dropped_profiles:
                    self.report.report_dropped(f"profile of {dataset_name}")
                continue

            if dataset_name not in tables_seen:
                tables_seen.add(dataset_name)
            else:
                logger.debug(f"{dataset_name} has already been seen, skipping...")
                continue

            missing_column_info_warn = self.report.warnings.get(MISSING_COLUMN_INFO)
            if (
                missing_column_info_warn is not None
                and dataset_name in missing_column_info_warn
            ):
                continue

            (partition, custom_sql) = self.generate_partition_profiler_query(
                schema, table, self.config.profiling.partition_datetime
            )

            if partition is None and self.is_table_partitioned(
                database=None, schema=schema, table=table
            ):
                self.warn(
                    logger,
                    "profile skipped as partitioned table is empty or partition id was invalid",
                    dataset_name,
                )
                continue

            if (
                partition is not None
                and not self.config.profiling.partition_profiling_enabled
            ):
                logger.debug(
                    f"{dataset_name} and partition {partition} is skipped because profiling.partition_profiling_enabled property is disabled"
                )
                continue

            self.report.report_entity_profiled(dataset_name)
            logger.debug(
                f"Preparing profiling request for {schema}, {table}, {partition}"
            )
            yield GEProfilerRequest(
                pretty_name=dataset_name,
                batch_kwargs=self.prepare_profiler_args(
                    schema=schema,
                    table=table,
                    partition=partition,
                    custom_sql=custom_sql,
                ),
            )

    def add_profile_metadata(self, inspector: Inspector) -> None:
        """
        Method to add profile metadata in a sub-class that can be used to enrich profile metadata.
        This is meant to change self.profile_metadata_info in the sub-class.
        """
        pass

    def loop_profiler(
        self,
        profile_requests: List["GEProfilerRequest"],
        profiler: "DatahubGEProfiler",
        platform: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        for request, profile in profiler.generate_profiles(
            profile_requests,
            self.config.profiling.max_workers,
            platform=platform,
            profiler_args=self.get_profile_args(),
        ):
            if profile is None:
                continue
            dataset_name = request.pretty_name
            if (
                dataset_name in self.profile_metadata_info.dataset_name_to_storage_bytes
                and profile.sizeInBytes is None
            ):
                profile.sizeInBytes = (
                    self.profile_metadata_info.dataset_name_to_storage_bytes[
                        dataset_name
                    ]
                )
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=profile,
            ).as_workunit()

    def prepare_profiler_args(
        self,
        schema: str,
        table: str,
        partition: Optional[str],
        custom_sql: Optional[str] = None,
    ) -> dict:
        return dict(
            schema=schema, table=table, partition=partition, custom_sql=custom_sql
        )

    def get_report(self):
        return self.report
