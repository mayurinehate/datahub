from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, Iterable, Optional

from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_flow_urn,
    make_data_job_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    FlowContainerSubTypes,
    JobContainerSubTypes,
)
from datahub.ingestion.source.sql.stored_procedures.lineage import parse_procedure_code
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataPlatformInstanceClass,
    DataTransformClass,
    DataTransformLogicClass,
    QueryStatementClass,
    SubTypesClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver


@dataclass
class BaseProcedure:
    name: str
    procedure_definition: Optional[str]
    created: Optional[datetime]
    last_altered: Optional[datetime]
    comment: Optional[str]
    argument_signature: Optional[str]
    return_type: Optional[str]
    language: str
    extra_properties: Optional[Dict[str, str]]


def generate_flow_workunits(
    database_key: DatabaseKey, schema_key: Optional[SchemaKey]
) -> Iterable[MetadataWorkUnit]:
    """Generate flow workunits for database and schema"""

    procedure_flow_name = get_procedure_flow_name(database_key, schema_key)

    flow_urn = make_data_flow_urn(
        orchestrator=database_key.platform,
        flow_id=procedure_flow_name,
        cluster=database_key.env or DEFAULT_ENV,
        platform_instance=database_key.instance,
    )

    yield MetadataChangeProposalWrapper(
        entityUrn=flow_urn,
        aspect=DataFlowInfoClass(
            name=procedure_flow_name,
        ),
    ).as_workunit()

    yield MetadataChangeProposalWrapper(
        entityUrn=flow_urn,
        aspect=SubTypesClass(
            typeNames=[FlowContainerSubTypes.MSSQL_PROCEDURE_CONTAINER],
        ),
    ).as_workunit()

    if database_key.instance:
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(database_key.platform),
                instance=make_dataplatform_instance_urn(
                    platform=database_key.platform,
                    instance=database_key.instance,
                ),
            ),
        ).as_workunit()

    # TODO: container aspect boolean flag?
    yield MetadataChangeProposalWrapper(
        entityUrn=flow_urn,
        aspect=ContainerClass(container=database_key.as_urn()),
    ).as_workunit()


def get_procedure_flow_name(
    database_key: DatabaseKey, schema_key: Optional[SchemaKey]
) -> str:
    if schema_key:
        procedure_flow_name = (
            f"{schema_key.database}.{schema_key.db_schema}.stored_procedures"
        )
    else:
        procedure_flow_name = f"{database_key.database}.stored_procedures"
    return procedure_flow_name


def generate_job_workunits(
    database_key: DatabaseKey,
    schema_key: Optional[SchemaKey],
    procedure: BaseProcedure,
) -> Iterable[MetadataWorkUnit]:
    """Generate job workunits for database, schema and procedure"""

    # TODO: escape special chars if present in stored procedure name
    # TODO: add argument signature hash to procedure identifier
    job_urn = make_data_job_urn(
        orchestrator=database_key.platform,
        flow_id=get_procedure_flow_name(database_key, schema_key),
        job_id=procedure.name,
        cluster=database_key.env or DEFAULT_ENV,
        platform_instance=database_key.instance,
    )

    yield MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=DataJobInfoClass(
            name=procedure.name,
            type=JobContainerSubTypes.STORED_PROCEDURE,
            description=procedure.comment,
            customProperties=procedure.extra_properties,
        ),
    ).as_workunit()

    yield MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=SubTypesClass(
            typeNames=[JobContainerSubTypes.STORED_PROCEDURE],
        ),
    ).as_workunit()

    if database_key.instance:
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(database_key.platform),
                instance=make_dataplatform_instance_urn(
                    platform=database_key.platform,
                    instance=database_key.instance,
                ),
            ),
        ).as_workunit()

    # TODO: container aspect boolean flag?
    container_key = schema_key or database_key  # database_key for 2-tier
    yield MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=ContainerClass(container=container_key.as_urn()),
    ).as_workunit()

    # TODO: Config whether to ingest lineage
    # TODO: modification in mssql+lineage library and ingest lineage

    # TODO: Config whether to ingest procedure code
    if procedure.procedure_definition:
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataTransformLogicClass(
                transforms=[
                    DataTransformClass(
                        queryStatement=QueryStatementClass(
                            value=procedure.procedure_definition,
                            language=procedure.language,
                        ),
                    )
                ]
            ),
        ).as_workunit()


def generate_procedure_lineage(
    *,
    schema_resolver: SchemaResolver,
    procedure: BaseProcedure,
    procedure_job_urn: str,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
    is_temp_table: Callable[[str], bool] = lambda _: False,
    raise_: bool = False,
) -> Iterable[MetadataChangeProposalWrapper]:
    if procedure.procedure_definition:
        datajob_input_output = parse_procedure_code(
            schema_resolver=schema_resolver,
            default_db=default_db,
            default_schema=default_schema,
            code=procedure.procedure_definition,
            is_temp_table=is_temp_table,
            raise_=raise_,
        )

        if datajob_input_output:
            yield MetadataChangeProposalWrapper(
                entityUrn=procedure_job_urn,
                aspect=datajob_input_output,
            )

    # TODO: generate lineage workunits


def generate_procedure_workunits(
    procedure: BaseProcedure,
    database_key: DatabaseKey,
    schema_key: Optional[SchemaKey],
    schema_resolver: Optional[SchemaResolver],
) -> Iterable[MetadataWorkUnit]:
    yield from generate_flow_workunits(database_key, schema_key)

    yield from generate_job_workunits(database_key, schema_key, procedure)

    if schema_resolver:
        job_urn = make_data_job_urn(
            orchestrator=database_key.platform,
            flow_id=get_procedure_flow_name(database_key, schema_key),
            job_id=procedure.name,
            cluster=database_key.env or DEFAULT_ENV,
            platform_instance=database_key.instance,
        )

        yield from auto_workunit(
            generate_procedure_lineage(
                schema_resolver=schema_resolver,
                procedure=procedure,
                procedure_job_urn=job_urn,
                default_db=database_key.database,
                default_schema=schema_key.db_schema if schema_key else None,
            )
        )
