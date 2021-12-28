import logging
from typing import Any, Dict, Optional, Union

from great_expectations.checkpoint.actions import ValidationAction
from great_expectations.core.batch import Batch
from great_expectations.core.batch_spec import (
    RuntimeQueryBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.data_asset.data_asset import DataAsset
from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.validator.validator import Validator
from sqlalchemy.engine.url import make_url

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ConstraintClass,
    DatasetBatchSpecClass,
    DatasetValidationResultsClass,
    DatasetValidationRunClass,
    FullTableBatchSpecClass,
    PartitionBatchSpecClass,
    QueryBatchSpecClass,
)

logger = logging.getLogger(__name__)


# TODO - long warnings for the cases that will not work,
# Add try except block/graceful handling, wherever possibility of exception
class DatahubValidationAction(ValidationAction):
    def __init__(
        self,
        data_context: DataContext,
        server_url: str,
        token: Optional[str] = None,
        connect_timeout_sec: Optional[float] = None,
        read_timeout_sec: Optional[float] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        ca_certificate_path: Optional[str] = None,
    ):
        super().__init__(data_context)
        self.server_url = server_url
        self.token = token
        self.connect_timeout_sec = connect_timeout_sec
        self.read_timeout_sec = read_timeout_sec
        self.extra_headers = extra_headers
        self.ca_certificate_path = ca_certificate_path

    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: ValidationResultIdentifier,
        data_asset: Union[Validator, DataAsset, Batch],
        payload: Any = None,
        expectation_suite_identifier: Optional[ExpectationSuiteIdentifier] = None,
        checkpoint_identifier: Any = None,
    ) -> Dict:
        dataset_urn = None
        limit = None

        # TODO - case if isinstance(validation_result_suite_identifier,GeCloudIdentifier)
        batch_id = validation_result_suite_identifier.batch_identifier

        # for now, we support only v3-api and sqlalchemy execution engine
        if isinstance(data_asset, Validator) and isinstance(
            data_asset.execution_engine, SqlAlchemyExecutionEngine
        ):
            execution_engine = data_asset.execution_engine
            url_instance = make_url(execution_engine.engine.url)
            data_platform = url_instance.drivername

            # Handle other data platform mapping here, if needed
            # postgresql->postgres, postgresql+psycopg2->postgres
            if "postgres" in data_platform:
                data_platform = "postgres"

            if validation_result_suite.meta is not None:
                batch_spec = validation_result_suite.meta.get("batch_spec")
            if isinstance(batch_spec, SqlAlchemyDatasourceBatchSpec) and (
                ("type" in batch_spec and batch_spec["type"] == "table")
                or ("table_name" in batch_spec)
            ):
                # e.g. ConfiguredAssetSqlDataConnector with splitter_method or sampling_method
                schema_name = (
                    batch_spec["schema_name"] if "schema_name" in batch_spec else None
                )
                table_name = (
                    batch_spec["table_name"] if "table_name" in batch_spec else None
                )
                limit = batch_spec.limit

                dataset_urn = make_dataset_urn(
                    data_platform, schema_name, table_name, url_instance
                )
                # TODO - "sampling_method" in batch_spec, model doesn't support yet
                if (
                    "splitter_method" in batch_spec
                    and batch_spec.get("splitter_method") != "_split_on_whole_table"
                ):
                    batch_identifiers = batch_spec.get("batch_identifiers", {})
                    batch_identifiers = {
                        k: str(v) for k, v in batch_identifiers.items()
                    }
                    batchSpecDetails = PartitionBatchSpecClass(
                        [batch_identifiers]
                    )  # type:ignore
                else:
                    batchSpecDetails = FullTableBatchSpecClass()  # type:ignore
            elif (
                isinstance(batch_spec, RuntimeQueryBatchSpec)
                and "query" in batch_spec
                and batch_spec["query"] == "SQLQuery"
            ):
                # TODO - Parse query to find dataset name and urn?
                query = data_asset.batches[batch_id].batch_request.runtime_parameters[
                    "query"
                ]
                batchSpecDetails = QueryBatchSpecClass(query=query)  # type:ignore
                logger.warning(
                    "DatahubValidationAction does not support RuntimeQueryBatchSpec yet"
                )
            else:
                logger.warning(
                    f"DatahubValidationAction does not recognize this GE batch spec - {type(batch_spec)}. \
                        No metadata will be reported."
                )
        else:
            logger.warning(
                f"DatahubValidationAction does not recognize this GE data asset - {type(data_asset)}. \
                    This is either using v2-api or execution engine other than sqlalchemy. \
                        No metadata will be reported."
            )

        if dataset_urn is None or batchSpecDetails is None:
            logger.warning(
                "Something went wrong. No metadata will be reported to datahub."
            )
            return {"datahub_notification_result": "none required"}

        run_time = validation_result_suite_identifier.run_id.run_time

        validation_results = []
        for result in validation_result_suite.results:
            expectation_config = result["expectation_config"]
            success = True if result["success"] else False
            expectation_type = expectation_config["expectation_type"]
            kwargs = expectation_config["kwargs"]
            kwargs = {k: str(v) for k, v in kwargs.items() if k != "batch_id"}

            result = result["result"]
            result = {
                k: v
                for k, v in result.items()
                if isinstance(v, int) or isinstance(v, float)
            }
            field: Optional[str]
            if "column" in kwargs and "observed_value" in result:
                field = builder.make_schema_field_urn(dataset_urn, kwargs["column"])
                constraintDomain = "columnAgg"
            elif "column" in kwargs and "unexpected_count" in result:
                field = builder.make_schema_field_urn(dataset_urn, kwargs["column"])
                constraintDomain = "columnValue"
            else:
                field = None
                constraintDomain = "table"
            validation_results.append(
                DatasetValidationResultsClass(
                    constraint=ConstraintClass(
                        constraintProvider="great-expectations",
                        constraintDomain=constraintDomain,
                        constraintType=expectation_type,
                        parameters=kwargs,
                        field=field,
                    ),
                    batchSpec=DatasetBatchSpecClass(
                        batchId=batch_id, batchSpecDetails=batchSpecDetails, limit=limit
                    ),
                    results=result,
                    success=success,
                )
            )

        # We need to actually emit appropriate data quality aspect/entity
        mcpw = MetadataChangeProposalWrapper(
            changeType=ChangeTypeClass.UPSERT,
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="datasetValidationRun",
            aspect=DatasetValidationRunClass(
                timestampMillis=int(run_time.timestamp() * 1000),
                constraintValidator="great-expectations",
                validationResults=validation_results,
                runId=str(run_time),
            ),
        )

        emitter = DatahubRestEmitter(
            self.server_url,
            self.token,
            self.connect_timeout_sec,
            self.read_timeout_sec,
            self.extra_headers,
            self.ca_certificate_path,
        )
        logger.debug(mcpw)
        logger.debug(int(run_time.timestamp() * 1000))
        logger.debug(mcpw.entityUrn)
        try:

            emitter.emit_mcp(mcpw)
            result = "Datahub notification succeeded"
        except Exception:
            result = "Datahub notification failed"
            logger.exception("Something went wrong in DatahubValidationAction")

        return {"datahub_notification_result": result}


def make_dataset_urn(data_platform, schema_name, table_name, url_instance):

    if data_platform in ["postgres", "mssql", "trino"]:
        if schema_name is None or url_instance.database is None:
            logger.warning(
                f"Failed to locate schema name and database name for {data_platform}"
            )
            return None
        # TODO - default schema name handling
        schema_name = "{}.{}".format(url_instance.database, schema_name)
    elif data_platform == "bigquery":
        if url_instance.host is None or url_instance.database is None:
            logger.warning(
                f"Failed to locate host and database name for {data_platform}"
            )
            return None
        schema_name = "{}.{}".format(url_instance.host, url_instance.database)

    schema_name = schema_name if schema_name else url_instance.database
    if schema_name is None:
        logger.warning(f"Failed to locate schema name for {data_platform}")
        return None

    dataset_name = "{}.{}".format(schema_name, table_name)

    dataset_urn = builder.make_dataset_urn(platform=data_platform, name=dataset_name)
    return dataset_urn
