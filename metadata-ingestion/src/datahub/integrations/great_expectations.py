import datetime
import logging
from datetime import timezone
from typing import Any, Dict, List, Optional, Union

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
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    Constraint,
    DatasetBatchSpec,
    DatasetValidationResults,
    DatasetValidationRun,
    FullTableBatchSpec,
    PartitionBatchSpec,
    QueryBatchSpec,
)
from datahub.metadata.com.linkedin.pegasus2avro.events.metadata import ChangeType

logger = logging.getLogger(__name__)


class DatahubValidationAction(ValidationAction):
    def __init__(
        self,
        data_context: DataContext,
        server_url: str,
        env: str = builder.DEFAULT_ENV,
        graceful_exceptions: bool = True,
        token: Optional[str] = None,
        timeout_sec: Optional[float] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ):
        super().__init__(data_context)
        self.server_url = server_url
        self.env = env
        self.graceful_exceptions = graceful_exceptions
        self.token = token
        self.timeout_sec = timeout_sec
        self.extra_headers = extra_headers

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
        try:

            # TODO - case if isinstance(validation_result_suite_identifier,GeCloudIdentifier)
            batch_id = validation_result_suite_identifier.batch_identifier

            # for now, we support only v3-api and sqlalchemy execution engine
            if isinstance(data_asset, Validator) and isinstance(
                data_asset.execution_engine, SqlAlchemyExecutionEngine
            ):
                execution_engine = data_asset.execution_engine
                url_instance = make_url(execution_engine.engine.url)
                data_platform = url_instance.drivername

                if validation_result_suite.meta is not None:
                    batch_spec = validation_result_suite.meta.get("batch_spec")
                if (
                    isinstance(batch_spec, SqlAlchemyDatasourceBatchSpec)
                    and "table_name" in batch_spec
                ):
                    # e.g. ConfiguredAssetSqlDataConnector with splitter_method or sampling_method
                    schema_name = (
                        batch_spec["schema_name"]
                        if "schema_name" in batch_spec
                        else None
                    )
                    table_name = (
                        batch_spec["table_name"] if "table_name" in batch_spec else None
                    )
                    limit = batch_spec.limit

                    dataset_urn = make_dataset_urn(
                        data_platform, schema_name, table_name, url_instance, self.env
                    )
                    # TODO - "sampling_method" in batch_spec, model doesn't support yet
                    if (
                        "splitter_method" in batch_spec
                        and batch_spec.get("splitter_method") != "_split_on_whole_table"
                    ):
                        batch_identifiers = batch_spec.get("batch_identifiers", {})
                        # String representation of partition values is used, as datahub model
                        # allows only strings i batch_identifiers
                        batch_identifiers = {
                            k: str(v) for k, v in batch_identifiers.items()
                        }
                        batchSpecDetails = PartitionBatchSpec(
                            [batch_identifiers]
                        )  # type:ignore
                    else:
                        batchSpecDetails = FullTableBatchSpec()  # type:ignore
                elif (
                    isinstance(batch_spec, RuntimeQueryBatchSpec)
                    and "query" in batch_spec
                    and batch_spec["query"] == "SQLQuery"
                ):
                    # TODO - Parse query to find dataset name and urn?
                    query = data_asset.batches[
                        batch_id
                    ].batch_request.runtime_parameters["query"]
                    batchSpecDetails = QueryBatchSpec(query=query)  # type:ignore
                    logger.warning(
                        "DatahubValidationAction does not support RuntimeQueryBatchSpec yet. \
                            No metadata will be reported."
                    )
                else:
                    logger.warning(
                        f"DatahubValidationAction does not recognize this GE batch spec - {type(batch_spec)}. \
                            No metadata will be reported."
                    )
            else:
                # TODO - v2-spec - SqlAlchemyDataset support
                logger.warning(
                    f"DatahubValidationAction does not recognize this GE data asset - {type(data_asset)}. \
                        This is either using v2-api or execution engine other than sqlalchemy. \
                            No metadata will be reported."
                )

            if dataset_urn is None or batchSpecDetails is None:
                return {"datahub_notification_result": "none required"}

            validation_results = []
            for result in validation_result_suite.results:
                expectation_config = result["expectation_config"]
                success = True if result["success"] else False
                expectation_type = expectation_config["expectation_type"]
                kwargs = expectation_config["kwargs"]

                result = result["result"]

                field: Optional[str] = None
                if "column" in kwargs and "observed_value" in result:
                    field = builder.make_schema_field_urn(dataset_urn, kwargs["column"])
                    constraintDomain = "columnAgg"
                elif "column" in kwargs and "unexpected_count" in result:
                    field = builder.make_schema_field_urn(dataset_urn, kwargs["column"])
                    constraintDomain = "columnValue"
                elif expectation_type in [
                    "expect_column_to_exist",
                    "expect_table_columns_to_match_ordered_list",
                    "expect_table_columns_to_match_set",
                    "expect_table_column_count_to_be_between",
                    "expect_table_column_count_to_equal",
                ]:
                    constraintDomain = "tableDef"
                else:
                    constraintDomain = "table"

                result = {
                    k: v
                    for k, v in result.items()
                    if isinstance(v, int) or isinstance(v, float)
                }

                kwargs_new: Dict[str, Union[int, float, str, List[str]]] = {
                    k: v
                    for k, v in kwargs.items()
                    if isinstance(v, (int, float, str))
                    and k not in ["batch_id", "column"]
                }
                # String representation of list values is used, as datahub model
                # allows list of strings only in constraint parameter's value
                kwargs_new.update(
                    {
                        k: [str(i) for i in v]
                        for k, v in kwargs.items()
                        if isinstance(v, (list))
                    }
                )

                validation_results.append(
                    DatasetValidationResults(
                        constraint=Constraint(
                            constraintProvider="great-expectations",
                            constraintDomain=constraintDomain,
                            constraintType=expectation_type,
                            parameters=kwargs_new,
                            field=field,
                        ),
                        batchSpec=DatasetBatchSpec(
                            batchId=batch_id,
                            batchSpecDetails=batchSpecDetails,
                            limit=limit,
                        ),
                        results=result,
                        success=success,
                    )
                )

            run_time = validation_result_suite_identifier.run_id.run_time.astimezone(
                timezone.utc
            )

            validation_time = validation_result_suite.meta.get(
                "validation_time",
                datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                ),
            )

            mcpw = MetadataChangeProposalWrapper(
                changeType=ChangeType.UPSERT,
                entityType="dataset",
                entityUrn=dataset_urn,
                aspectName="datasetValidationRun",
                aspect=DatasetValidationRun(
                    timestampMillis=int(
                        datetime.datetime.strptime(
                            validation_time, "%Y%m%dT%H%M%S.%fZ"
                        ).timestamp()
                        * 1000
                    ),
                    constraintValidator="great-expectations",
                    validationResults=validation_results,
                    runId=run_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ),
            )

            emitter = DatahubRestEmitter(
                self.server_url,
                self.token,
                self.timeout_sec,
                self.timeout_sec,
                self.extra_headers,
            )
            logger.debug(mcpw)
            logger.debug(int(run_time.timestamp() * 1000))
            logger.debug(mcpw.entityUrn)

            emitter.emit_mcp(mcpw)
            result = "Datahub notification succeeded"
        except Exception:
            result = "Datahub notification failed"
            if self.graceful_exceptions:
                logger.exception("Something went wrong in DatahubValidationAction")
                logger.info("Supressing error because graceful_exceptions is set")
            else:
                raise

        return {"datahub_notification_result": result}


def make_dataset_urn(data_platform, schema_name, table_name, url_instance, env):

    # Handle other data platform mapping here, if needed
    # postgresql->postgres, postgresql+psycopg2->postgres
    if "postgres" in data_platform:
        data_platform = "postgres"
    elif "mssql" in data_platform:
        data_platform = "mssql"
    elif "mysql" in data_platform:
        data_platform = "mysql"
    elif "redshift" in data_platform:
        data_platform = "redshift"
    elif "athena" in data_platform:
        data_platform = "athena"

    if data_platform in ["redshift", "postgres"]:
        schema_name = schema_name if schema_name else "public"
        if url_instance.database is None:
            logger.warning(
                f"DatahubValidationAction failed to locate database name for {data_platform}. \
                    No metadata will be reported."
            )
            return None
        schema_name = "{}.{}".format(url_instance.database, schema_name)
    elif data_platform == "mssql":
        schema_name = schema_name if schema_name else "dbo"
        if url_instance.database is None:
            logger.warning(
                f"DatahubValidationAction failed to locate database name for {data_platform}. \
                    No metadata will be reported."
            )
            return None
        schema_name = "{}.{}".format(url_instance.database, schema_name)
    elif data_platform in ["trino","snowflake"]:
        if schema_name is None or url_instance.database is None:
            logger.warning(
                f"DatahubValidationAction failed to locate schema name and/or database name \
                    for {data_platform}. No metadata will be reported."
            )
            return None
        schema_name = "{}.{}".format(url_instance.database, schema_name)
    elif data_platform == "bigquery":
        if url_instance.host is None or url_instance.database is None:
            logger.warning(
                f"DatahubValidationAction failed to locate host and/or database name for \
                    {data_platform}. No metadata will be reported."
            )
            return None
        schema_name = "{}.{}".format(url_instance.host, url_instance.database)

    schema_name = schema_name if schema_name else url_instance.database
    if schema_name is None:
        logger.warning(
            f"DatahubValidationAction failed to locate schema name for {data_platform}. No metadata will be reported."
        )
        return None

    dataset_name = "{}.{}".format(schema_name, table_name)

    dataset_urn = builder.make_dataset_urn(
        platform=data_platform, name=dataset_name, env=env
    )
    return dataset_urn
