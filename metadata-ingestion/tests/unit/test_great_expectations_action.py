import logging
from datetime import datetime, timezone
from unittest import mock

from great_expectations.core.batch import Batch, BatchDefinition, RuntimeBatchRequest
from great_expectations.core.batch_spec import (
    RuntimeQueryBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.id_dict import IDDict
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context import DataContext
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.execution_engine.pandas_execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.validator.validator import Validator

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.integrations.great_expectations import DatahubValidationAction
from datahub.metadata.schema_classes import (
    ConstraintClass,
    DatasetBatchSpecClass,
    DatasetValidationResultsClass,
    DatasetValidationRunClass,
    FullTableBatchSpecClass,
    PartitionBatchSpecClass,
)

logger = logging.getLogger(__name__)


def test_DatahubValidationAction_basic(tmp_path: str) -> None:
    data_context_parameterized_expectation_suite = DataContext.create(tmp_path)
    validation_result_suite = ExpectationSuiteValidationResult(
        results=[],
        success=True,
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.13.40",
            "expectation_suite_name": "asset.default",
            "run_id": {
                "run_name": "test_100",
            },
        },
    )
    validation_result_suite_id = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(run_name="test_100"),
        batch_identifier="1234",
    )
    validator = Validator(execution_engine=PandasExecutionEngine())

    server_url = "http://localhost:9999"

    # test with server_url set; expect pass no action
    datahub_action = DatahubValidationAction(
        data_context=data_context_parameterized_expectation_suite, server_url=server_url
    )

    assert (
        datahub_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=None,
        )
        == {"datahub_notification_result": "none required"}
    )

    # test with engine other than sqlalchemy, expect pass, no action
    assert (
        datahub_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=validator,
        )
        == {"datahub_notification_result": "none required"}
    )

    validation_result_suite.meta["batch_spec"] = SqlAlchemyDatasourceBatchSpec(
        {
            "schema_name": "public",
            "table_name": "table_partitioned_by_date_column__A",
            "batch_identifiers": {},
            "splitter_method": "_split_on_whole_table",
            "splitter_kwargs": {},
            "sampling_method": "_sample_using_limit",
            "sampling_kwargs": {"n": 20},
        }
    )
    validator = Validator(
        execution_engine=SqlAlchemyExecutionEngine(
            connection_string="postgresql://localhost:5432/test"
        )
    )

    # Test without server_url set; expect fail
    assert (
        datahub_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=validator,
        )
        == {"datahub_notification_result": "Datahub notification failed"}
    )


@mock.patch("datahub.emitter.rest_emitter.DatahubRestEmitter.emit_mcp", autospec=True)
def test_DatahubValidationAction_fulltable(
    mock_emitter: mock.MagicMock, tmp_path: str
) -> None:
    data_context_parameterized_expectation_suite = DataContext.create(tmp_path)
    validation_result_suite = ExpectationSuiteValidationResult(
        results=[
            {
                "success": True,
                "result": {"observed_value": 10000},
                "expectation_config": {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {
                        "max_value": 10000,
                        "min_value": 10000,
                        "batch_id": "1234",
                    },
                    "meta": {},
                },
            },
            {
                "result": {
                    "element_count": 10000,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "partial_unexpected_index_list": None,
                    "partial_unexpected_counts": [],
                },
                "success": True,
                "expectation_config": {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "date", "batch_id": "1234"},
                },
            },
            {
                "result": {"observed_value": 1.3577},
                "success": False,
                "expectation_config": {
                    "expectation_type": "expect_column_mean_to_be_between",
                    "kwargs": {
                        "column": "passenger_count",
                        "max_value": 1.5716,
                        "min_value": 1.5716,
                        "batch_id": "1234",
                    },
                },
            },
        ],
        success=True,
        statistics={
            "evaluated_expectations": 3,
            "successful_expectations": 2,
            "unsuccessful_expectations": 1,
            "success_percent": 66.66,
        },
        meta={
            "great_expectations_version": "v0.13.40",
            "expectation_suite_name": "asset.default",
            "run_id": {
                "run_name": "test_100",
            },
            "batch_spec": SqlAlchemyDatasourceBatchSpec(
                {
                    "schema_name": "public",
                    "table_name": "table_partitioned_by_date_column__A",
                    "batch_identifiers": {},
                    "splitter_method": "_split_on_whole_table",
                    "splitter_kwargs": {},
                    "sampling_method": "_sample_using_limit",
                    "sampling_kwargs": {"n": 20},
                }
            ),
        },
    )
    validator = Validator(
        execution_engine=SqlAlchemyExecutionEngine(
            connection_string="postgresql://localhost:5432/test"
        )
    )

    validation_result_suite_id = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(
            run_name="test_100",
            run_time=datetime.fromtimestamp(1640701702, tz=timezone.utc),
        ),
        batch_identifier="1234",
    )

    server_url = "http://localhost:9999"

    datahub_action = DatahubValidationAction(
        data_context=data_context_parameterized_expectation_suite, server_url=server_url
    )

    assert (
        datahub_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=validator,
        )
        == {"datahub_notification_result": "Datahub notification succeeded"}
    )

    mcpw = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType="UPSERT",
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:postgres,test.public.table_partitioned_by_date_column__A,PROD)",
        entityKeyAspect=None,
        auditHeader=None,
        aspectName="datasetValidationRun",
        aspect=DatasetValidationRunClass(
            timestampMillis=1640701702000,
            constraintValidator="great-expectations",
            runId="2021-12-28T14:28:22Z",
            validationResults=[
                DatasetValidationResultsClass(
                    constraint=ConstraintClass(
                        constraintProvider="great-expectations",
                        constraintType="expect_table_row_count_to_be_between",
                        constraintDomain="table",
                        parameters={
                            "max_value": 10000,
                            "min_value": 10000,
                        },
                    ),
                    batchSpec=DatasetBatchSpecClass(
                        batchId="1234",
                        batchSpecDetails=FullTableBatchSpecClass(),
                    ),
                    success=True,
                    results={"observed_value": 10000},
                ),
                DatasetValidationResultsClass(
                    constraint=ConstraintClass(
                        constraintProvider="great-expectations",
                        constraintType="expect_column_values_to_not_be_null",
                        constraintDomain="columnValue",
                        field="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,test.public.table_partitioned_by_date_column__A,PROD),date)",
                        parameters={},
                    ),
                    batchSpec=DatasetBatchSpecClass(
                        batchId="1234",
                        batchSpecDetails=FullTableBatchSpecClass(),
                    ),
                    success=True,
                    results={
                        "element_count": 10000,
                        "unexpected_count": 0,
                        "unexpected_percent": 0.0,
                    },
                ),
                DatasetValidationResultsClass(
                    constraint=ConstraintClass(
                        constraintProvider="great-expectations",
                        constraintType="expect_column_mean_to_be_between",
                        constraintDomain="columnAgg",
                        field="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,test.public.table_partitioned_by_date_column__A,PROD),passenger_count)",
                        parameters={
                            "max_value": 1.5716,
                            "min_value": 1.5716,
                        },
                    ),
                    batchSpec=DatasetBatchSpecClass(
                        batchId="1234",
                        batchSpecDetails=FullTableBatchSpecClass(),
                    ),
                    success=False,
                    results={"observed_value": 1.3577},
                ),
            ],
        ),
    )
    mock_emitter.assert_called_once_with(mock.ANY, mcpw)


@mock.patch("datahub.emitter.rest_emitter.DatahubRestEmitter.emit_mcp", autospec=True)
def test_DatahubValidationAction_partitiontable(
    mock_emitter: mock.MagicMock, tmp_path: str
) -> None:
    data_context_parameterized_expectation_suite = DataContext.create(tmp_path)
    validation_result_suite = ExpectationSuiteValidationResult(
        results=[
            {
                "success": True,
                "result": {"observed_value": 10000},
                "expectation_config": {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {
                        "max_value": 10000,
                        "min_value": 10000,
                        "batch_id": "1234",
                    },
                    "meta": {},
                },
            }
        ],
        success=True,
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.13.40",
            "expectation_suite_name": "asset.default",
            "run_id": {
                "run_name": "test_100",
            },
            "batch_spec": SqlAlchemyDatasourceBatchSpec(
                {
                    "table_name": "table_partitioned_by_date_column__A",
                    "batch_identifiers": {"date": "2021-12-20"},
                    "splitter_method": "_split_on_converted_datetime",
                    "splitter_kwargs": {
                        "column_name": "date",
                        "date_format_string": "%Y-%m-%d",
                    },
                    "sampling_method": "_sample_using_limit",
                    "sampling_kwargs": {"n": 20},
                }
            ),
        },
    )
    validator = Validator(
        execution_engine=SqlAlchemyExecutionEngine(
            connection_string="postgresql://localhost:5432/test"
        )
    )
    validation_result_suite_id = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(
            run_name="test_100", run_time=datetime.fromtimestamp(1640681902)
        ),
        batch_identifier="1234",
    )

    server_url = "http://localhost:9999"

    datahub_action = DatahubValidationAction(
        data_context=data_context_parameterized_expectation_suite, server_url=server_url
    )

    assert (
        datahub_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=validator,
        )
        == {"datahub_notification_result": "Datahub notification succeeded"}
    )

    mcpw = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType="UPSERT",
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:postgres,test.public.table_partitioned_by_date_column__A,PROD)",
        entityKeyAspect=None,
        auditHeader=None,
        aspectName="datasetValidationRun",
        aspect=DatasetValidationRunClass(
            timestampMillis=1640701702000,
            constraintValidator="great-expectations",
            runId="2021-12-28T14:28:22Z",
            validationResults=[
                DatasetValidationResultsClass(
                    constraint=ConstraintClass(
                        constraintProvider="great-expectations",
                        constraintType="expect_table_row_count_to_be_between",
                        constraintDomain="table",
                        parameters={
                            "max_value": 10000,
                            "min_value": 10000,
                        },
                    ),
                    batchSpec=DatasetBatchSpecClass(
                        batchId="1234",
                        batchSpecDetails=PartitionBatchSpecClass(
                            partitions=[{"date": "2021-12-20"}]
                        ),
                    ),
                    success=True,
                    results={"observed_value": 10000},
                )
            ],
        ),
    )
    mock_emitter.assert_called_once_with(mock.ANY, mcpw)


@mock.patch("datahub.emitter.rest_emitter.DatahubRestEmitter.emit_mcp", autospec=True)
def test_DatahubValidationAction_query(
    mock_emitter: mock.MagicMock, tmp_path: str
) -> None:
    data_context_parameterized_expectation_suite = DataContext.create(tmp_path)
    validation_result_suite = ExpectationSuiteValidationResult(
        results=[
            {
                "success": True,
                "result": {"observed_value": 10000},
                "expectation_config": {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {
                        "max_value": 10000,
                        "min_value": 10000,
                        "batch_id": "8cfd7cf3dd553ffa14546d6e87d73f06",
                    },
                    "meta": {},
                },
            }
        ],
        success=True,
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.13.40",
            "expectation_suite_name": "asset.default",
            "run_id": {
                "run_name": "test_100",
            },
            "batch_spec": RuntimeQueryBatchSpec(
                {
                    "data_asset_name": "some-data-asset",
                    "query": "SQLQuery",  # "select * from my_table where col>5",
                }
            ),
        },
    )

    validation_result_suite_id = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(
            run_name="test_100", run_time=datetime.fromtimestamp(1640681902)
        ),
        batch_identifier="8cfd7cf3dd553ffa14546d6e87d73f06",
    )

    validator = Validator(
        execution_engine=SqlAlchemyExecutionEngine(
            connection_string="postgresql://localhost:5432/test"
        ),
        batches=[
            Batch(
                data=[],
                batch_request=RuntimeBatchRequest(
                    datasource_name="my_postgres_datasource",
                    data_connector_name="default_runtime_data_connector_name",
                    data_asset_name="some-data-asset",
                    runtime_parameters={
                        "query": "select * from my_table where col>5",
                    },
                    batch_identifiers={"default_identifier_name": "default_identifier"},
                ),
                batch_spec=RuntimeQueryBatchSpec(
                    {
                        "data_asset_name": "some-data-asset",
                        "query": "SQLQuery",  # "select * from my_table where col>5",
                    }
                ),
                batch_definition=BatchDefinition(
                    datasource_name="my_postgres_datasource",
                    data_connector_name="default_runtime_data_connector_name",
                    data_asset_name="some-data-asset",
                    batch_identifiers=IDDict(),
                ),
            )
        ],
    )

    server_url = "http://localhost:9999"

    datahub_action = DatahubValidationAction(
        data_context=data_context_parameterized_expectation_suite, server_url=server_url
    )

    x = datahub_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=validation_result_suite,
        data_asset=validator,
    )
    assert x == {"datahub_notification_result": "none required"}

    mock_emitter.assert_not_called()
