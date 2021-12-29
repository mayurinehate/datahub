from typing import List
import time

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    Constraint, DatasetBatchSpec, DatasetValidationResults, DatasetValidationRun, FullTableBatchSpec
)
from datahub.metadata.schema_classes import ChangeTypeClass

def datasetUrn(tbl):
    return builder.make_dataset_urn("postgres", tbl)

def fldUrn(tbl, fld):
    return f"urn:li:schemaField:({datasetUrn(tbl)}, {fld})"

constraint_maxVal = Constraint(
    constraintType="column_value_is_less_than", 
    constraintDomain="columnValue", 
    constraintProvider="greatExpectations",
    field = fldUrn("fooTable", "col1"),
    parameters = {"column": "col1", "max_value": 99}
)

constraint_meanRange = Constraint(
    constraintType="column_mean_is_between", 
    constraintDomain="columnAgg", 
    constraintProvider="greatExpectations",
    field = fldUrn("fooTable", "col1"),
    parameters = {"column": "col1", "min_value": 90, "max_value": 99}
)

constraint_rowCount = Constraint(
    constraintType="table_row_count_is_between", 
    constraintDomain="table", 
    constraintProvider="greatExpectations",
    field = None,
    parameters = {"min_value": 4000, "max_value": 5500}
)

constraint_column_names =  Constraint(
    constraintType="table_columns_match_unordered", 
    constraintDomain="tableDef", 
    constraintProvider="greatExpectations",
    field = None,
    parameters = {"columns": ["col1", "col2", "col3"]}
)

# in this example, there is a single "batch" that contains the entire table
# practically, there could be multiple batches in the same run e.g. multiple partitions etc.
# See DatasetBatchSpec.batchSpecDetails for various types of specs supported.

batch = DatasetBatchSpec(
    batchId = "batch101",
    batchSpecDetails = FullTableBatchSpec()
)

# TODO: check null behavior in GE

# Validation results per constraint per batch. 
# Note that since we have a single batch in our example, we have the results per constraint (with fixed batch).
validationResults = [
    DatasetValidationResults(
        constraint=constraint_maxVal,
        batchSpec=batch,
        success=False,
        results={
            "row_count": 5000,
            "unexpected_count": 50, 
            "missing_count": 10
        }
    ),
    DatasetValidationResults(
        constraint=constraint_meanRange,
        batchSpec=batch,
        success=True,
        results={
            "row_count": 5000,
            "actual_agg_value": 92.5, 
            "missing_count": 10
        }
    ),
    DatasetValidationResults(
        constraint=constraint_rowCount,
        batchSpec=batch,
        success=True,
        results={
            "row_count": 5000,
            "missing_count": 10
        }
    ),
    DatasetValidationResults(
        constraint=constraint_column_names,
        batchSpec=batch,
        success=True,
        results = {}
        # maybe good to have unexpected values for this constraint... not in current spec.
    )
]

validationRun = DatasetValidationRun(
    timestampMillis=int(time.time() * 1000),
    constraintValidator="greatExpectations",
    runId="uuid1234",
    validationResults=validationResults
)

# Construct a MetadataChangeProposalWrapper object.
validationRun_mcp = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=datasetUrn("fooTable"),
    aspectName="datasetValidationRun",
    aspect=validationRun,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(validationRun_mcp)