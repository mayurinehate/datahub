import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from datahub_classify.helper_classes import ColumnInfo, Metadata
from pydantic import Field, PositiveInt

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.emitter.mce_builder import get_sys_time, make_term_urn, make_user_urn
from datahub.ingestion.glossary.classifier import ClassificationConfig, Classifier
from datahub.ingestion.glossary.classifier_registry import classifier_registry
from datahub.ingestion.source.state.classification_state_handler import (
    ClassificationStateHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulClassificationConfig,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    GlossaryTermAssociation,
    GlossaryTerms,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class ClassificationReportMixin:
    num_tables_classification_attempted: int = 0
    num_tables_classification_failed: int = 0
    num_tables_classified: int = 0

    num_tables_classification_skipped: int = 0

    info_types_detected: LossyDict[str, LossyList[str]] = field(
        default_factory=LossyDict
    )

    classification_skipped: LossyList[str] = field(default_factory=LossyList)


class ClassificationSourceConfigMixin(ConfigModel):
    classification: ClassificationConfig = Field(
        default=ClassificationConfig(),
        description="For details, refer [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md).",
    )


class ClassificationHandler:
    def __init__(
        self,
        config: ClassificationConfig,
        report: ClassificationReportMixin,
        state_handler: Optional[ClassificationStateHandler] = None,
        stateful_config: Optional[StatefulClassificationConfig] = None,
    ):
        self.config = config
        self.report = report
        self.classifiers = self.get_classifiers()
        self.state_handler = state_handler
        self.stateful_config = stateful_config

    def is_classification_enabled(self) -> bool:
        return self.config.enabled and len(self.config.classifiers) > 0

    def is_classification_enabled_for_table(self, dataset_name: str) -> bool:
        return (
            self.config.enabled
            and len(self.config.classifiers) > 0
            and self.config.table_pattern.allowed(dataset_name)
        )

    def is_classification_enabled_for_column(
        self, dataset_name: str, column_name: str
    ) -> bool:
        return (
            self.config.enabled
            and len(self.config.classifiers) > 0
            and self.config.table_pattern.allowed(dataset_name)
            and self.config.column_pattern.allowed(f"{dataset_name}.{column_name}")
        )

    def get_classifiers(self) -> List[Classifier]:
        classifiers = []

        for classifier in self.config.classifiers:
            classifier_class = classifier_registry.get(classifier.type)
            if classifier_class is None:
                raise ConfigurationError(
                    f"Cannot find classifier class of type={self.config.classifiers[0].type} "
                    " in the registry! Please check the type of the classifier in your config."
                )
            classifiers.append(
                classifier_class.create(
                    config_dict=classifier.config,  # type: ignore
                )
            )

        return classifiers

    def classify_schema_fields(
        self,
        dataset_name: str,
        schema_metadata: SchemaMetadata,
        sample_data: Dict[str, list],
        dataset_urn: str,
        last_updated_at: Optional[datetime],
    ) -> None:
        column_infos = self.get_columns_to_classify(
            dataset_name, schema_metadata, sample_data
        )

        if not column_infos:
            logger.debug(f"No columns in {dataset_name} considered for classification")
            return None

        last_classification_time: Optional[int] = None
        if self.state_handler:
            last_classification_time = self.state_handler.get_last_classification(
                dataset_urn
            )

            if (
                last_classification_time is None
            ) or self.repeat_classification(  # If table has never been classified ->
                last_updated_at, last_classification_time
            ):
                pass  # proceed to classify the table
            else:
                self.report.num_tables_classification_skipped += 1
                logger.debug(f"Classification skipped for table {dataset_name}")
                self.report.classification_skipped.append(dataset_name)
                self.state_handler.add_to_state(dataset_urn, last_classification_time)
                return

        logger.debug(f"Classifying table {dataset_name}")
        self.report.num_tables_classification_attempted += 1
        field_terms: Dict[str, str] = {}
        with PerfTimer() as timer:
            try:
                for classifier in self.classifiers:
                    column_info_with_proposals = classifier.classify(column_infos)
                    self.extract_field_wise_terms(
                        field_terms, column_info_with_proposals
                    )
                if self.state_handler:
                    self.state_handler.add_to_state(
                        dataset_urn, int(datetime.now().timestamp() * 1000)
                    )
            except Exception:
                self.report.num_tables_classification_failed += 1
                raise
            finally:
                time_taken = timer.elapsed_seconds()
                logger.debug(
                    f"Finished classification {dataset_name}; took {time_taken:.3f} seconds"
                )

        if field_terms:
            self.report.num_tables_classified += 1
            self.populate_terms_in_schema_metadata(schema_metadata, field_terms)

    def repeat_classification(
        self,
        last_updated_at: Optional[datetime],
        last_classification_time: PositiveInt,
    ) -> bool:
        return (  # If last altered is present and repeat_after_table_update is enabled
            last_updated_at is not None
            and int(last_updated_at.timestamp() * 1000) > last_classification_time
            and self.stateful_config is not None
            and self.stateful_config.repeat_after_table_update
        ) or (  # If classification time is older than x days and repeat_after_x days is enabled
            self.stateful_config is not None
            and self.stateful_config.repeat_after_x_days is not None
            and (
                last_classification_time
                < int(
                    (
                        datetime.now()
                        - timedelta(days=self.stateful_config.repeat_after_x_days)
                    ).timestamp()
                    * 1000
                )
            )
        )

    def populate_terms_in_schema_metadata(
        self,
        schema_metadata: SchemaMetadata,
        field_terms: Dict[str, str],
    ) -> None:
        for schema_field in schema_metadata.fields:
            if schema_field.fieldPath in field_terms:
                schema_field.glossaryTerms = GlossaryTerms(
                    terms=[
                        GlossaryTermAssociation(
                            urn=make_term_urn(field_terms[schema_field.fieldPath])
                        )
                    ]
                    # Keep existing terms if present
                    + (
                        schema_field.glossaryTerms.terms
                        if schema_field.glossaryTerms
                        else []
                    ),
                    auditStamp=AuditStamp(
                        time=get_sys_time(), actor=make_user_urn("datahub")
                    ),
                )

    def extract_field_wise_terms(
        self,
        field_terms: Dict[str, str],
        column_info_with_proposals: List[ColumnInfo],
    ) -> None:
        for col_info in column_info_with_proposals:
            if not col_info.infotype_proposals:
                continue
            infotype_proposal = max(
                col_info.infotype_proposals, key=lambda p: p.confidence_level
            )
            self.report.info_types_detected.setdefault(
                infotype_proposal.infotype, LossyList()
            ).append(f"{col_info.metadata.dataset_name}.{col_info.metadata.name}")
            field_terms[col_info.metadata.name] = self.config.info_type_to_term.get(
                infotype_proposal.infotype, infotype_proposal.infotype
            )

    def get_columns_to_classify(
        self,
        dataset_name: str,
        schema_metadata: SchemaMetadata,
        sample_data: Dict[str, list],
    ) -> List[ColumnInfo]:
        column_infos: List[ColumnInfo] = []

        for schema_field in schema_metadata.fields:
            if not self.is_classification_enabled_for_column(
                dataset_name, schema_field.fieldPath
            ):
                logger.debug(
                    f"Skipping column {dataset_name}.{schema_field.fieldPath} from classification"
                )
                continue
            column_infos.append(
                ColumnInfo(
                    metadata=Metadata(
                        {
                            "Name": schema_field.fieldPath,
                            "Description": schema_field.description,
                            "DataType": schema_field.nativeDataType,
                            "Dataset_Name": dataset_name,
                        }
                    ),
                    values=sample_data[schema_field.fieldPath]
                    if schema_field.fieldPath in sample_data.keys()
                    else [],
                )
            )

        return column_infos
