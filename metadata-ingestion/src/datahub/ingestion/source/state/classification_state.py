from typing import Dict

import pydantic

from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class ClassificationCheckpointState(CheckpointStateBase):
    """
    Base class for representing the checkpoint state for all classification based sources.
    Stores the last successful classification time per urn.
    Subclasses can define additional state as appropriate.
    """

    # Stores urn, classification timestamp millis in a dict
    last_classification: Dict[str, pydantic.PositiveInt]
