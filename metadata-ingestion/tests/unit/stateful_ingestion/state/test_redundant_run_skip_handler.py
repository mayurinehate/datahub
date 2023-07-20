from datetime import datetime, timezone
from unittest import mock

import pytest

from datahub.configuration.time_window_config import BucketDuration, get_time_bucket
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    DynamicTypedStateProviderConfig,
)
from datahub.ingestion.source.state.usage_common_state import (
    BaseTimeWindowCheckpointState,
)
from datahub.utilities.time import datetime_to_ts_millis

GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


@pytest.fixture
def stateful_source(mock_datahub_graph: DataHubGraph) -> SnowflakeV2Source:
    pipeline_name = "test_redundant_run_lineage"
    run_id = "test_redundant_run"
    ctx = PipelineContext(
        pipeline_name=pipeline_name,
        run_id=run_id,
        graph=mock_datahub_graph,
    )
    config = SnowflakeV2Config(
        account_id="ABC12345.ap-south-1",
        username="TST_USR",
        password="TST_PWD",
        stateful_ingestion=StatefulStaleMetadataRemovalConfig(
            enabled=True,
            state_provider=DynamicTypedStateProviderConfig(
                type="datahub", config={"datahub_api": {"server": GMS_SERVER}}
            ),
        ),
    )
    source = SnowflakeV2Source(ctx=ctx, config=config)
    return source


# last run
last_run_start_time = datetime(2023, 7, 2, tzinfo=timezone.utc)
last_run_end_time = datetime(2023, 7, 3, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "start_time,end_time,should_skip,suggested_start_time,suggested_end_time",
    [
        # Case = current run time window is same as of last run time window
        [
            datetime(2023, 7, 2, tzinfo=timezone.utc),
            datetime(2023, 7, 3, 12, tzinfo=timezone.utc),
            True,
            None,
            None,
        ],
        # Case = current run time window is subset of last run time window
        [
            datetime(2023, 7, 2, tzinfo=timezone.utc),
            datetime(2023, 7, 3, tzinfo=timezone.utc),
            True,
            None,
            None,
        ],
        # Case = current run time window is after last run time window but has some overlap with last run
        # Scenario for next day's run for scheduled daily ingestions
        [
            datetime(2023, 7, 3, tzinfo=timezone.utc),
            datetime(2023, 7, 4, 12, tzinfo=timezone.utc),
            False,
            datetime(2023, 7, 3, 12, tzinfo=timezone.utc),
            datetime(2023, 7, 4, 12, tzinfo=timezone.utc),
        ],
        # Case = current run time window is after last run time window and has no some overlap with last run
        [
            datetime(2023, 7, 5, tzinfo=timezone.utc),
            datetime(2023, 7, 7, 12, tzinfo=timezone.utc),
            False,
            datetime(2023, 7, 5, tzinfo=timezone.utc),
            datetime(2023, 7, 7, 12, tzinfo=timezone.utc),
        ],
        # Case = current run time window is before last run time window but has some overlap with last run
        # Scenario for manual run for past dates
        [
            datetime(2023, 6, 30, tzinfo=timezone.utc),
            datetime(2023, 7, 2, 12, tzinfo=timezone.utc),
            False,
            datetime(2023, 6, 30, tzinfo=timezone.utc),
            datetime(2023, 7, 2, tzinfo=timezone.utc),
        ],
        # Case = current run time window is before last run time window and has no overlap with last run
        # Scenario for manual run for past dates
        [
            datetime(2023, 6, 20, tzinfo=timezone.utc),
            datetime(2023, 6, 30, tzinfo=timezone.utc),
            False,
            datetime(2023, 6, 20, tzinfo=timezone.utc),
            datetime(2023, 6, 30, tzinfo=timezone.utc),
        ],
        # Case = current run time window subsumes last run time window and extends on both sides
        # Scenario for manual run
        [
            datetime(2023, 6, 20, tzinfo=timezone.utc),
            datetime(2023, 7, 20, tzinfo=timezone.utc),
            False,
            datetime(2023, 6, 20, tzinfo=timezone.utc),
            datetime(2023, 7, 20, tzinfo=timezone.utc),
        ],
    ],
)
def test_redundant_run_skip_handler_basic(
    stateful_source: SnowflakeV2Source,
    start_time: datetime,
    end_time: datetime,
    should_skip: bool,
    suggested_start_time: datetime,
    suggested_end_time: datetime,
) -> None:
    # mock_datahub_graph

    # mocked_source = mock.MagicMock()
    # mocked_config = mock.MagicMock()

    with mock.patch(
        "datahub.ingestion.source.state.stateful_ingestion_base.StateProviderWrapper.get_last_checkpoint"
    ) as mocked_fn:
        set_mock_last_run_time_window(
            mocked_fn,
            last_run_start_time,
            last_run_end_time,
        )

        # Redundant Lineage Skip Handler
        assert stateful_source.lineage_extractor is not None
        assert stateful_source.lineage_extractor.redundant_run_skip_handler is not None
        assert (
            stateful_source.lineage_extractor.redundant_run_skip_handler.should_skip_this_run(
                datetime_to_ts_millis(start_time),
                datetime_to_ts_millis(end_time),
            )
            == should_skip
        )

        if not should_skip:
            suggested_time_window = stateful_source.lineage_extractor.redundant_run_skip_handler.suggest_run_time_window(
                start_time, end_time
            )
            assert suggested_time_window == (suggested_start_time, suggested_end_time)

        set_mock_last_run_time_window_usage(
            mocked_fn, last_run_start_time, last_run_end_time
        )
        # Redundant Usage Skip Handler
        assert stateful_source.usage_extractor is not None
        assert stateful_source.usage_extractor.redundant_run_skip_handler is not None
        assert (
            stateful_source.usage_extractor.redundant_run_skip_handler.should_skip_this_run(
                datetime_to_ts_millis(start_time),
                datetime_to_ts_millis(end_time),
            )
            == should_skip
        )

        if not should_skip:
            suggested_time_window = stateful_source.usage_extractor.redundant_run_skip_handler.suggest_run_time_window(
                start_time, end_time
            )
            assert suggested_time_window == (
                get_time_bucket(suggested_start_time, BucketDuration.DAY),
                suggested_end_time,
            )


def set_mock_last_run_time_window(mocked_fn, start_time, end_time):
    mock_checkpoint = mock.MagicMock()
    mock_checkpoint.state = BaseTimeWindowCheckpointState(
        begin_timestamp_millis=datetime_to_ts_millis(start_time),
        end_timestamp_millis=datetime_to_ts_millis(end_time),
    )
    mocked_fn.return_value = mock_checkpoint


def set_mock_last_run_time_window_usage(mocked_fn, start_time, end_time):
    mock_checkpoint = mock.MagicMock()
    mock_checkpoint.state = BaseTimeWindowCheckpointState(
        begin_timestamp_millis=datetime_to_ts_millis(start_time),
        end_timestamp_millis=datetime_to_ts_millis(end_time),
        bucket_duration=BucketDuration.DAY,
    )
    mocked_fn.return_value = mock_checkpoint
