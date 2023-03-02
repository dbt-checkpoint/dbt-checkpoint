from unittest.mock import patch

import mixpanel
import pytest

from pre_commit_dbt.tracking import dbtCheckpointTracking
from pre_commit_dbt.utils import get_config_file


class TestDbtCheckpointTracking:
    def test_init(self, config_path_str):
        script_args = {"config": config_path_str}
        tracker = dbtCheckpointTracking(script_args)

        assert tracker.config == get_config_file(config_path_str)
        assert tracker.script_args == script_args
        assert tracker.disable_tracking is True

    def test_track_hook_event(self, config_with_tracking_path_str):
        script_args = {"config": config_with_tracking_path_str}
        print(script_args)
        tracker = dbtCheckpointTracking(script_args)

        with patch.object(mixpanel.Mixpanel, "track") as mock_track:
            # set the side_effect to raise an exception
            mock_track.side_effect = Exception("Test")

            # call your function that uses Mixpanel's track function
            with pytest.raises(Exception):
                tracker.track_hook_event("name", {}, {})

    def test_init_raises_value_error_if_config_path_not_str(self):
        with pytest.raises(ValueError):
            dbtCheckpointTracking({})

    def test_property_transformations_nones(self, config_path_str):
        script_args = {"config": config_path_str}
        tracker = dbtCheckpointTracking(script_args)

        event_properties = tracker._property_transformations(None, None)

        assert event_properties == {}

    def test_property_transformations(self, config_path_str):
        script_args = {"config": config_path_str}
        tracker = dbtCheckpointTracking(script_args)

        event_properties = tracker._property_transformations(
            {"user_id": "abc123"}, {"hook_name": "hook_name"}
        )

        assert event_properties == {"user_id": "abc123", "hook_name": "hook_name"}

    def test_property_transformations_with_status_code(self, config_path_str):
        script_args = {"config": config_path_str}
        tracker = dbtCheckpointTracking(script_args)

        event_properties = tracker._property_transformations(
            {"user_id": "abc123"}, {"status": 0, "hook_name": "hook.sql"}
        )

        assert event_properties == {
            "status": "Success",
            "hook_name": "hook",
            "user_id": "abc123",
        }

    def test_remove_ext_in_hook_name(self):
        transformed_properties = dbtCheckpointTracking._remove_ext_in_hook_name(
            {"hook_name": "hook.sql"}
        )

        assert transformed_properties == {"hook_name": "hook"}

    def test_remove_ext_in_hook_name_with_no_hook_name(self):
        transformed_properties = dbtCheckpointTracking._remove_ext_in_hook_name({})

        assert transformed_properties == {}
