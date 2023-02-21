import os
from typing import Any
from typing import Dict
from typing import NoReturn
from typing import Optional

from mixpanel import Mixpanel

from pre_commit_dbt.utils import get_config_file

MIXPANEL_DEV_ENV = "34ffa16dc37f248c18ad6d1b9ea9c3a8"
MIXPANEL_PROD_ENV = "3fa3db873f6950d10bd770a49c57e33e"


class dbtCheckpointTracking:
    def __init__(self, script_args: Dict[str, Any]):
        config_path = script_args.get("config")
        if config_path is None or not isinstance(config_path, str):
            raise ValueError("config_path must be a non-empty string")

        self.config = get_config_file(config_path)

        self.script_args = script_args
        self.token = self._get_mixpanel_env_token()
        self.disable_tracking = self.config.get("disable-tracking", False)

    def track_hook_event(
        self,
        event_name: str,
        event_properties: Dict[str, Any],
        manifest: Dict[str, Any],
    ) -> NoReturn:
        if not self.disable_tracking:
            dbt_metadata = manifest.get("metadata")
            distinct_id = (
                dbt_metadata.get("user_id") if dbt_metadata is not None else None
            )
            event_properties = self._property_transformations(
                dbt_metadata, event_properties
            )
            try:
                mixpanel = Mixpanel(token=self.token)
                mixpanel.track(
                    distinct_id=distinct_id,
                    event_name=event_name,
                    properties=event_properties,
                )
            except Exception as error:
                print(f"Mixpanel Error: {error}")

    def _property_transformations(
        self,
        dbt_metadata: Optional[Dict[str, Any]],
        event_properties: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if event_properties is None:
            event_properties = {}
        if dbt_metadata is not None:
            event_properties.update(dbt_metadata)
            transformation_func = [
                self._status_code_to_text,
                self._remove_ext_in_hook_name,
            ]

            for function in transformation_func:
                event_properties = function(event_properties)

        return event_properties

    def _get_mixpanel_env_token(self) -> str:
        is_test = self.config.get("is-test", False) or self.script_args.get("is_test")
        token = MIXPANEL_DEV_ENV if is_test else MIXPANEL_PROD_ENV
        return token

    @staticmethod
    def _status_code_to_text(event_properties: Dict[str, Any]) -> Dict[str, Any]:
        transformed_properties = event_properties.copy()
        if transformed_properties.get("status") == 0:
            transformed_properties["status"] = "Success"
        elif transformed_properties.get("status") == 1:
            transformed_properties["status"] = "Fail"

        return transformed_properties

    @staticmethod
    def _remove_ext_in_hook_name(
        event_properties: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        transformed_properties = {} if event_properties is None else event_properties
        hook_name = transformed_properties.get("hook_name")
        if hook_name is not None:
            transformed_properties["hook_name"] = os.path.splitext(hook_name)[0]
        return transformed_properties
