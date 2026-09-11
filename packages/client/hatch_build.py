from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class BundleFirstCommon(BuildHookInterface):
    """
    Ship the shared first_common schemas inside the alcf-ai wheel so end users
    install a single self-contained package.

    Editable installs are skipped: a copy there would shadow the workspace's
    editable first-common package.
    """

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if version == "standard":
            build_data["force_include"]["../common/first_common"] = "first_common"
