#!/usr/bin/env python3

from base import CommandGenerator
from dx_utils import DXUtils
from typing import List, Dict, Optional, Tuple, Set

class DXCommandGenerator(CommandGenerator):
    """
    Abstract base class for command generators that interact with DNAnexus.
    Provides common methods for project info, file finding, and auth token handling.
    """

    def __init__(self):
        super().__init__()
        # DXUtils methods are static, so no need to instantiate DXUtils

    def _run_dx_find_command(self, dx_command_args: List[str], command_description: str) -> List[Dict]:
        """Wrapper for DXUtils.run_dx_find_command."""
        return DXUtils.run_dx_find_command(dx_command_args, command_description)

    def _get_project_name(self, project_id: str) -> Optional[str]:
        """Wrapper for DXUtils.get_project_name."""
        return DXUtils.get_project_name(project_id)

    def _detect_project_info(self, dx_file_id: str) -> Tuple[str, str]:
        """Wrapper for DXUtils.detect_project_info."""
        return DXUtils.detect_project_info(dx_file_id)

    def _extract_pan_numbers(self, dx_file_id: str) -> Optional[Set[str]]:
        """Wrapper for DXUtils.extract_pan_numbers."""
        return DXUtils.extract_pan_numbers(dx_file_id)

    def _get_auth_token(self) -> str:
        """Wrapper for DXUtils.get_auth_token, using config path."""
        return DXUtils.get_auth_token(self.config_values['dnanexus_auth_token_path'])