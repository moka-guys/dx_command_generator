#!/usr/bin/env python3

import os
import sys
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Set, Any
from modules.base import CommandGenerator
from modules.dx_utils import DXUtils

class DXCommandGenerator(CommandGenerator):
    """
    Abstract base class for command generators that interact with DNAnexus.
    Provides common methods for project info, file finding, and auth token handling.
    Also includes utilities for common CLI argument parsing and output file creation.
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

    def _get_project_id_from_input(self, prompt_message: str) -> Optional[str]:
        """
        Prompts the user for a DNAnexus project ID or reads it from sys.argv.
        """
        if len(sys.argv) != 2:
            print(f"\n{self.name} Configuration:")
            print("---------------------------")
            project_id = input(f"{prompt_message} (e.g., project-xxxx): ").strip()
            
            if not project_id:
                print("Error: No project ID provided.")
                return None
            
            if not project_id.startswith("project-"):
                print("Error: Project ID must start with 'project-'")
                return None
        else:
            project_id = sys.argv[1]
        return project_id

    def _initialize_output_file(self, output_file: str, project_id: str,
                                project_name: str, script_description: str,
                                include_project_vars: bool = True) -> bool:
        """
        Initializes the output shell script with a shebang, header, and makes it executable.
        Optionally includes AUTH_TOKEN, PROJECT_ID, and PROJECT_NAME variables.
        Returns True on success, False on failure.
        """
        try:
            with open(output_file, 'w') as f:
                f.write("#!/bin/bash\n")
                f.write(f"# {script_description}\n")
                f.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Project: {project_name} ({project_id})\n\n")

                if include_project_vars:
                    auth_token = self._get_auth_token()
                    f.write(f"AUTH_TOKEN=\"{auth_token}\"\n")
                    f.write(f"PROJECT_ID=\"{project_id}\"\n")
                    f.write(f"PROJECT_NAME=\"{project_name}\"\n\n")

            os.chmod(output_file, 0o755)
            print(f"\nSuccessfully initialized output script: {output_file}")
            return True
        except IOError as e:
            print(f"Error writing to output file {output_file}: {e}", file=sys.stderr)
            return False
        except Exception as e:
            print(f"An unexpected error occurred during output file initialization: {e}", file=sys.stderr)
            return False

    def _find_dx_files(self, project_id: str, glob_pattern: str, file_class: str = "file") -> List[Dict]:
        """
        Finds DNAnexus files matching a glob pattern in a given project.
        Returns a list of dictionaries, each containing 'id' and 'describe' keys.
        """
        dx_command_args = [
            "dx", "find", "data",
            "--name", glob_pattern,
            "--class", file_class,
            "--project", project_id,
            "--json"
        ]
        return self._run_dx_find_command(dx_command_args, f"'{glob_pattern}' file query")

    def _pair_dx_files(self, primary_files_data: List[Dict], primary_suffix: str,
                       secondary_files_data: List[Dict], secondary_suffix: str,
                       base_name_transform: Optional[Any] = None) -> List[Tuple[str, str]]:
        """
        Pairs DNAnexus files based on a common base name.
        `base_name_transform` is an optional function to apply to the filename before extracting base name.
        Returns a list of (primary_file_id, secondary_file_id) tuples.
        """
        primary_files: Dict[str, str] = {}
        secondary_files: Dict[str, str] = {}

        for item in primary_files_data:
            try:
                file_id = item['id']
                file_name = item['describe']['name']
                if base_name_transform:
                    file_name = base_name_transform(file_name)

                if file_name.endswith(primary_suffix):
                    base_name = file_name[:-len(primary_suffix)]
                    primary_files[base_name] = file_id
                else:
                    print(f"Warning: Primary file '{file_name}' (ID: {file_id}) from query did not end with '{primary_suffix}'. Skipping.", file=sys.stderr)
            except KeyError as e:
                print(f"Skipping primary item due to missing key {e} in JSON item: {item}", file=sys.stderr)
                continue
        
        for item in secondary_files_data:
            try:
                file_id = item['id']
                file_name = item['describe']['name']
                if base_name_transform:
                    file_name = base_name_transform(file_name)

                if file_name.endswith(secondary_suffix):
                    base_name = file_name[:-len(secondary_suffix)]
                    secondary_files[base_name] = file_id
                else:
                    print(f"Warning: Secondary file '{file_name}' (ID: {file_id}) from query did not end with '{secondary_suffix}'. Skipping.", file=sys.stderr)
            except KeyError as e:
                print(f"Skipping secondary item due to missing key {e} in JSON item: {item}", file=sys.stderr)
                continue
        
        print(f"\nIdentified {len(primary_files)} unique primary base names for pairing.", file=sys.stderr)
        print(f"Identified {len(secondary_files)} unique secondary base names for pairing.", file=sys.stderr)

        pairs: List[Tuple[str, str]] = []
        unpaired_primary_count = 0
        orphaned_secondary_count = 0

        for base_name, primary_id in sorted(primary_files.items()):
            if base_name in secondary_files:
                pairs.append((primary_id, secondary_files[base_name]))
            else:
                print(f"Warning: Primary file for base '{base_name}' (ID: {primary_id}) has no corresponding secondary file.", file=sys.stderr)
                unpaired_primary_count += 1

        for base_name, secondary_id in secondary_files.items():
            if base_name not in primary_files:
                print(f"Warning: Secondary file for base '{base_name}' (ID: {secondary_id}) has no corresponding primary file.", file=sys.stderr)
                orphaned_secondary_count += 1

        print(f"\nFound {len(pairs)} pairs.", file=sys.stderr)
        if unpaired_primary_count > 0:
            print(f"{unpaired_primary_count} primary files did not have a matching secondary file", file=sys.stderr)
        if orphaned_secondary_count > 0:
            print(f"{orphaned_secondary_count} secondary files did not have a matching primary file", file=sys.stderr)

        return pairs