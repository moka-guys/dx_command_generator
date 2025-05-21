#!/usr/bin/env python3

import subprocess
import json
import sys
import re
import os
from typing import List, Dict, Optional, Tuple, Set

class DXUtils:
    """
    A utility class for common DNAnexus interactions.
    """

    @staticmethod
    def run_dx_find_command(dx_command_args: List[str], command_description: str) -> List[Dict]:
        """
        Helper function to run a dx find data command and parse JSON output.
        Exits if there's an error.
        """
        print(f"Executing: {' '.join(dx_command_args)}", file=sys.stderr)
        try:
            process = subprocess.run(dx_command_args, capture_output=True, text=True, check=False)

            if process.returncode != 0:
                print(f"Error executing {command_description} (return code {process.returncode}):", file=sys.stderr)
                print(f"Command: {' '.join(dx_command_args)}", file=sys.stderr)
                if process.stderr:
                    print(f"dx stderr:\n{process.stderr}", file=sys.stderr)
                if process.stdout and process.stdout.strip():
                    print(f"dx stdout (if error message present):\n{process.stdout}", file=sys.stderr)
                sys.exit(1)

            if not process.stdout.strip() and process.returncode == 0:
                print(f"No files found by {command_description}. Proceeding.", file=sys.stderr)
                return []

            return json.loads(process.stdout)

        except json.JSONDecodeError as e:
            print(f"Error parsing JSON output from {command_description}: {e}", file=sys.stderr)
            print(f"Raw output that caused error: >>>\n{process.stdout}\n<<<", file=sys.stderr)
            sys.exit(1)
        except FileNotFoundError:
            print(f"Error: dx command-line tool not found. Please ensure it's installed and in your PATH.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred while running {command_description}: {e}", file=sys.stderr)
            sys.exit(1)

    @staticmethod
    def get_project_name(project_id: str) -> Optional[str]:
        """
        Get project name from project ID using 'dx describe'.
        """
        try:
            dx_describe = subprocess.run(["dx", "describe", project_id, "--json"],
                                         capture_output=True, text=True, check=True)
            project_info = json.loads(dx_describe.stdout)
            return project_info.get("name")
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"Warning: Could not get project name from project ID '{project_id}': {e}", file=sys.stderr)
            return None

    @staticmethod
    def detect_project_info(dx_file_id: str) -> Tuple[str, str]:
        """
        Detect project ID and name from a DNAnexus file ID using 'dx describe'.
        """
        project_id = ""
        project_name = ""

        print(f"Extracting project information from DNAnexus file {dx_file_id}...")

        try:
            dx_describe_cmd = ["dx", "describe", dx_file_id]
            print(f"Executing: {' '.join(dx_describe_cmd)}")
            dx_describe_output = subprocess.check_output(dx_describe_cmd, text=True, stderr=subprocess.PIPE)

            project_id_match = re.search(r"Project\s+(project-[a-zA-Z0-9]+)", dx_describe_output)
            if project_id_match:
                project_id = project_id_match.group(1)
                print(f"Detected Project ID: {project_id}")
            else:
                print("Warning: Could not detect Project ID from dx describe output.")

            folder_path_match = re.search(r"Folder\s+([^\n]+)", dx_describe_output)
            if folder_path_match:
                folder_path = folder_path_match.group(1).strip()
                project_name_candidate = folder_path.lstrip('/').split('/')[0]
                if project_name_candidate:
                    project_name = project_name_candidate
                    print(f"Detected Project Name (from folder path): {project_name}")
                else:
                    print("Warning: Folder path was '/' or empty, could not derive project name from folder.")
            else:
                print("Warning: Could not detect folder path from dx describe output.")

        except subprocess.CalledProcessError as e:
            print(f"Error: Failed to execute 'dx describe {dx_file_id}'. Return code: {e.returncode}")
            print(f"Command output: {e.output}")
            print(f"Command error: {e.stderr}")
            print("Please check your DNAnexus login status and if the file ID is correct.")
        except FileNotFoundError:
            print("Error: 'dx' command not found. Please ensure the DNAnexus toolkit is installed and in your PATH.")

        return project_id, project_name

    @staticmethod
    def extract_pan_numbers(dx_file_id: str) -> Optional[Set[str]]:
        """Extract unique Pan numbers from RunManifest.csv"""
        pan_numbers = set()

        try:
            # Use dx cat to read the manifest file
            dx_cat_cmd = ["dx", "cat", dx_file_id]
            manifest_content = subprocess.check_output(dx_cat_cmd, text=True, stderr=subprocess.PIPE)

            # Process each line
            for line in manifest_content.splitlines():
                line = line.strip()
                if line:  # Skip empty lines
                    # Look for Pan<number> pattern in the line
                    pan_match = re.search(r'Pan\d+', line, re.IGNORECASE)
                    if pan_match:
                        pan_numbers.add(pan_match.group(0))

        except subprocess.CalledProcessError as e:
            print(f"Error reading manifest file: {e}")
            print(f"Command error output: {e.stderr}")
        except Exception as e:
            print(f"An unexpected error occurred while extracting Pan numbers: {e}")

        return pan_numbers

    @staticmethod
    def get_auth_token(dnanexus_auth_token_path: str) -> str:
        """Get authentication token from file or use placeholder"""
        auth_token = ""
        try:
            if os.path.isfile(dnanexus_auth_token_path):
                with open(dnanexus_auth_token_path, 'r') as f:
                    auth_token = f.read().strip()
                if auth_token:
                    print(f"Successfully read auth token from {dnanexus_auth_token_path}")
                else:
                    print(f"Warning: Auth token file {dnanexus_auth_token_path} is empty. Using placeholder.")
                    auth_token = "{AUTH_TOKEN_PLACEHOLDER}"
            else:
                print(f"Error: Auth token file not found at {dnanexus_auth_token_path}. Using placeholder.")
                auth_token = "{AUTH_TOKEN_PLACEHOLDER}"
        except Exception as e:
            print(f"Error reading auth token from {dnanexus_auth_token_path}: {e}. Using placeholder.")
            auth_token = "{AUTH_TOKEN_PLACEHOLDER}"
        return auth_token