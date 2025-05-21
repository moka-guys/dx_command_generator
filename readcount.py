#!/usr/bin/env python3

from base import CommandGenerator
import subprocess
import re
from typing import Optional, Set
import os
from datetime import datetime

class ReadcountCommandGenerator(CommandGenerator):
    """Generates readcount command for exome depth analysis"""

    def __init__(self):
        super().__init__()

    @property
    def name(self) -> str:
        return "Readcount Generator"

    @property
    def description(self) -> str:
        return "Generate DNAnexus readcount command for exome depth analysis from RunManifest.csv"

    def _get_auth_token(self) -> str:
        """Get authentication token from file or use placeholder"""
        auth_token = ""
        try:
            with open(self.auth_token_path, 'r') as f:
                auth_token = f.read().strip()
            if auth_token:
                print(f"Successfully read auth token from {self.auth_token_path}")
            else:
                print(f"Warning: Auth token file {self.auth_token_path} is empty. Using placeholder.")
                auth_token = "{AUTH_TOKEN_PLACEHOLDER}"
        except Exception as e:
            print(f"Error reading auth token from {self.auth_token_path}: {e}. Using placeholder.")
            auth_token = "{AUTH_TOKEN_PLACEHOLDER}"
        return auth_token

    def _detect_project_info(self, dx_file_id: str) -> tuple[str, str]:
        """Detect project information from DNAnexus file ID"""
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

    def _extract_pan_numbers(self, dx_file_id: str) -> Optional[Set[str]]:
        """Extract unique PAN numbers from sample names in the manifest file"""
        print(f"Extracting PAN numbers from samples in DNAnexus file: {dx_file_id}")
        pan_numbers = set()

        try:
            dx_cat_cmd = ["dx", "cat", dx_file_id]
            print(f"Executing: {' '.join(dx_cat_cmd)}")
            dx_cat_output = subprocess.check_output(dx_cat_cmd, text=True, stderr=subprocess.PIPE)

            for line in dx_cat_output.splitlines():
                line = line.strip()
                # Look for Pan<digits> in the line
                pan_match = re.search(r'Pan\d+', line, re.IGNORECASE)
                if pan_match:
                    pan_numbers.add(pan_match.group(0))

            if not pan_numbers:
                print(f"Warning: No PAN numbers found in the DNAnexus file '{dx_file_id}'.")
                return None

            print(f"Found {len(pan_numbers)} unique PAN numbers: {', '.join(sorted(pan_numbers))}")
            return pan_numbers

        except subprocess.CalledProcessError as e:
            print(f"Error: Failed to execute 'dx cat {dx_file_id}'. Return code: {e.returncode}")
            print(f"Command output: {e.output}")
            print(f"Command error: {e.stderr}")
            print("Please check your DNAnexus login status and if the file ID is correct.")
            return None
        except FileNotFoundError:
            print("Error: 'dx' command not found. Please ensure the DNAnexus toolkit is installed and in your PATH.")
            return None
        except Exception as e:
            print(f"An unexpected error occurred while extracting PAN numbers: {e}")
            return None

    def generate(self) -> None:
        """Generate the readcount command"""
        print("\nReadcount Command Generator Configuration:")
        print("----------------------------------------")
        print("Please provide the DNAnexus file-id for RunManifest.csv")

        try:
            dxfile = input("Enter DNAnexus file ID (e.g., file-xxxx): ").strip()
            
            if not dxfile:
                print("Error: No file ID provided.")
                return
            
            if not dxfile.startswith("file-"):
                print("Error: Invalid DNAnexus file ID format. Must start with 'file-'")
                return

            # Get project information
            project_id, project_name = self._detect_project_info(dxfile)
            if not project_id:
                project_id = "{PROJECT_ID_PLACEHOLDER}"
                print(f"Using placeholder for Project ID: {project_id}")
            if not project_name:
                project_name = "UNKNOWN_PROJECT"
                print(f"Using placeholder for Project Name: {project_name}")

            # Get PAN numbers
            pan_numbers = self._extract_pan_numbers(dxfile)
            if not pan_numbers:
                print("Error: Could not extract PAN numbers from the manifest file.")
                return

            # Get auth token
            auth_token = self._get_auth_token()

            # Generate the command
            pan_numlist = ",".join(sorted(pan_numbers))
            output_filename = f"{project_name.replace(' ', '_')}_readcount_cmd.sh"

            try:
                with open(output_filename, 'w') as f:
                    f.write(f"""#!/bin/bash
# DNAnexus Readcount Command
# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Command Type: {self.name}

# --- Configuration ---
AUTH_TOKEN="{auth_token}"
PROJECT_ID="{project_id}"
PROJECT_NAME="{project_name}"
PAN_NUMBERS="{pan_numlist}"

echo "Starting readcount command execution..."
echo "Auth Token: ${{AUTH_TOKEN}}"
echo "Project ID: ${{PROJECT_ID}}"
echo "Project Name: ${{PROJECT_NAME}}"
echo "PAN Numbers: ${{PAN_NUMBERS}}"
echo "----------------------------------------"

# Execute readcount command
dx run project-ByfFPz00jy1fk6PjpZ95F27J:applet-GyQGKjQ0qG6x59F4f1qBFvgz \\
    --priority high -y \\
    --instance-type mem1_ssd1_v2_x8 \\
    --name "ED_Readcount-CP2" \\
    -ireference_genome=project-ByfFPz00jy1fk6PjpZ95F27J:file-B6ZY7VG2J35Vfvpkj8y0KZ01 \\
    -ibedfile=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/Pan5279_exomeDepth.bed \\
    -ibam_str="*markdup.ba*" \\
    -inormals_RData=project-J0Jb8Vj0Xj5JG61ZK66fjg47:file-J0Kzf3804zjJygFfFfJqXG6K \\
    -iproject_name="${{PROJECT_NAME}}" \\
    -ibamfile_pannumbers="${{PAN_NUMBERS}}" \\
    --instance-type mem1_ssd1_v2_x36 \\
    --dest="${{PROJECT_ID}}" \\
    --brief -y \\
    --auth "${{AUTH_TOKEN}}"

echo "----------------------------------------"
echo "Command generation finished."
echo "Output script: {output_filename}"
echo "To run the generated command: bash {output_filename}"
""")
                os.chmod(output_filename, 0o755)  # Make executable
                print(f"\nGenerated readcount command script: {output_filename}")
                print(f"To execute the command, run:\n  bash {os.path.abspath(output_filename)}")

            except IOError as e:
                print(f"Error: Could not write to output file {output_filename}: {e}")
                return

        except EOFError:
            print("\nInput cancelled. Exiting command generation.")
            return
        except KeyboardInterrupt:
            print("\nOperation interrupted by user. Exiting command generation.")
            return
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            print("Exiting command generation.")
            return 