#!/usr/bin/env python3

from dx_command_generator import DXCommandGenerator # Changed import
import subprocess
import re
from typing import Optional, Set
import os
from datetime import datetime

class ReadcountCommandGenerator(DXCommandGenerator): # Inherit from DXCommandGenerator
    """Generates readcount command for exome depth analysis"""

    def __init__(self):
        super().__init__()

    @property
    def name(self) -> str:
        return "Readcount Generator"

    @property
    def description(self) -> str:
        return "Generate DNAnexus readcount command for exome depth analysis from RunManifest.csv"

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

            # Use inherited method
            project_id, project_name = self._detect_project_info(dxfile)
            if not project_id:
                project_id = "{PROJECT_ID_PLACEHOLDER}"
                print(f"Using placeholder for Project ID: {project_id}")
            if not project_name:
                project_name = "UNKNOWN_PROJECT"
                print(f"Using placeholder for Project Name: {project_name}")

            # Use inherited method
            pan_numbers = self._extract_pan_numbers(dxfile)
            if not pan_numbers:
                print("Error: Could not extract PAN numbers from the manifest file.")
                return

            # Use inherited method
            auth_token = self._get_auth_token()

            # Generate the command
            pan_numlist = ",".join(sorted(pan_numbers))
            output_filename = f"{project_name.replace(' ', '_')}_readcount_cmd.sh"

            try:
                with open(output_filename, 'w') as f:
                    f.write(f"""dx run project-ByfFPz00jy1fk6PjpZ95F27J:applet-GyQGKjQ0qG6x59F4f1qBFvgz --priority high -y --instance-type mem1_ssd1_v2_x8 --name "ED_Readcount-CP2" -ireference_genome=project-ByfFPz00jy1fk6PjpZ95F27J:file-B6ZY7VG2J35Vfvpkj8y0KZ01 -ibedfile=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/Pan5279_exomeDepth.bed -ibam_str="*markdup.ba*" -inormals_RData=project-J0Jb8Vj0Xj5JG61ZK66fjg47:file-J0Kzf3804zjJygFfFfJqXG6K -iproject_name="{project_name}" -ibamfile_pannumbers="{pan_numlist}" --instance-type mem1_ssd1_v2_x36 --dest="{project_id}" --brief -y --auth "{auth_token}"\n""")
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