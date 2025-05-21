#!/usr/bin/env python3

from modules.dx_command_generator import DXCommandGenerator
import subprocess
import re
from typing import Optional, Set
import os
from datetime import datetime

class ReadcountCommandGenerator(DXCommandGenerator):
    """Generates readcount command for exome depth analysis"""

    def __init__(self):
        super().__init__()
        self.readcount_applet_id = self.config_values.get('readcount_applet')
        self.reference_genome = self.config_values.get('reference_genome')
        self.normals_RData = self.config_values.get('normals_RData')
        self.common_data_project = self.config_values.get('common_data_project') # Used for bedfile path

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
            dxfile_id = input("Enter DNAnexus file ID (e.g., file-xxxx): ").strip()
            
            if not dxfile_id:
                print("Error: No file ID provided.")
                return
            
            if not dxfile_id.startswith("file-"):
                print("Error: Invalid DNAnexus file ID format. Must start with 'file-'")
                return

            project_id, project_name = self._detect_project_info(dxfile_id)
            if not project_id:
                project_id = "{PROJECT_ID_PLACEHOLDER}"
                print(f"Using placeholder for Project ID: {project_id}")
            if not project_name:
                project_name = "UNKNOWN_PROJECT"
                print(f"Using placeholder for Project Name: {project_name}")

            pan_numbers = self._extract_pan_numbers(dxfile_id)
            if not pan_numbers:
                print("Error: Could not extract PAN numbers from the manifest file.")
                return

            # auth_token is now retrieved inside _initialize_output_file if include_project_vars is True
            # or directly using self._get_auth_token()

            pan_numlist = ",".join(sorted(pan_numbers))
            output_filename = f"{project_name.replace(' ', '_')}_readcount_cmd.sh"

            # Initialize output file, including project vars
            if not self._initialize_output_file(output_filename, project_id, project_name, "Readcount Analysis Commands"):
                return

            try:
                with open(output_filename, 'a') as f: # Append to initialized file
                    command = (
                        f"dx run {self.readcount_applet_id} --priority high -y --instance-type mem1_ssd1_v2_x8 --name \"ED_Readcount-CP2\" "
                        f"-ireference_genome={self.reference_genome} "
                        f"-ibedfile={self.common_data_project}:/Data/BED/Pan5279_exomeDepth.bed " # Hardcoded BED, but project ID from config
                        f"-ibam_str=\"*markdup.ba*\" "
                        f"-inormals_RData={self.normals_RData} "
                        f"-iproject_name=\"${{PROJECT_NAME}}\" " # Use shell variable
                        f"-ibamfile_pannumbers=\"{pan_numlist}\" "
                        f"--instance-type mem1_ssd1_v2_x36 "
                        f"--dest=\"${{PROJECT_ID}}\" --brief -y --auth \"${{AUTH_TOKEN}}\"\n" # Use shell variables
                    )
                    f.write(command)
                
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

if __name__ == "__main__":
    generator = ReadcountCommandGenerator()
    generator.generate()