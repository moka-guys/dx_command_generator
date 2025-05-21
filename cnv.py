#!/usr/bin/env python3

import os
import re
import subprocess
import json
import sys
import importlib.util
from datetime import datetime
from typing import List, Dict, Set, Optional
from base import CommandGenerator

class CNVCommandGenerator(CommandGenerator):
    """Generates CNV analysis commands for samples in a DNAnexus project"""

    def __init__(self):
        super().__init__()
        self.panel_config = self._fetch_panel_config()

    def _fetch_panel_config(self) -> Dict:
        """Load the panel configuration from local file"""
        try:
            # Path to the local panel config
            config_path = "/usr/local/src/mokaguys/apps/automate_demultiplex/config/panel_config.py"
            
            # Load the module using importlib
            spec = importlib.util.spec_from_file_location("panel_config", config_path)
            if spec is None or spec.loader is None:
                raise ImportError("Could not load panel_config module")
                
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Get the PanelConfig class and access PANEL_DICT
            if hasattr(module, 'PanelConfig'):
                return module.PanelConfig.PANEL_DICT
            else:
                raise AttributeError("PanelConfig class not found in panel_config.py")
            
        except Exception as e:
            print(f"Warning: Failed to load panel configuration: {e}")
            print("Falling back to default configuration")
            return {}

    def _get_cnv_bedfile(self, pan_number: str) -> Optional[str]:
        """Get the CNV bedfile for a given pan number"""
        try:
            panel_info = self.panel_config.get(pan_number)
            if panel_info is None:
                raise ValueError(f"Pan number {pan_number} not found in panel configuration")
            
            cnv_bedfile = panel_info.get('ed_cnvcalling_bedfile')
            if cnv_bedfile:
                return f"project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{cnv_bedfile}_CNV.bed"
            else:
                print(f"Note: Pan number {pan_number} found but has no CNV bedfile configured - skipping CNV analysis")
                return None
                
        except Exception as e:
            print(f"Error: Failed to process {pan_number}: {e}")
            raise

    @property
    def name(self) -> str:
        return "CNV Analysis"

    @property
    def description(self) -> str:
        return "Generate CNV analysis commands for samples from RunManifest.csv"

    def generate(self) -> None:
        """Main method to generate CNV commands"""
        print("\nCNV Analysis Configuration:")
        print("---------------------------")
        print("Please provide the DNAnexus file-id for RunManifest.csv")

        try:
            dxfile = input("Enter DNAnexus file ID (e.g., file-xxxx): ").strip()
            
            if not dxfile:
                print("Error: No file ID provided.")
                return
            
            if not dxfile.startswith("file-"):
                print("Error: Invalid DNAnexus file ID format. Must start with 'file-'")
                return

            # Get project information from the manifest file
            project_id, project_name = self._detect_project_info(dxfile)
            
            if not project_id:
                print("Error: Could not detect project ID from the provided file.")
                return
                
            if not project_name:
                print("Error: Could not detect project name from the provided file.")
                return

            print(f"\nDetected Project Information:")
            print(f"  Project ID: {project_id}")
            print(f"  Project Name: {project_name}")

            # Extract Pan numbers from manifest
            pan_numbers = self._extract_pan_numbers(dxfile)
            if not pan_numbers:
                print("Error: No Pan numbers found in the manifest file.")
                return

            print(f"\nFound {len(pan_numbers)} unique Pan numbers:")
            for pan in sorted(pan_numbers):
                print(f"  - {pan}")

            # Find readcount file
            readcount_file = self._find_readcount_file(project_id)
            if not readcount_file:
                print("Error: Could not find .RData readcount file in the project.")
                return

            # Generate output file name
            output_file = f"{project_name.replace(' ', '_')}_cnv_cmds.sh"
            
            # Generate and write commands
            self._generate_cnv_commands(
                pan_numbers=pan_numbers,
                readcount_file=readcount_file,
                project_id=project_id,
                project_name=project_name,
                output_file=output_file
            )

        except EOFError:
            print("\nInput cancelled. Exiting.")
            return
        except KeyboardInterrupt:
            print("\nOperation interrupted. Exiting.")
            return

    def _detect_project_info(self, dx_file_id: str) -> tuple[str, str]:
        """Detect project information from DNAnexus file ID"""
        project_id = ""
        project_name = ""

        print(f"Extracting project information from DNAnexus file {dx_file_id}...")

        try:
            dx_describe_cmd = ["dx", "describe", dx_file_id]
            dx_describe_output = subprocess.check_output(dx_describe_cmd, text=True, stderr=subprocess.PIPE)

            project_id_match = re.search(r"Project\s+(project-[a-zA-Z0-9]+)", dx_describe_output)
            if project_id_match:
                project_id = project_id_match.group(1)

            folder_path_match = re.search(r"Folder\s+([^\n]+)", dx_describe_output)
            if folder_path_match:
                folder_path = folder_path_match.group(1).strip()
                project_name_candidate = folder_path.lstrip('/').split('/')[0]
                if project_name_candidate:
                    project_name = project_name_candidate

        except subprocess.CalledProcessError as e:
            print(f"Error executing 'dx describe {dx_file_id}'. Return code: {e.returncode}")
            print(f"Command output: {e.output}")
            print(f"Command error: {e.stderr}")
        except Exception as e:
            print(f"An unexpected error occurred while detecting project info: {e}")

        return project_id, project_name

    def _extract_pan_numbers(self, dx_file_id: str) -> Set[str]:
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

    def _find_readcount_file(self, project_id: str) -> Optional[str]:
        """Find the .RData readcount file in the project"""
        try:
            # Search for .RData files
            dx_find_cmd = [
                "dx", "find", "data",
                "--name", "*.RData",
                "--class", "file",
                "--project", project_id,
                "--brief"
            ]
            
            readcount_files = subprocess.check_output(dx_find_cmd, text=True, stderr=subprocess.PIPE).splitlines()
            
            if not readcount_files:
                print("No .RData files found in the project.")
                return None
                
            if len(readcount_files) > 1:
                print(f"Warning: Multiple .RData files found. Using the first one: {readcount_files[0]}")
                
            return readcount_files[0]

        except subprocess.CalledProcessError as e:
            print(f"Error searching for readcount file: {e}")
            print(f"Command error output: {e.stderr}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred while finding readcount file: {e}")
            return None

    def _generate_cnv_commands(self, pan_numbers: Set[str], readcount_file: str,
                             project_id: str, project_name: str, output_file: str) -> None:
        """Generate CNV analysis commands for each Pan number"""
        try:
            with open(output_file, 'w') as f:
                # Write header
                f.write("#!/bin/bash\n")
                f.write(f"# CNV Analysis Commands\n")
                f.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Project: {project_name} ({project_id})\n\n")

                # Track for dependency list
                f.write("# Initialize dependency tracking\n")
                f.write("DEPENDS_LIST=\"\"\n\n")

                # Generate command for each Pan number
                for pan_number in sorted(pan_numbers):
                    # Get the CNV bedfile - skip this pan if no bedfile configured
                    cnv_bedfile = self._get_cnv_bedfile(pan_number)
                    if cnv_bedfile is None:
                        continue
                        
                    command = (
                        f"JOB_ID_CNV_{pan_number}=$(dx run project-ByfFPz00jy1fk6PjpZ95F27J:applet-GybZV0006bZFBzgf54KP7BKj "
                        f"--priority high -y "
                        f"--name ED_CNVcalling-{pan_number} "
                        f"-ireadcount_file={readcount_file} "
                        f"-ibam_str=markdup "
                        f"-ireference_genome=project-ByfFPz00jy1fk6PjpZ95F27J:file-B6ZY7VG2J35Vfvpkj8y0KZ01 "
                        f"-isamplename_str=_markdup.bam "
                        f"-isubpanel_bed={cnv_bedfile} "
                        f"-iproject_name={project_name} "
                        f"-ibamfile_pannumbers={pan_number} "
                        f"--dest={project_id} --brief -y)\n"
                    )
                    
                    # Add job tracking
                    f.write(f"\n# Process {pan_number}\n")
                    f.write(command)
                    f.write(f"""
if [ -z "${{JOB_ID_CNV_{pan_number}}}" ]; then
    echo "ERROR: Failed to submit CNV job for {pan_number}. Check dx toolkit output."
else
    echo "Successfully submitted CNV job for {pan_number}: ${{JOB_ID_CNV_{pan_number}}}"
    DEPENDS_LIST="${{DEPENDS_LIST}} -d ${{JOB_ID_CNV_{pan_number}}}"
fi
""")

                # Make executable
                os.chmod(output_file, 0o755)
                print(f"\nSuccessfully generated commands for CNV analysis")
                print(f"Output written to: {output_file}")

        except IOError as e:
            print(f"Error writing to output file {output_file}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while generating commands: {e}")

if __name__ == "__main__":
    generator = CNVCommandGenerator()
    generator.generate() 