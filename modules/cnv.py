#!/usr/bin/env python3

import os
import re
import subprocess
import json
import sys
import importlib.util
from datetime import datetime
from typing import List, Dict, Set, Optional
from modules.dx_command_generator import DXCommandGenerator

class CNVCommandGenerator(DXCommandGenerator):
    """Generates CNV analysis commands for samples in a DNAnexus project"""

    def __init__(self):
        super().__init__()
        self.panel_config = self._fetch_panel_config()
        self.cnv_applet_id = self.config_values.get('cnv_applet')
        self.common_data_project = self.config_values.get('common_data_project')
        self.reference_genome = self.config_values.get('reference_genome')


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
                print(f"Warning: Pan number {pan_number} not found in panel configuration. No BED file can be retrieved.", file=sys.stderr)
                return None
                
            cnv_bedfile = panel_info.get('ed_cnvcalling_bedfile')
            if cnv_bedfile:
                # Use common_data_project from config
                return f"{self.common_data_project}:/Data/BED/{cnv_bedfile}_CNV.bed"
            else:
                print(f"Note: Pan number {pan_number} found but has no CNV bedfile configured - skipping CNV analysis", file=sys.stderr)
                return None
                
        except Exception as e:
            print(f"Error: Failed to process {pan_number}: {e}", file=sys.stderr)
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
            dxfile_id = input("Enter DNAnexus file ID (e.g., file-xxxx): ").strip()
            
            if not dxfile_id:
                print("Error: No file ID provided.")
                return
            
            if not dxfile_id.startswith("file-"):
                print("Error: Invalid DNAnexus file ID format. Must start with 'file-'")
                return

            project_id, project_name = self._detect_project_info(dxfile_id)
            
            if not project_id or not project_name:
                print("Error: Could not detect project information from the provided file.")
                return

            print(f"\nDetected Project Information:")
            print(f"  Project ID: {project_id}")
            print(f"  Project Name: {project_name}")

            pan_numbers = self._extract_pan_numbers(dxfile_id)
            if not pan_numbers:
                print("Error: No Pan numbers found in the manifest file.")
                return

            print(f"\nFound {len(pan_numbers)} unique Pan numbers:")
            for pan in sorted(pan_numbers):
                print(f"  - {pan}")

            readcount_file = self._find_readcount_file(project_id)
            if not readcount_file:
                print("Error: Could not find .RData readcount file in the project.")
                return

            output_file = f"{project_name.replace(' ', '_')}_cnv_cmds.sh"
            
            if not self._initialize_output_file(output_file, project_id, project_name, "CNV Analysis Commands", include_project_vars=False):
                return

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
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return

    def _find_readcount_file(self, project_id: str) -> Optional[str]:
        """Find the .RData readcount file in the project"""
        readcount_files_data = self._find_dx_files(project_id, "*.RData")
        
        if not readcount_files_data:
            print("No .RData files found in the project.")
            return None
            
        if len(readcount_files_data) > 1:
            print(f"Warning: Multiple .RData files found. Using the first one: {readcount_files_data[0]['id']}", file=sys.stderr)
            
        return readcount_files_data[0]['id']

    def _generate_cnv_commands(self, pan_numbers: Set[str], readcount_file: str,
                                 project_id: str, project_name: str, output_file: str) -> None:
        """Generate CNV analysis commands for each Pan number"""
        try:
            with open(output_file, 'a') as f: # Append to initialized file
                for pan_number in sorted(pan_numbers):
                    # Get the CNV bedfile - skip this pan if no bedfile configured
                    cnv_bedfile = self._get_cnv_bedfile(pan_number)
                    if cnv_bedfile is None:
                        continue
                        
                    command = (
                        f"dx run {self.cnv_applet_id} " # Use applet from config
                        f"--priority high -y "
                        f"--name ED_CNVcalling-{pan_number} "
                        f"-ireadcount_file={readcount_file} "
                        f"-ibam_str=markdup "
                        f"-ireference_genome={self.reference_genome} " # Use reference genome from config
                        f"-isamplename_str=_markdup.bam "
                        f"-isubpanel_bed={cnv_bedfile} "
                        f"-iproject_name={project_name} "
                        f"-ibamfile_pannumbers={pan_number} "
                        f"--dest={project_id} --brief -y\n" # Original format: single line, no JOB_ID, no DEPENDS_LIST
                    )
                    f.write(command)

                print(f"\nSuccessfully generated commands for CNV analysis")
                print(f"Output written to: {output_file}")

        except IOError as e:
            print(f"Error writing to output file {output_file}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while generating commands: {e}")

class CNVReanalysisCommandGenerator(CNVCommandGenerator):
    """Generates CNV reanalysis commands for a specific sample and new panel"""

    @property
    def name(self) -> str:
        return "CNV ExomeDepth Reanalysis"

    @property
    def description(self) -> str:
        return "Generate a single CNV reanalysis command for a sample with a new Pan number/BED file"

    def _find_original_pan_for_sample(self, dxfile_id: str, sample_identifier: str) -> Optional[str]:
        """
        Reads RunManifest.csv to find the original Pan number for a given sample identifier.
        Assumes sample identifier is the first element in a comma-separated line,
        and Pan number is in the format 'PanXXXX' later in the line.
        """
        print(f"Searching RunManifest.csv ({dxfile_id}) for original Pan number for sample: {sample_identifier}")
        try:
            dx_cat_cmd = ["dx", "cat", dxfile_id]
            # Capture stderr to suppress dx tool messages unless an actual error occurs
            process = subprocess.run(dx_cat_cmd, capture_output=True, text=True, check=False)

            if process.returncode != 0:
                print(f"Error reading manifest file {dxfile_id}: {process.stderr}", file=sys.stderr)
                return None

            manifest_content = process.stdout

            for line in manifest_content.splitlines():
                line = line.strip()
                if not line:
                    continue

                # Looks for the sample_identifier (case-insensitive) anywhere in the line,
                # then captures the first 'Pan\d+' found in that same line.
                if sample_identifier in line: # Simple check first to narrow down lines
                    pan_match = re.search(r'(Pan\d+)', line, re.IGNORECASE)
                    if pan_match:
                        original_pan = pan_match.group(1)
                        print(f"Found original Pan number for {sample_identifier}: {original_pan}")
                        return original_pan
            
            print(f"Warning: Original Pan number for sample {sample_identifier} not found in RunManifest.csv.")
            return None

        except FileNotFoundError:
            print(f"Error: 'dx' command not found. Please ensure the DNAnexus toolkit is installed and in your PATH.", file=sys.stderr)
            return None
        except Exception as e:
            print(f"An unexpected error occurred while extracting original Pan number: {e}", file=sys.stderr)
            return None

    def generate(self) -> None:
        """Main method to generate a single CNV reanalysis command"""
        print("\nCNV ExomeDepth Reanalysis Configuration:")
        print("--------------------------------------")
        print("Please provide details for the reanalysis request.")

        try:
            dxfile_id = input("Enter DNAnexus file ID for RunManifest.csv (e.g., file-xxxx): ").strip()
            if not dxfile_id.startswith("file-"):
                print("Error: Invalid DNAnexus file ID format. Must start with 'file-'")
                return
            
            # The prompt implies the sample identifier is a 6-digit number for the reanalysis.
            sample_identifier = input("Enter sample identifier (e.g., 123456 or NGS0001_R1.001): ").strip()
            if not sample_identifier:
                print("Error: Sample identifier cannot be empty.")
                return
            
            new_pan_number = input("Enter NEW Pan number for reanalysis (e.g., Pan1234): ").strip()
            if not re.fullmatch(r'Pan\d+', new_pan_number, re.IGNORECASE):
                print("Error: Invalid NEW Pan number format. Must be 'Pan' followed by digits (e.g., Pan1234).")
                return

            project_id, project_name = self._detect_project_info(dxfile_id)
            if not project_id or not project_name:
                print("Error: Could not detect project information from the provided file.")
                return

            print(f"\nDetected Project Information:")
            print(f"  Project ID: {project_id}")
            print(f"  Project Name: {project_name}")

            readcount_file = self._find_readcount_file(project_id)
            if not readcount_file:
                print("Error: Could not find .RData readcount file in the project.")
                return

            cnv_bedfile = self._get_cnv_bedfile(new_pan_number)
            if cnv_bedfile is None:
                print(f"Error: No CNV bedfile configured for NEW Pan number {new_pan_number}. Cannot proceed with reanalysis.")
                return

            # Get the original Pan number from the manifest for the specific sample
            original_pan_number = self._find_original_pan_for_sample(dxfile_id, sample_identifier)
            if original_pan_number is None:
                print(f"Error: Could not find original Pan number for sample {sample_identifier} in the manifest. Cannot proceed.")
                return
            

            output_file = f"{project_name.replace(' ', '_')}_cnv_reanalysis_cmds.sh"

            if not self._initialize_output_file(output_file, project_id, project_name, "CNV ExomeDepth Reanalysis Commands", include_project_vars=False):
                return

            with open(output_file, 'a') as f:
                command = (
                    f"JOB_ID_CNV_REANALYSIS_{original_pan_number}=$(dx run {self.cnv_applet_id} "
                    f"--priority high -y "
                    f"--name ED_CNVcallingREANALYSIS-{new_pan_number} "
                    f"-ireadcount_file={readcount_file} "
                    f"-ibam_str=markdup "
                    f"-ireference_genome={self.reference_genome} "
                    f"-isamplename_str=_markdup.bam "
                    f"-isubpanel_bed={cnv_bedfile} "
                    f"-iproject_name={project_name} "
                    f"-ibamfile_pannumbers={original_pan_number} "
                    f"--dest={project_id}:/exomedepth_output/{new_pan_number} --brief -y)\n"
                )
                f.write(command)
                print(f"\nGenerated CNV reanalysis command for sample {sample_identifier} (Original Pan: {original_pan_number}) with NEW panel {new_pan_number}")
                print(f"Output written to: {output_file}")

        except EOFError:
            print("\nInput cancelled. Exiting.")
            return
        except KeyboardInterrupt:
            print("\nOperation interrupted. Exiting.")
            return
        except Exception as e:
            print(f"An unexpected error occurred during reanalysis command generation: {e}")
            return

if __name__ == "__main__":
    generator = CNVCommandGenerator()
    generator.generate()