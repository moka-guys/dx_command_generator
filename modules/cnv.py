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
                raise ValueError(f"Pan number {pan_number} not found in panel configuration")
                
            cnv_bedfile = panel_info.get('ed_cnvcalling_bedfile')
            if cnv_bedfile:
                # Use common_data_project from config
                return f"{self.common_data_project}:/Data/BED/{cnv_bedfile}_CNV.bed"
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
            print(f"Warning: Multiple .RData files found. Using the first one: {readcount_files_data[0]['id']}")
            
        return readcount_files_data[0]['id']

    def _generate_cnv_commands(self, pan_numbers: Set[str], readcount_file: str,
                                 project_id: str, project_name: str, output_file: str) -> None:
        """Generate CNV analysis commands for each Pan number"""
        try:
            with open(output_file, 'a') as f: # Append to initialized file
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
                        f"JOB_ID_CNV_{pan_number}=$(dx run {self.cnv_applet_id} " # Use applet from config
                        f"--priority high -y "
                        f"--name ED_CNVcalling-{pan_number} "
                        f"-ireadcount_file={readcount_file} "
                        f"-ibam_str=markdup "
                        f"-ireference_genome={self.reference_genome} " # Use reference genome from config
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

                print(f"\nSuccessfully generated commands for CNV analysis")
                print(f"Output written to: {output_file}")

        except IOError as e:
            print(f"Error writing to output file {output_file}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while generating commands: {e}")

if __name__ == "__main__":
    generator = CNVCommandGenerator()
    generator.generate()