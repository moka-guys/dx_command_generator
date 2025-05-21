#!/usr/bin/env python3

import sys
import os
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from dx_command_generator import DXCommandGenerator

class PicardCommandGenerator(DXCommandGenerator):
    """Generates Picard analysis commands for BAM files in a DNAnexus project"""

    def __init__(self):
        super().__init__()
        self.picard_applet_id = self.config_values.get('picard_applet')
        self.picard_fasta_index = self.config_values.get('picard_fasta_index')
        self.picard_vendor_exome_bedfile = self.config_values.get('picard_vendor_exome_bedfile')


    @property
    def name(self) -> str:
        return "Picard Analysis"

    @property
    def description(self) -> str:
        return "Generate Picard analysis commands for BAM files in a DNAnexus project"

    def generate(self) -> None:
        """Main method to generate Picard commands"""
        project_id = self._get_project_id_from_input("Enter DNAnexus project ID")
        if not project_id:
            return

        project_name = self._get_project_name(project_id)
        if not project_name:
            project_name = f"{project_id}_unknown_project" # Fallback if name not found
            print(f"Could not determine project name, using '{project_name}' for output filename")
        else:
            print(f"Using project name '{project_name}' for output filename")
        
        output_file = f"{project_name.replace(' ', '_')}_picard_cmds.sh"

        print(f"\nStarting Picard command generation for project: {project_id}")
        print(f"Commands will be written to: {output_file}")

        if not self._initialize_output_file(output_file, project_id, project_name, "Picard Analysis Commands"):
            return

        bam_files = self._find_sorted_bams(project_id)

        if not bam_files:
            print("No sorted BAM files found. No commands will be generated.")
            return

        self._generate_picard_commands(bam_files, output_file, project_id)

    def _find_sorted_bams(self, project_id: str) -> List[str]:
        """Finds sorted BAM files in the project using common utility"""
        bam_glob_pattern = "*markdup.bam"
        bam_files_data = self._find_dx_files(project_id, bam_glob_pattern)
        
        return [item['id'] for item in bam_files_data if item['describe']['name'].endswith(".bam")]

    def _generate_picard_commands(self, bam_files: List[str], output_file: str, project_id: str) -> None:
        """Generates Picard analysis commands"""
        # Base command template from picard_extract.py
        base_command = (
            f"dx run {self.picard_applet_id} " # Use applet from config
            "-isorted_bam={bam_id} "
            f"-ifasta_index={self.picard_fasta_index} " # From config
            f"-ivendor_exome_bedfile={self.picard_vendor_exome_bedfile} " # From config
            "-iCapture_panel=\"Hybridisation\" "
            f"--dest {project_id} -y"
        )

        try:
            with open(output_file, 'a') as f: # Append to initialized file
                for i, bam_id in enumerate(bam_files, 1):
                    command = base_command.format(bam_id=bam_id)
                    f.write(f"{command}\n")
                    print(f"Generated command {i}/{len(bam_files)} for BAM: {bam_id}", file=sys.stderr)

            print(f"\nSuccessfully wrote {len(bam_files)} commands to {output_file}", file=sys.stderr)

        except IOError as e:
            print(f"Error writing to output file {output_file}: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    generator = PicardCommandGenerator()
    generator.generate()