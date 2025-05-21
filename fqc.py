#!/usr/bin/env python3

import sys
import os
from typing import Dict, List, Optional, Tuple
from dx_command_generator import DXCommandGenerator

class FastQCCommandGenerator(DXCommandGenerator):
    """Generates FastQC analysis commands for FASTQ pairs in a DNAnexus project"""

    def __init__(self):
        super().__init__()
        self.fastqc_applet_id = self.config_values.get('fastqc_applet')

    @property
    def name(self) -> str:
        return "FastQC Analysis"

    @property
    def description(self) -> str:
        return "Generate FastQC analysis commands for FASTQ pairs in a DNAnexus project"

    def generate(self) -> None:
        """Main method to generate FastQC commands"""
        project_id = self._get_project_id_from_input("Enter DNAnexus project ID")
        if not project_id:
            return

        project_name = self._get_project_name(project_id)
        if not project_name:
            project_name = f"{project_id}_unknown_project" # Fallback if name not found
            print(f"Could not determine project name, using '{project_name}' for output filename")
        else:
            print(f"Using project name '{project_name}' for output filename")
        
        output_file = f"{project_name.replace(' ', '_')}_fastqc_cmds.sh"

        print(f"\nStarting FastQC command generation for project: {project_id}")
        print(f"Commands will be written to: {output_file}")

        if not self._initialize_output_file(output_file, project_id, project_name, "FastQC Analysis Commands"):
            return

        fastq_pairs = self._find_fastq_pairs(project_id)

        if not fastq_pairs:
            print("No FASTQ pairs found. No commands will be generated.")
            return

        self._generate_fastqc_commands(fastq_pairs, output_file, project_id)

    def _fastq_base_name_transform(self, filename: str) -> str:
        """Transforms filename to a common base name for R1/R2 pairing."""
        # Replace R1/R2 with R# for consistent base name extraction
        return filename.replace("_R1.", "_R#.").replace("_R2.", "_R#.")

    def _find_fastq_pairs(self, project_id: str) -> List[Tuple[str, str]]:
        """Finds R1/R2 FASTQ pairs in the project using common utility"""
        r1_glob_pattern = "*_R1.fastq.gz"
        r2_glob_pattern = "*_R2.fastq.gz"

        r1_files_data = self._find_dx_files(project_id, r1_glob_pattern)
        r2_files_data = self._find_dx_files(project_id, r2_glob_pattern)
        
        return self._pair_dx_files(r1_files_data, "_R1.fastq.gz", r2_files_data, "_R2.fastq.gz",
                                   base_name_transform=self._fastq_base_name_transform)

    def _generate_fastqc_commands(self, fastq_pairs: List[Tuple[str, str]], 
                                  output_file: str, project_id: str) -> None:
        """Generates FastQC analysis commands"""
        # Base command template
        base_command = (
            f"dx run {self.fastqc_applet_id} " # Use applet from config
            "-ireads={r1_id} "
            "-ireads={r2_id} "
            f"--dest {project_id} -y"
        )

        try:
            with open(output_file, 'a') as f: # Append to initialized file
                for i, (r1_id, r2_id) in enumerate(fastq_pairs, 1):
                    command = base_command.format(r1_id=r1_id, r2_id=r2_id)
                    f.write(f"{command}\n")
                    print(f"Generated command {i}/{len(fastq_pairs)} for FASTQ pair: {r1_id}, {r2_id}", file=sys.stderr)

            print(f"\nSuccessfully wrote {len(fastq_pairs)} commands to {output_file}", file=sys.stderr)

        except IOError as e:
            print(f"Error writing to output file {output_file}: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    generator = FastQCCommandGenerator()
    generator.generate()