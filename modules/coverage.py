#!/usr/bin/env python3

import sys
import os
from typing import List, Dict, Tuple, Optional
from modules.dx_command_generator import DXCommandGenerator

class CoverageCommandGenerator(DXCommandGenerator):
    """Generates coverage analysis commands for BAM/BAI pairs in a DNAnexus project"""

    def __init__(self):
        super().__init__()
        self.chanjo_sambamba_coverage = self.config_values.get('chanjo_sambamba_coverage')
        self.sambamba_bed = self.config_values.get('sambamba_bed')

    @property
    def name(self) -> str:
        return "Coverage Analysis"

    @property
    def description(self) -> str:
        return "Generate coverage analysis commands for BAM/BAI pairs in a DNAnexus project"

    def generate(self) -> None:
        """Main method to generate coverage commands"""
        project_id = self._get_project_id_from_input("Enter DNAnexus project ID")
        if not project_id:
            return

        project_name = self._get_project_name(project_id)
        if not project_name:
            project_name = f"{project_id}_unknown_project" # Fallback if name not found
            print(f"Could not determine project name, using '{project_name}' for output filename")
        else:
            print(f"Using project name '{project_name}' for output filename")
        
        output_file = f"{project_name.replace(' ', '_')}_coverage_cmds.sh"

        print(f"\nStarting coverage command generation for project: {project_id}")
        print(f"Commands will be written to: {output_file}")

        if not self._initialize_output_file(output_file, project_id, project_name, "Coverage Analysis Commands"):
            return

        bam_bai_pairs = self._find_bam_bai_pairs(project_id)

        if not bam_bai_pairs:
            print("No BAM/BAI pairs found. No commands will be generated.")
            return

        self._generate_coverage_commands(bam_bai_pairs, output_file, project_id)

    def _find_bam_bai_pairs(self, project_id: str) -> List[Tuple[str, str]]:
        """Finds BAM/BAI pairs in the project using common utility"""
        bam_glob_pattern = "*markdup.bam"
        bai_glob_pattern = "*markdup.bam.bai"

        bam_files_data = self._find_dx_files(project_id, bam_glob_pattern)
        bai_files_data = self._find_dx_files(project_id, bai_glob_pattern)
        
        return self._pair_dx_files(bam_files_data, ".bam", bai_files_data, ".bam.bai")

    def _generate_coverage_commands(self, bam_bai_pairs: List[Tuple[str, str]], 
                                     output_file: str, project_id: str) -> None:
        """Generates coverage analysis commands"""
        # Base command template
        base_command = (
            f"dx run {self.chanjo_sambamba_coverage} " # Use applet from config
            "-icoverage_level=30 "
            "-ibamfile={bam_id} "
            "-ibam_index={bai_id} "
            "-imin_base_qual=10 "
            "-imin_mapping_qual=20 "
            "-iadditional_filter_commands=\"not (unmapped or secondary_alignment)\" "
            "-iexclude_duplicate_reads=true "
            "-iexclude_failed_quality_control=true "
            "-imerge_overlapping_mate_reads=true "
            f"-isambamba_bed={self.sambamba_bed} " # Use sambamba_bed from config
            f"--dest {project_id} -y"
        )

        try:
            with open(output_file, 'a') as f: # Append to initialized file
                for i, (bam_id, bai_id) in enumerate(bam_bai_pairs, 1):
                    command = base_command.format(bam_id=bam_id, bai_id=bai_id)
                    f.write(f"{command}\n")
                    print(f"Generated command {i}/{len(bam_bai_pairs)} for BAM: {bam_id}", file=sys.stderr)

            print(f"\nSuccessfully wrote {len(bam_bai_pairs)} commands to {output_file}", file=sys.stderr)

        except IOError as e:
            print(f"Error writing to output file {output_file}: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    generator = CoverageCommandGenerator()
    generator.generate()