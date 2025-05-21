#!/usr/bin/env python3

import subprocess
import json
import sys
import os
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from dx_command_generator import DXCommandGenerator # Changed import

class PicardCommandGenerator(DXCommandGenerator): # Inherit from DXCommandGenerator
    """Generates Picard analysis commands for BAM files in a DNAnexus project"""

    def __init__(self):
        super().__init__()

    @property
    def name(self) -> str:
        return "Picard Analysis"

    @property
    def description(self) -> str:
        return "Generate Picard analysis commands for BAM files in a DNAnexus project"

    def generate(self) -> None:
        """Main method to generate Picard commands"""
        if len(sys.argv) != 2:
            print("\nPicard Analysis Configuration:")
            print("---------------------------")
            project_id = input("Enter DNAnexus project ID (e.g., project-xxxx): ").strip()
            
            if not project_id:
                print("Error: No project ID provided.")
                return
            
            if not project_id.startswith("project-"):
                print("Error: Project ID must start with 'project-'")
                return
        else:
            project_id = sys.argv[1]

        # Use inherited method
        project_name = self._get_project_name(project_id)
        if project_name:
            output_file = f"{project_name}_picard_cmds.sh"
            print(f"Using project name '{project_name}' for output filename")
        else:
            output_file = f"{project_id}_picard_commands.sh"
            print(f"Could not determine project name, using project ID for output filename")

        print(f"\nStarting Picard command generation for project: {project_id}")
        print(f"Commands will be written to: {output_file}")

        # Find sorted BAM files
        bam_files = self._find_sorted_bams(project_id)

        if not bam_files:
            print("No sorted BAM files found. No commands will be generated.")
            return

        # Generate and write commands
        self._generate_picard_commands(bam_files, output_file, project_id)

    def _find_sorted_bams(self, project_id: str) -> List[str]:
        """Finds sorted BAM files in the project"""
        # Glob pattern for sorted BAM files
        bam_glob_pattern = "*markdup.bam"

        # Define dx command with project specification
        dx_command_bam_args = [
            "dx", "find", "data",
            "--name", bam_glob_pattern,
            "--class", "file",
            "--project", project_id,
            "--json"
        ]

        # Execute dx command using inherited method
        bam_files_data = self._run_dx_find_command(dx_command_bam_args, "BAM file query")
        
        # Process found files
        bam_files: List[str] = []

        print(f"\nProcessing {len(bam_files_data)} items from BAM query ('{bam_glob_pattern}').", file=sys.stderr)
        for item in bam_files_data:
            try:
                file_id = item['id']
                file_name = item['describe']['name']
                
                if file_name.endswith(".bam"):
                    bam_files.append(file_id)
                else:
                    print(f"Warning: File '{file_name}' (ID: {file_id}) from BAM query did not end with '.bam'. Skipping.", file=sys.stderr)
            except KeyError as e:
                print(f"Skipping BAM item due to missing key {e} in JSON item: {item}", file=sys.stderr)
                continue

        print(f"\nFound {len(bam_files)} sorted BAM files", file=sys.stderr)
        return bam_files

    def _generate_picard_commands(self, bam_files: List[str], output_file: str, project_id: str) -> None:
        """Generates Picard analysis commands"""
        # Base command template from picard_extract.py
        base_command = (
            "dx run applet-GQKxx1Q0jy1kFXjx5961Pb8j "
            "-isorted_bam={bam_id} "
            "-ifasta_index=project-ByfFPz00jy1fk6PjpZ95F27J:file-ByYgX700b80gf4ZY1GxvF3Jv "
            "-ivendor_exome_bedfile=project-ByfFPz00jy1fk6PjpZ95F27J:file-Gzj07J00jy1kVJXbFj8z67G0 "
            "-iCapture_panel=\"Hybridisation\" "
            f"--dest {project_id} -y"
        )

        try:
            with open(output_file, 'w') as f:
                # Write header
                f.write("#!/bin/bash\n")
                f.write(f"# Picard Analysis Commands\n")
                f.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Project ID: {project_id}\n\n")

                for i, bam_id in enumerate(bam_files, 1):
                    command = base_command.format(bam_id=bam_id)
                    f.write(f"{command}\n")
                    print(f"Generated command {i}/{len(bam_files)} for BAM: {bam_id}", file=sys.stderr)

            # Make the file executable
            os.chmod(output_file, 0o755)
            print(f"\nSuccessfully wrote {len(bam_files)} commands to {output_file}", file=sys.stderr)

        except IOError as e:
            print(f"Error writing to output file {output_file}: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    generator = PicardCommandGenerator()
    generator.generate()