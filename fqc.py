#!/usr/bin/env python3

import subprocess
import json
import sys
import os
from typing import Dict, List, Optional, Tuple
from workflow import CommandGenerator  # Import the base class

class FastQCCommandGenerator(CommandGenerator):
    """Generates FastQC analysis commands for FASTQ pairs in a DNAnexus project"""

    def __init__(self, auth_token_path: str = "/usr/local/src/mokaguys/.dnanexus_auth_token"):
        self.auth_token_path = auth_token_path

    @property
    def name(self) -> str:
        return "FastQC Analysis"

    @property
    def description(self) -> str:
        return "Generate FastQC analysis commands for FASTQ pairs in a DNAnexus project"

    def generate(self) -> None:
        """Main method to generate FastQC commands"""
        if len(sys.argv) != 2:
            print("\nFastQC Analysis Configuration:")
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

        # Get project name for output filename
        project_name = self._get_project_name(project_id)
        if project_name:
            output_file = f"{project_name}_fastqc_cmds.sh"
            print(f"Using project name '{project_name}' for output filename")
        else:
            output_file = f"{project_id}_fastqc_commands.sh"
            print(f"Could not determine project name, using project ID for output filename")

        print(f"\nStarting FastQC command generation for project: {project_id}")
        print(f"Commands will be written to: {output_file}")

        # Find FASTQ pairs
        fastq_pairs = self._find_fastq_pairs(project_id)

        if not fastq_pairs:
            print("No FASTQ pairs found. No commands will be generated.")
            return

        # Generate and write commands
        self._generate_fastqc_commands(fastq_pairs, output_file, project_id)

    def _run_dx_find_command(self, dx_command_args: List[str], command_description: str) -> List[Dict]:
        """Helper function to run a dx find data command"""
        print(f"Executing: {' '.join(dx_command_args)}", file=sys.stderr)
        try:
            process = subprocess.run(dx_command_args, capture_output=True, text=True, check=False)
            
            if process.returncode != 0:
                print(f"Error executing {command_description} (return code {process.returncode}):", file=sys.stderr)
                print(f"Command: {' '.join(dx_command_args)}", file=sys.stderr)
                if process.stderr:
                    print(f"dx stderr:\n{process.stderr}", file=sys.stderr)
                if process.stdout and process.stdout.strip():
                    print(f"dx stdout (if error message present):\n{process.stdout}", file=sys.stderr)
                sys.exit(1)
            
            if not process.stdout.strip() and process.returncode == 0:
                print(f"No files found by {command_description}. Proceeding.", file=sys.stderr)
                return []

            return json.loads(process.stdout)

        except json.JSONDecodeError as e:
            print(f"Error parsing JSON output from {command_description}: {e}", file=sys.stderr)
            print(f"Raw output that caused error: >>>\n{process.stdout}\n<<<", file=sys.stderr)
            sys.exit(1)
        except FileNotFoundError:
            print(f"Error: dx command-line tool not found. Please ensure it's installed and in your PATH.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred while running {command_description}: {e}", file=sys.stderr)
            sys.exit(1)

    def _find_fastq_pairs(self, project_id: str) -> List[Tuple[str, str]]:
        """Finds R1/R2 FASTQ pairs in the project"""
        # Glob patterns for FASTQ files
        r1_glob_pattern = "*_R1.fastq.gz"
        r2_glob_pattern = "*_R2.fastq.gz"

        # Define dx commands with project specification
        dx_command_r1_args = [
            "dx", "find", "data",
            "--name", r1_glob_pattern,
            "--class", "file",
            "--project", project_id,
            "--json"
        ]
        dx_command_r2_args = [
            "dx", "find", "data",
            "--name", r2_glob_pattern,
            "--class", "file",
            "--project", project_id,
            "--json"
        ]

        # Execute dx commands
        r1_files_data = self._run_dx_find_command(dx_command_r1_args, "R1 FASTQ file query")
        r2_files_data = self._run_dx_find_command(dx_command_r2_args, "R2 FASTQ file query")
        
        # Process found files
        r1_files: Dict[str, str] = {}  # {base_filename: r1_file_id}
        r2_files: Dict[str, str] = {}  # {base_filename: r2_file_id}

        print(f"\nProcessing {len(r1_files_data)} items from R1 FASTQ query ('{r1_glob_pattern}').", file=sys.stderr)
        for item in r1_files_data:
            try:
                file_id = item['id']
                file_name = item['describe']['name']
                
                # Extract base name by removing R1 part and extension
                base_name = file_name.replace("_R1.", "_R#.")
                if base_name.endswith(".fastq.gz"):
                    base_name = base_name[:-9]
                r1_files[base_name] = file_id
            except KeyError as e:
                print(f"Skipping R1 item due to missing key {e} in JSON item: {item}", file=sys.stderr)
                continue
            
        print(f"Processing {len(r2_files_data)} items from R2 FASTQ query ('{r2_glob_pattern}').", file=sys.stderr)
        for item in r2_files_data:
            try:
                file_id = item['id']
                file_name = item['describe']['name']

                # Extract base name by removing R2 part and extension
                base_name = file_name.replace("_R2.", "_R#.")
                if base_name.endswith(".fastq.gz"):
                    base_name = base_name[:-9]
                r2_files[base_name] = file_id
            except KeyError as e:
                print(f"Skipping R2 item due to missing key {e} in JSON item: {item}", file=sys.stderr)
                continue
        
        print(f"\nIdentified {len(r1_files)} unique R1 FASTQ files for pairing.", file=sys.stderr)
        print(f"Identified {len(r2_files)} unique R2 FASTQ files for pairing.", file=sys.stderr)

        # Create pairs and track statistics
        pairs: List[Tuple[str, str]] = []
        unpaired_r1_count = 0
        orphaned_r2_count = 0

        # Create pairs
        for base_name, r1_id in sorted(r1_files.items()):
            if base_name in r2_files:
                pairs.append((r1_id, r2_files[base_name]))
            else:
                print(f"Warning: R1 FASTQ file for base '{base_name}' (ID: {r1_id}) has no corresponding R2 file.", file=sys.stderr)
                unpaired_r1_count += 1

        # Check for orphaned R2 files
        for base_name, r2_id in r2_files.items():
            if base_name not in r1_files:
                print(f"Warning: R2 FASTQ file for base '{base_name}' (ID: {r2_id}) has no corresponding R1 file.", file=sys.stderr)
                orphaned_r2_count += 1

        # Print summary
        print(f"\nFound {len(pairs)} FASTQ R1/R2 pairs", file=sys.stderr)
        if unpaired_r1_count > 0:
            print(f"{unpaired_r1_count} R1 FASTQ files did not have a matching R2 file", file=sys.stderr)
        if orphaned_r2_count > 0:
            print(f"{orphaned_r2_count} R2 FASTQ files did not have a matching R1 file", file=sys.stderr)

        return pairs

    def _generate_fastqc_commands(self, fastq_pairs: List[Tuple[str, str]], 
                                output_file: str, project_id: str) -> None:
        """Generates FastQC analysis commands"""
        # Base command template
        base_command = (
            "dx run applet-GKXqZV80jy1QxF4yKYB4Y3Kz "
            "-ireads={r1_id} "
            "-ireads={r2_id} "
            f"--dest {project_id} -y"
        )

        try:
            with open(output_file, 'w') as f:
                for i, (r1_id, r2_id) in enumerate(fastq_pairs, 1):
                    command = base_command.format(r1_id=r1_id, r2_id=r2_id)
                    f.write(f"{command}\n")
                    print(f"Generated command {i}/{len(fastq_pairs)} for FASTQ pair: {r1_id}, {r2_id}", file=sys.stderr)

            print(f"\nSuccessfully wrote {len(fastq_pairs)} commands to {output_file}", file=sys.stderr)

        except IOError as e:
            print(f"Error writing to output file {output_file}: {e}", file=sys.stderr)
            sys.exit(1)

    def _get_project_name(self, project_id: str) -> Optional[str]:
        """Get project name from project ID"""
        try:
            dx_describe = subprocess.run(["dx", "describe", project_id, "--json"], 
                                       capture_output=True, text=True, check=True)
            project_info = json.loads(dx_describe.stdout)
            return project_info.get("name")
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"Warning: Could not get project name from project ID: {e}", file=sys.stderr)
            return None 