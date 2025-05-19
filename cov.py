#!/usr/bin/env python3

import subprocess
import json
import sys
import os
from typing import List, Dict, Tuple, Optional

def run_dx_find_command(dx_command_args: List[str], command_description: str) -> List[Dict]:
    """
    Helper function to run a dx find data command, handle errors,
    and return parsed JSON data.
    """
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

def find_bam_bai_pairs(project_id: str) -> List[Tuple[str, str]]:
    """
    Finds *markdup.bam and *markdup.bam.bai files in the specified project,
    and returns a list of (bam_file_id, bai_file_id) tuples.
    """
    # Glob patterns for BAM and BAI files
    bam_glob_pattern = "*markdup.bam"
    bai_glob_pattern = "*markdup.bam.bai"

    # Define dx commands with project specification
    dx_command_bam_args = [
        "dx", "find", "data",
        "--name", bam_glob_pattern,
        "--class", "file",
        "--project", project_id,
        "--json"
    ]
    dx_command_bai_args = [
        "dx", "find", "data",
        "--name", bai_glob_pattern,
        "--class", "file",
        "--project", project_id,
        "--json"
    ]

    # Execute dx commands
    bam_files_data = run_dx_find_command(dx_command_bam_args, "BAM file query")
    bai_files_data = run_dx_find_command(dx_command_bai_args, "BAI file query")
    
    # Process found files
    bams: Dict[str, str] = {}  # {base_filename: bam_file_id}
    bais: Dict[str, str] = {}  # {base_filename: bai_file_id}

    print(f"\nProcessing {len(bam_files_data)} items from BAM query ('{bam_glob_pattern}').", file=sys.stderr)
    for item in bam_files_data:
        try:
            file_id = item['id']
            file_name = item['describe']['name']
            
            if file_name.endswith(".bam"):
                base_name = file_name[:-len(".bam")]
                bams[base_name] = file_id
            else:
                print(f"Warning: File '{file_name}' (ID: {file_id}) from BAM query did not end with '.bam'. Skipping.", file=sys.stderr)
        except KeyError as e:
            print(f"Skipping BAM item due to missing key {e} in JSON item: {item}", file=sys.stderr)
            continue
            
    print(f"Processing {len(bai_files_data)} items from BAI query ('{bai_glob_pattern}').", file=sys.stderr)
    for item in bai_files_data:
        try:
            file_id = item['id']
            file_name = item['describe']['name']

            if file_name.endswith(".bam.bai"):
                base_name = file_name[:-len(".bam.bai")]
                bais[base_name] = file_id
            else:
                print(f"Warning: File '{file_name}' (ID: {file_id}) from BAI query did not end with '.bam.bai'. Skipping.", file=sys.stderr)
        except KeyError as e:
            print(f"Skipping BAI item due to missing key {e} in JSON item: {item}", file=sys.stderr)
            continue
    
    print(f"\nIdentified {len(bams)} unique BAM base names for pairing.", file=sys.stderr)
    print(f"Identified {len(bais)} unique BAI base names for pairing.", file=sys.stderr)

    # Create pairs and track statistics
    pairs: List[Tuple[str, str]] = []
    unpaired_bams_count = 0
    orphaned_bai_count = 0

    # Create pairs
    for base_name, bam_id in sorted(bams.items()):
        if base_name in bais:
            pairs.append((bam_id, bais[base_name]))
        else:
            print(f"Warning: BAM file for base '{base_name}' (ID: {bam_id}) has no corresponding BAI file.", file=sys.stderr)
            unpaired_bams_count += 1

    # Check for orphaned BAI files
    for base_name, bai_id in bais.items():
        if base_name not in bams:
            print(f"Warning: BAI file for base '{base_name}' (ID: {bai_id}) has no corresponding BAM file.", file=sys.stderr)
            orphaned_bai_count += 1

    # Print summary
    print(f"\nFound {len(pairs)} BAM/BAI pairs", file=sys.stderr)
    if unpaired_bams_count > 0:
        print(f"{unpaired_bams_count} BAM files did not have a matching BAI file", file=sys.stderr)
    if orphaned_bai_count > 0:
        print(f"{orphaned_bai_count} BAI files did not have a matching BAM file", file=sys.stderr)

    return pairs

def generate_coverage_commands(bam_bai_pairs: List[Tuple[str, str]], output_file: str, project_id: str) -> None:
    """
    Generates coverage analysis commands for each BAM/BAI pair and writes them to the output file.
    
    Args:
        bam_bai_pairs: List of (bam_id, bai_id) tuples
        output_file: Path to the output file
        project_id: DNAnexus project ID to use as the destination
    """
    # Base command template
    base_command = (
        "dx run applet-G6vyyf00jy1kPkX9PJ1YkxB1 "
        "-icoverage_level=30 "
        "-ibamfile={bam_id} "
        "-ibam_index={bai_id} "
        "-imin_base_qual=10 "
        "-imin_mapping_qual=20 "
        "-iadditional_filter_commands=\"not (unmapped or secondary_alignment)\" "
        "-iexclude_duplicate_reads=true "
        "-iexclude_failed_quality_control=true "
        "-imerge_overlapping_mate_reads=true "
        "-isambamba_bed=project-ByfFPz00jy1fk6PjpZ95F27J:file-Gzj0Pyj0jy1VpJz3768kz8KY "
        f"--dest {project_id} -y"
    )

    try:
        with open(output_file, 'w') as f:
            for i, (bam_id, bai_id) in enumerate(bam_bai_pairs, 1):
                command = base_command.format(bam_id=bam_id, bai_id=bai_id)
                f.write(f"{command}\n")
                print(f"Generated command {i}/{len(bam_bai_pairs)} for BAM: {bam_id}", file=sys.stderr)

        print(f"\nSuccessfully wrote {len(bam_bai_pairs)} commands to {output_file}", file=sys.stderr)

    except IOError as e:
        print(f"Error writing to output file {output_file}: {e}", file=sys.stderr)
        sys.exit(1)

def get_project_name(project_id: str) -> Optional[str]:
    """
    Get project name from project ID using dx describe.
    Returns None if project name cannot be determined.
    """
    try:
        dx_describe = subprocess.run(["dx", "describe", project_id, "--json"], 
                                   capture_output=True, text=True, check=True)
        project_info = json.loads(dx_describe.stdout)
        return project_info.get("name")
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        print(f"Warning: Could not get project name from project ID: {e}", file=sys.stderr)
        return None

def main():
    """Main function to generate coverage commands from a DNAnexus project"""
    if len(sys.argv) != 2:
        print("Usage: python cov.py project-id", file=sys.stderr)
        print("Example: python cov.py project-ByfFPz00jy1fk6PjpZ95F27J", file=sys.stderr)
        sys.exit(1)

    project_id = sys.argv[1]
    if not project_id.startswith("project-"):
        print("Error: Project ID must start with 'project-'", file=sys.stderr)
        sys.exit(1)

    # Get project name for output filename
    project_name = get_project_name(project_id)
    if project_name:
        output_file = f"{project_name}_coverage_cmds.sh"
        print(f"Using project name '{project_name}' for output filename", file=sys.stderr)
    else:
        output_file = f"{project_id}_coverage_commands.sh"
        print(f"Could not determine project name, using project ID for output filename", file=sys.stderr)

    print(f"Starting coverage command generation for project: {project_id}", file=sys.stderr)
    print(f"Commands will be written to: {output_file}", file=sys.stderr)

    # Find BAM/BAI pairs
    bam_bai_pairs = find_bam_bai_pairs(project_id)

    if not bam_bai_pairs:
        print("No BAM/BAI pairs found. No commands will be generated.", file=sys.stderr)
        sys.exit(1)

    # Generate and write commands
    generate_coverage_commands(bam_bai_pairs, output_file, project_id)

if __name__ == "__main__":
    main() 