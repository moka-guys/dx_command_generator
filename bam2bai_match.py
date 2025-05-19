#!/usr/bin/env python3
import subprocess
import json
import sys
import os

def run_dx_find_command(dx_command_args, command_description):
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
            # Also print stdout in case of error, as it might contain an error message from dxpy
            if process.stdout and process.stdout.strip(): # Check if stdout has content
                 print(f"dx stdout (if error message present):\n{process.stdout}", file=sys.stderr)
            sys.exit(1)
        
        # If stdout is empty but return code is 0, it might mean no files found.
        if not process.stdout.strip() and process.returncode == 0:
            print(f"No files found by {command_description}. Proceeding.", file=sys.stderr)
            return [] # Return empty list if no files found

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


def find_and_pair_bam_bai_separate_queries(output_filename="bam_bai_pairs.txt"):
    """
    Finds *markdup.bam and *markdup.bam.bai files using separate DNAnexus queries,
    and writes their file ID pairs (bam_file_id:bai_file_id) to the output file.
    """

    # Glob pattern for BAM files (DNAnexus search is typically case-insensitive for names)
    bam_glob_pattern = "*markdup.bam"
    # Glob pattern for BAI files
    bai_glob_pattern = "*markdup.bam.bai"

    # --- Define dx commands ---
    # You can add --path "/your/dx/folder/" to the lists below
    # if you want to target a specific folder.
    # By default, it searches the current DNAnexus project and folder.
    
    dx_command_bam_args = [
        "dx", "find", "data",
        "--name", bam_glob_pattern,
        "--class", "file",
        "--json"
    ]
    dx_command_bai_args = [
        "dx", "find", "data",
        "--name", bai_glob_pattern,
        "--class", "file",
        "--json"
    ]

    # --- Execute dx commands ---
    bam_files_data = run_dx_find_command(dx_command_bam_args, "BAM file query")
    bai_files_data = run_dx_find_command(dx_command_bai_args, "BAI file query")
    
    # --- Process found files ---
    bams = {}  # Stores {base_filename_for_pairing: bam_file_id}
    bais = {}  # Stores {base_filename_for_pairing: bai_file_id}

    print(f"\nProcessing {len(bam_files_data)} items from BAM query ('{bam_glob_pattern}').", file=sys.stderr)
    for item in bam_files_data:
        try:
            file_id = item['id']
            file_name = item['describe']['name'] # Filename without path
            
            # The glob pattern should ensure it ends with 'markdup.bam'.
            # We derive the base name by removing '.bam'.
            if file_name.endswith(".bam"): # Check for .bam suffix specifically
                base_name_for_pairing = file_name[:-len(".bam")]
                bams[base_name_for_pairing] = file_id
            else:
                # This case should be rare if the glob pattern works as expected.
                print(f"Warning: File '{file_name}' (ID: {file_id}) from BAM query did not end with '.bam'. Skipping.", file=sys.stderr)
        except KeyError as e:
            print(f"Skipping BAM item due to missing key {e} in JSON item: {item}", file=sys.stderr)
            continue
            
    print(f"Processing {len(bai_files_data)} items from BAI query ('{bai_glob_pattern}').", file=sys.stderr)
    for item in bai_files_data:
        try:
            file_id = item['id']
            file_name = item['describe']['name'] # Filename without path

            # The glob pattern should ensure it ends with 'markdup.bam.bai'.
            # We derive the base name by removing '.bam.bai'.
            if file_name.endswith(".bam.bai"): # Check for .bam.bai suffix specifically
                base_name_for_pairing = file_name[:-len(".bam.bai")]
                bais[base_name_for_pairing] = file_id
            else:
                # This case should be rare.
                print(f"Warning: File '{file_name}' (ID: {file_id}) from BAI query did not end with '.bam.bai'. Skipping.", file=sys.stderr)
        except KeyError as e:
            print(f"Skipping BAI item due to missing key {e} in JSON item: {item}", file=sys.stderr)
            continue
    
    print(f"\nIdentified {len(bams)} unique BAM base names for pairing.", file=sys.stderr)
    print(f"Identified {len(bais)} unique BAI base names for pairing.", file=sys.stderr)

    # --- Pair files and write to output ---
    paired_count = 0
    unpaired_bams_count = 0
    
    absolute_output_filename = os.path.abspath(output_filename)

    with open(absolute_output_filename, "w") as outfile:
        # Iterate through sorted base names from BAMs for consistent output order
        sorted_bam_base_names = sorted(bams.keys())

        for base_name in sorted_bam_base_names:
            bam_id = bams[base_name]
            if base_name in bais:
                bai_id = bais[base_name]
                outfile.write(f"{bam_id}:{bai_id}\n")
                paired_count += 1
            else:
                print(f"Warning: BAM file for base '{base_name}' (ID: {bam_id}) has no corresponding BAI file found from BAI query.", file=sys.stderr)
                unpaired_bams_count += 1
    
    print(f"\nScript finished.", file=sys.stderr)
    print(f"{paired_count} BAM/BAI pairs written to {absolute_output_filename}", file=sys.stderr)
    if unpaired_bams_count > 0:
        print(f"{unpaired_bams_count} BAM files did not have a matching BAI from the BAI query.", file=sys.stderr)
    
    orphaned_bai_count = 0
    for base_name_from_bai, bai_id_from_orphan_check in bais.items():
        if base_name_from_bai not in bams:
            orphaned_bai_count +=1
            print(f"Warning: BAI file for base '{base_name_from_bai}' (ID: {bai_id_from_orphan_check}) has no corresponding BAM file found from BAM query.", file=sys.stderr)
    if orphaned_bai_count > 0:
        print(f"{orphaned_bai_count} BAI files did not have a matching BAM from the BAM query.", file=sys.stderr)


if __name__ == "__main__":
    custom_output_file = sys.argv[1] if len(sys.argv) > 1 else "bam_bai_pairs.txt"
    find_and_pair_bam_bai_separate_queries(custom_output_file)