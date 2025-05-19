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


def find_and_pair_fastq_files(output_filename="fastq_pairs.txt"):
    """
    Finds R1.fastq.gz and R2.fastq.gz files using DNAnexus queries,
    and writes their file ID pairs (r1_file_id:r2_file_id) to the output file.
    """

    # Glob pattern for FASTQ files
    r1_glob_pattern = "*_R1.fastq.gz"
    r2_glob_pattern = "*_R2.fastq.gz"

    # --- Define dx commands ---
    # You can add --path "/your/dx/folder/" to the lists below
    # if you want to target a specific folder.
    # By default, it searches the current DNAnexus project and folder.
    
    dx_command_r1_args = [
        "dx", "find", "data",
        "--name", r1_glob_pattern,
        "--class", "file",
        "--json"
    ]
    dx_command_r2_args = [
        "dx", "find", "data",
        "--name", r2_glob_pattern,
        "--class", "file",
        "--json"
    ]

    # --- Execute dx commands ---
    r1_files_data = run_dx_find_command(dx_command_r1_args, "R1 FASTQ file query")
    r2_files_data = run_dx_find_command(dx_command_r2_args, "R2 FASTQ file query")
    
    # --- Process found files ---
    r1_files = {}  # Stores {base_filename_for_pairing: r1_file_id}
    r2_files = {}  # Stores {base_filename_for_pairing: r2_file_id}

    print(f"\nProcessing {len(r1_files_data)} items from R1 FASTQ query ('{r1_glob_pattern}').", file=sys.stderr)
    for item in r1_files_data:
        try:
            file_id = item['id']
            file_name = item['describe']['name'] # Filename without path
            
            # Extract the base sample name by removing the R1 part and file extension
            # The pattern appears to be "_R1." not "_R1_" based on the error messages
            base_name = file_name.replace("_R1.", "_R#.")
            # Remove the file extension
            if base_name.endswith(".fastq.gz"):
                base_name = base_name[:-9]  # Remove ".fastq.gz"
            r1_files[base_name] = file_id
        except KeyError as e:
            print(f"Skipping R1 item due to missing key {e} in JSON item: {item}", file=sys.stderr)
            continue
            
    print(f"Processing {len(r2_files_data)} items from R2 FASTQ query ('{r2_glob_pattern}').", file=sys.stderr)
    for item in r2_files_data:
        try:
            file_id = item['id']
            file_name = item['describe']['name'] # Filename without path

            # Extract the base sample name by removing the R2 part and file extension
            # The pattern appears to be "_R2." not "_R2_" based on the error messages
            base_name = file_name.replace("_R2.", "_R#.")
            # Remove the file extension
            if base_name.endswith(".fastq.gz"):
                base_name = base_name[:-9]  # Remove ".fastq.gz"
            r2_files[base_name] = file_id
        except KeyError as e:
            print(f"Skipping R2 item due to missing key {e} in JSON item: {item}", file=sys.stderr)
            continue
    
    print(f"\nIdentified {len(r1_files)} unique R1 FASTQ files for pairing.", file=sys.stderr)
    print(f"Identified {len(r2_files)} unique R2 FASTQ files for pairing.", file=sys.stderr)

    # --- Pair files and write to output ---
    paired_count = 0
    unpaired_r1_count = 0
    
    absolute_output_filename = os.path.abspath(output_filename)

    with open(absolute_output_filename, "w") as outfile:
        # Iterate through sorted base names from R1 files for consistent output order
        sorted_r1_base_names = sorted(r1_files.keys())

        for base_name in sorted_r1_base_names:
            r1_id = r1_files[base_name]
            if base_name in r2_files:
                r2_id = r2_files[base_name]
                outfile.write(f"{r1_id}:{r2_id}\n")
                paired_count += 1
            else:
                print(f"Warning: R1 FASTQ file for base '{base_name}' (ID: {r1_id}) has no corresponding R2 file.", file=sys.stderr)
                unpaired_r1_count += 1
    
    print(f"\nScript finished.", file=sys.stderr)
    print(f"{paired_count} R1/R2 FASTQ pairs written to {absolute_output_filename}", file=sys.stderr)
    if unpaired_r1_count > 0:
        print(f"{unpaired_r1_count} R1 FASTQ files did not have a matching R2 file.", file=sys.stderr)
    
    orphaned_r2_count = 0
    for base_name_from_r2, r2_id_from_orphan_check in r2_files.items():
        if base_name_from_r2 not in r1_files:
            orphaned_r2_count +=1
            print(f"Warning: R2 FASTQ file for base '{base_name_from_r2}' (ID: {r2_id_from_orphan_check}) has no corresponding R1 file.", file=sys.stderr)
    if orphaned_r2_count > 0:
        print(f"{orphaned_r2_count} R2 FASTQ files did not have a matching R1 file.", file=sys.stderr)


if __name__ == "__main__":
    # Fix argument parsing to properly handle the --output flag
    if len(sys.argv) > 1:
        if sys.argv[1] == "--output" and len(sys.argv) > 2:
            custom_output_file = sys.argv[2]
        else:
            custom_output_file = sys.argv[1]
    else:
        custom_output_file = "fastq_pairs.txt"
    
    find_and_pair_fastq_files(custom_output_file)