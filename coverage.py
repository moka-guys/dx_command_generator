#!/usr/bin/env python3

def replace_file_id(input_file_path, output_file_path):
    """
    Reads file IDs from input_file_path, replaces the BAM and BAI file IDs in the command,
    and writes the full commands to output_file_path.
    """
    # Original command with the file IDs to be replaced
    base_command = (
        "dx run applet-G6vyyf00jy1kPkX9PJ1YkxB1 "
        "-icoverage_level=30 "
        "-ibamfile=BAMFILE "
        "-ibam_index=BAMINDEX "
        "-imin_base_qual=10 "
        "-imin_mapping_qual=20 "
        "-iadditional_filter_commands=\"not (unmapped or secondary_alignment)\" "
        "-iexclude_duplicate_reads=true "
        "-iexclude_failed_quality_control=true "
        "-imerge_overlapping_mate_reads=true "
        "-isambamba_bed=project-ByfFPz00jy1fk6PjpZ95F27J:file-Gzj0Pyj0jy1VpJz3768kz8KY "
        "--dest project-J0XqG0Q0Qj9xF1zJGfj53QgV -y"
    )
    
    # File IDs to be replaced
    bam_placeholder = "BAMFILE"
    bai_placeholder = "BAMINDEX"
    
    try:
        # Open the input file for reading
        with open(input_file_path, 'r') as input_file:
            # Open the output file for writing
            with open(output_file_path, 'w') as output_file:
                # Process each line in the input file
                for line_number, line in enumerate(input_file, 1):
                    # Strip whitespace and newline characters
                    file_ids = line.strip()
                    
                    # Skip empty lines
                    if not file_ids:
                        continue
                    
                    # Split the input to get BAM and BAI file IDs
                    if ":" in file_ids:
                        bam_file_id, bai_file_id = file_ids.split(":", 1)
                    else:
                        print(f"Warning: Line {line_number} does not contain expected format (bam_id:bai_id)")
                        continue
                    # Write the new command to the output file
                    # Replace the file IDs with the new ones
                    new_command = base_command.replace(bam_placeholder, bam_file_id)
                    new_command = new_command.replace(bai_placeholder, bai_file_id)
                    
                    # Write the new command to the output file
                    output_file.write(f"{new_command}\n")
                    
                    # Optional: print progress
                    print(f"Processed line {line_number}: BAM={bam_file_id}, BAI={bai_file_id}")
        
        print(f"Successfully created commands in {output_file_path}")
        
    except FileNotFoundError:
        print(f"Error: Could not find file {input_file_path}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python coverage.py input_file.txt results.txt")
        sys.exit(1)
    
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    
    replace_file_id(input_file_path, output_file_path)