#!/usr/bin/env python3

def replace_file_id(input_file_path, output_file_path):
    """
    Reads file IDs from input_file_path, replaces the R1 and R2 file IDs in the command,
    and writes the full commands to output_file_path.
    """
    # FastQC command with the file IDs to be replaced
    base_command = (
        "dx run applet-GKXqZV80jy1QxF4yKYB4Y3Kz "
        "-ireads=R1 "
        "-ireads=R2 -y"
    )
    
    # File IDs to be replaced
    r1_placeholder = "R1"
    r2_placeholder = "R2"
    
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
                    
                    # Split the input to get R1 and R2 file IDs
                    if ":" in file_ids:
                        r1_file_id, r2_file_id = file_ids.split(":", 1)
                    else:
                        print(f"Warning: Line {line_number} does not contain expected format (r1_id:r2_id)")
                        continue
                    
                    # Replace the file IDs with the new ones
                    new_command = base_command.replace(r1_placeholder, r1_file_id)
                    new_command = new_command.replace(r2_placeholder, r2_file_id)
                    
                    # Write the new command to the output file
                    output_file.write(f"{new_command}\n")
                    
                    # Optional: print progress
                    print(f"Processed line {line_number}: R1={r1_file_id}, R2={r2_file_id}")
        
        print(f"Successfully created FastQC commands in {output_file_path}")
        
    except FileNotFoundError:
        print(f"Error: Could not find file {input_file_path}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python fastqc.py input_file.txt results.txt")
        sys.exit(1)
    
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    
    replace_file_id(input_file_path, output_file_path)