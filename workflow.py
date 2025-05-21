#!/usr/bin/env python3

import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Any
from base import CommandGenerator


class CP2WorkflowGenerator(CommandGenerator):
    """Generates commands for CP2 workflow"""

    def __init__(self):
        super().__init__()

    @property
    def name(self) -> str:
        return "CP2 Workflow"

    @property
    def description(self) -> str:
        return "Generate DNAnexus genomics workflow commands for CP2 analysis from RunManifest.csv"

    def generate(self) -> None:
        args = self._parse_arguments()
        if args: # Proceed only if arguments were successfully parsed
            self._process_workflow(args)

    def _parse_arguments(self) -> Optional[Any]:
        """Parse command line arguments specific to CP2 workflow using interactive prompts"""
        print("\nCP2 Workflow Configuration:")
        print("---------------------------")
        print("Please provide the DNAnexus file-id for RunManifest.csv")

        try:
            dxfile = input("Enter DNAnexus file ID (e.g., file-xxxx): ").strip()
            
            if not dxfile:
                print("Error: No file ID provided.")
                return None
            
            if not dxfile.startswith("file-"):
                print("Error: Invalid DNAnexus file ID format. Must start with 'file-'")
                return None

            # Create a simple object to hold arguments
            class ArgsNamespace:
                def __init__(self, **kwargs):
                    self.__dict__.update(kwargs)
            
            args_dict = {
                "dxfile": dxfile,
                "sample": None,
                "file": None,
                "output": None,
                "project": None,  # Will be auto-detected
                "failures": "failures.csv"  # Default failures file
            }

            # Detect project info immediately
            project_id, project_name = self._detect_project_info(dxfile)
            
            if not project_id:
                print("Error: Could not detect project ID from the provided file.")
                return None
                
            if not project_name:
                print("Error: Could not detect project name from the provided file.")
                return None

            print(f"\nDetected Project Information:")
            print(f"  Project ID: {project_id}")
            print(f"  Project Name: {project_name}")

            # Set the project ID in args
            args_dict["project"] = project_id
            
            # Set output filename based on project name
            args_dict["output"] = f"{project_name.replace(' ', '_')}_workflow_cmds.sh"
            
            return ArgsNamespace(**args_dict)

        except EOFError:
            print("\nInput cancelled during configuration. Exiting workflow generation.")
            return None
        except KeyboardInterrupt:
            print("\nConfiguration interrupted. Exiting workflow generation.")
            return None

    def _detect_project_info(self, dx_file_id: str) -> Tuple[str, str]:
        """Detect project information from DNAnexus file ID"""
        project_id = ""
        project_name = ""

        print(f"Extracting project information from DNAnexus file {dx_file_id}...")

        try:
            dx_describe_cmd = ["dx", "describe", dx_file_id]
            print(f"Executing: {' '.join(dx_describe_cmd)}")
            dx_describe_output = subprocess.check_output(dx_describe_cmd, text=True, stderr=subprocess.PIPE)

            project_id_match = re.search(r"Project\s+(project-[a-zA-Z0-9]+)", dx_describe_output)
            if project_id_match:
                project_id = project_id_match.group(1)
                print(f"Detected Project ID: {project_id}")
            else:
                print("Warning: Could not detect Project ID from dx describe output.")

            folder_path_match = re.search(r"Folder\s+([^\n]+)", dx_describe_output)
            if folder_path_match:
                folder_path = folder_path_match.group(1).strip()
                # Remove the leading / and extract the project name (first part of the path)
                project_name_candidate = folder_path.lstrip('/').split('/')[0]
                if project_name_candidate:
                    project_name = project_name_candidate
                    print(f"Detected Project Name (from folder path): {project_name}")
                else:
                    print("Warning: Folder path was '/' or empty, could not derive project name from folder.")
            else:
                print("Warning: Could not detect folder path from dx describe output.")

        except subprocess.CalledProcessError as e:
            print(f"Error: Failed to execute 'dx describe {dx_file_id}'. Return code: {e.returncode}")
            print(f"Command output: {e.output}")
            print(f"Command error: {e.stderr}")
            print("Please check your DNAnexus login status and if the file ID is correct.")
        except FileNotFoundError:
            print("Error: 'dx' command not found. Please ensure the DNAnexus toolkit is installed and in your PATH.")
        
        return project_id, project_name

    def _get_auth_token(self) -> str:
        """Get authentication token from file or use placeholder"""
        auth_token = ""
        if os.path.isfile(self.config_values["dnanexus_auth_token_path"]):
            try:
                with open(self.config_values["dnanexus_auth_token_path"], 'r') as f:
                    auth_token = f.read().strip()
                if auth_token:
                    print(f"Successfully read auth token from {self.config_values["dnanexus_auth_token_path"]}")
                else:
                    print(f"Warning: Auth token file {self.config_values["dnanexus_auth_token_path"]} is empty. Using placeholder.")
                    auth_token = "{AUTH_TOKEN_PLACEHOLDER}"
            except Exception as e:
                print(f"Error reading auth token from {self.config_values["dnanexus_auth_token_path"]}: {e}. Using placeholder.")
                auth_token = "{AUTH_TOKEN_PLACEHOLDER}"
        else:
            auth_token = "{AUTH_TOKEN_PLACEHOLDER}"
            print(f"Warning: Auth token file not found at {self.config_values["dnanexus_auth_token_path"]}. Using placeholder.")

        return auth_token

    def _extract_samples_from_dx_file(self, dx_file_id: str) -> Optional[str]:
        """Extract sample names from a DNAnexus file, returns path to temporary file"""
        print(f"Fetching samples from DNAnexus file: {dx_file_id}")
        temp_file_path = ""

        try:
            # Create a temporary file to store sample names
            with tempfile.NamedTemporaryFile(delete=False, mode='w+t', suffix=".txt") as temp_f:
                temp_file_path = temp_f.name

            dx_cat_cmd = ["dx", "cat", dx_file_id]
            print(f"Executing: {' '.join(dx_cat_cmd)}")
            dx_cat_output = subprocess.check_output(dx_cat_cmd, text=True, stderr=subprocess.PIPE)

            samples = []
            for line in dx_cat_output.splitlines():
                line = line.strip()
                # Regex to find lines starting with NGS<digits> (potential sample lines)
                # and extract the first part before a comma (if any) as the sample name.
                match = re.match(r"^(NGS\d+[A-Za-z0-9_.-]*)(?:,.*)?", line)
                if match:
                    sample_name = match.group(1)
                    samples.append(sample_name)

            if not samples:
                print(f"Error: No samples found in the DNAnexus file '{dx_file_id}'. The file might be empty or not in the expected format (e.g., one sample identifier per line, or CSV with sample in first column, starting with NGS).")
                os.unlink(temp_file_path) # Clean up empty temp file
                return None

            with open(temp_file_path, 'w') as f_out:
                for sample_name in samples:
                    f_out.write(f"{sample_name}\n")

            print(f"Found {len(samples)} samples in the DNAnexus file. Stored in temporary file: {temp_file_path}")
            return temp_file_path

        except subprocess.CalledProcessError as e:
            print(f"Error: Failed to execute 'dx cat {dx_file_id}'. Return code: {e.returncode}")
            print(f"Command output: {e.output}")
            print(f"Command error: {e.stderr}")
            print("Please check your DNAnexus login status and if the file ID is correct.")
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            return None
        except FileNotFoundError:
            print("Error: 'dx' command not found. Please ensure the DNAnexus toolkit is installed and in your PATH.")
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            return None
        except Exception as e:
            print(f"An unexpected error occurred while extracting samples: {e}")
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            return None

    def _process_sample(self, sample_name: str, output_file: str, failures_csv: str,
                          project_id_val: str, project_name_val: str) -> bool:
        """Process a single sample and generate run command"""
        print(f"\nProcessing sample: {sample_name}")

        r_number_match = re.search(r'R\d+(?:\.\d+)?', sample_name)
        r_number = r_number_match.group(0) if r_number_match else None

        if not r_number and re.search(r'SingletonWES|WES', sample_name, re.IGNORECASE):
            r_number = "WES"
            print("  Info: Detected WES sample without standard R number, using special WES configuration.")
        elif not r_number:
            msg = f"Could not extract R number from sample name: {sample_name}. Expected format: *R[number]* or *SingletonWES* or *WES*."
            print(f"  Warning: {msg}")
            with open(failures_csv, 'a') as f:
                f.write(f'"{sample_name}","{msg}"\n')
            return False

        pan_code_match = re.search(r'Pan\d+', sample_name, re.IGNORECASE)
        pan_code = pan_code_match.group(0) if pan_code_match else None

        if not pan_code:
            msg = f"Could not extract Pan code from sample name: {sample_name}. Expected format: *Pan[number]*."
            print(f"  Warning: {msg}")
            with open(failures_csv, 'a') as f:
                f.write(f'"{sample_name}","{msg}"\n')
            return False

        batch_pool_match = re.search(r'(NGS\d+[A-Za-z0-9]*?)(?:_|$)', sample_name)  # Match NGS and following chars until underscore or end
        batch = batch_pool_match.group(1) if batch_pool_match else None  # No fallback value

        if not batch:
            msg = f"Could not detect batch information (starting with NGS) from sample name '{sample_name}'."
            print(f"  Warning: {msg}")
            with open(failures_csv, 'a') as f:
                f.write(f'"{sample_name}","{msg}"\n')
            return False

        # Bed files (as per original script, these are hardcoded for this workflow)
        variant_bed = "Pan5272_data.bed"
        coverage_bed = "Pan5272_sambamba.bed"

        prs_skip = "true"
        if r_number == "R134": # Case-sensitive comparison for R numbers
            prs_skip = "false"
            print("  Info: PRS analysis will be enabled for R134 sample.")
        else:
            print("  Info: PRS analysis will be skipped.")

        polyedge_params = ""
        if r_number in ["R210", "R211"]:
            polyedge_params = "-istage-GK8G6kj03JGyVGvk2Q44KQG1.gene=MSH2 -istage-GK8G6kj03JGyVGvk2Q44KQG1.chrom=2 -istage-GK8G6kj03JGyVGvk2Q44KQG1.poly_start=47641559 -istage-GK8G6kj03JGyVGvk2Q44KQG1.poly_end=47641586 -istage-GK8G6kj03JGyVGvk2Q44KQG1.skip=false"
            print(f"  Info: PolyEdge analysis parameters enabled for {r_number} sample.")
        else:
            print(f"  Info: PolyEdge analysis parameters will be skipped for {r_number} sample.")

        cnv_stage_skip = "true"
        if "NA12878" in sample_name: # Case-sensitive check for NA12878
            cnv_stage_skip = "false"
            print("  Info: vcf_eval will be enabled for NA12878 control sample.")
        else:
            print("  Info: vcf_eval will be skipped.")

        # Generate single-line command
        run_command = f"dx run project-ByfFPz00jy1fk6PjpZ95F27J:workflow-Gzj03g80jy1XbKzZY4yz7JXZ --priority high -y --name \"{sample_name}\" -istage-Ff0P5Jj0GYKY717pKX3vX8Z3.reads=\"${{PROJECT_ID}}:/${{PROJECT_NAME}}/Samples/{sample_name}_R1.fastq.gz\" -istage-Ff0P5Jj0GYKY717pKX3vX8Z3.reads=\"${{PROJECT_ID}}:/${{PROJECT_NAME}}/Samples/{sample_name}_R2.fastq.gz\" -istage-Ff0P73j0GYKX41VkF3j62F9j.reads_fastqgzs=\"${{PROJECT_ID}}:/${{PROJECT_NAME}}/Samples/{sample_name}_R1.fastq.gz\" -istage-Ff0P73j0GYKX41VkF3j62F9j.reads2_fastqgzs=\"${{PROJECT_ID}}:/${{PROJECT_NAME}}/Samples/{sample_name}_R2.fastq.gz\" -istage-Ff0P73j0GYKX41VkF3j62F9j.output_metrics=true -istage-Ff0P73j0GYKX41VkF3j62F9j.germline_algo=Haplotyper -istage-Ff0P73j0GYKX41VkF3j62F9j.sample=\"{sample_name}\" -istage-Ff0P73j0GYKX41VkF3j62F9j.output_gvcf=true -istage-Ff0P73j0GYKX41VkF3j62F9j.gvcftyper_algo_options='--genotype_model multinomial' -istage-G77VfJ803JGy589J21p7Jkqj.bedfile=\"project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{variant_bed}\" -istage-Ff0P5pQ0GYKVBB0g1FG27BV8.Capture_panel=Hybridisation -istage-Ff0P5pQ0GYKVBB0g1FG27BV8.vendor_exome_bedfile=\"project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{variant_bed}\" -istage-Ff0P82Q0GYKQ4j8b4gXzjqxX.coverage_level=30 -istage-Ff0P82Q0GYKQ4j8b4gXzjqxX.sambamba_bed=\"project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{coverage_bed}\" -istage-GK8G6p803JGx48f74jf16Kjx.skip={cnv_stage_skip} -istage-GK8G6p803JGx48f74jf16Kjx.prefix=\"{sample_name}\" -istage-GK8G6p803JGx48f74jf16Kjx.panel_bed=\"project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{variant_bed}\" -istage-GK8G6k003JGx48f74jf16Kjv.skip={prs_skip} {polyedge_params} --dest=\"${{PROJECT_ID}}\" --brief --auth \"${{AUTH_TOKEN}}\" -y\n"

        try:
            with open(output_file, 'a') as f:
                f.write(run_command)
        except IOError as e:
            print(f"  Error: Could not write to output file {output_file}: {e}")
            return False

        print(f"  ✓ Added run command for {sample_name} to {output_file}")
        print(f"    - R-Number: {r_number}")
        print(f"    - Pan Code: {pan_code}")
        print(f"    - Batch Info: {batch}")
        print(f"    - Variant Calling BED: {variant_bed}")
        print(f"    - Coverage BED: {coverage_bed}")
        print(f"    - PRS Skip: {prs_skip}")
        print(f"    - vcf_eval Skip: {cnv_stage_skip}")
        if polyedge_params:
             print(f"    - PolyEdge Params: Enabled")
        return True

    def _process_workflow(self, args: Any) -> None:
        """Process the CP2 workflow with the given arguments"""
        project_id_to_use = ""
        project_name_to_use = "UNKNOWN_PROJECT" # Default if not found
        sample_file_path = None
        temp_file_created_path = None # Store path of temp file if created

        # 1. Determine Project ID and Name
        if args.dxfile:
            detected_pid, detected_pname = self._detect_project_info(args.dxfile)
            if detected_pid: project_id_to_use = detected_pid
            if detected_pname: project_name_to_use = detected_pname

        if args.project: # User-provided project ID overrides detected one
            project_id_to_use = args.project
            print(f"Using user-provided Project ID: {project_id_to_use}")
            # If project name wasn't found via dxfile, try to get it from this project ID
            if project_name_to_use == "UNKNOWN_PROJECT" and project_id_to_use:
                try:
                    dx_desc_proj_cmd = ["dx", "describe", project_id_to_use, "--json"]
                    print(f"Executing: {' '.join(dx_desc_proj_cmd)}")
                    project_desc_json = subprocess.check_output(dx_desc_proj_cmd, text=True, stderr=subprocess.PIPE)
                    import json
                    project_desc = json.loads(project_desc_json)
                    project_name_to_use = project_desc.get("name", "UNKNOWN_PROJECT")
                    print(f"Detected Project Name from ID '{project_id_to_use}': {project_name_to_use}")
                except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as e:
                    print(f"Warning: Could not automatically determine project name from project ID '{project_id_to_use}'. Error: {e}")

        if not project_id_to_use:
            project_id_to_use = "{PROJECT_ID_PLACEHOLDER}"
            print(f"Using placeholder for Project ID: {project_id_to_use}")
        if project_name_to_use == "UNKNOWN_PROJECT" and project_id_to_use == "{PROJECT_ID_PLACEHOLDER}":
             print(f"Warning: Project Name is unknown and Project ID is a placeholder. FastQ paths in script might be incorrect.")

        # 2. Determine Output File Name
        output_filename = args.output
        if not output_filename:
            if project_name_to_use != "UNKNOWN_PROJECT" and project_name_to_use:
                output_filename = f"{project_name_to_use.replace(' ', '_')}_workflow_cmds.sh"
            else:
                output_filename = "dnanexus_workflow_cmds.sh"
            print(f"Using default output script filename: {output_filename}")
        else:
            print(f"Using specified output script filename: {output_filename}")

        # 3. Get Auth Token
        auth_token = self._get_auth_token()

        # 4. Initialize Output Files
        try:
            with open(output_filename, 'w') as f:
                f.write(f"AUTH_TOKEN=\"{auth_token}\"\nPROJECT_ID=\"{project_id_to_use}\"\nPROJECT_NAME=\"{project_name_to_use}\"\n\n")
            os.chmod(output_filename, 0o755) # Make executable
            print(f"Initialized output script: {output_filename}")
        except IOError as e:
            print(f"Fatal Error: Could not write to or set permissions on output file {output_filename}: {e}")
            return

        failures_csv_file = args.failures if args.failures else "failures.csv"
        try:
            with open(failures_csv_file, 'w') as f:
                f.write("sample_name,failure_reason\n")
            print(f"Initialized failures log: {failures_csv_file}")
        except IOError as e:
            print(f"Warning: Could not initialize failures CSV {failures_csv_file}: {e}")

        # 5. Process Samples
        processed_count = 0
        failed_count = 0
        samples_to_process = []

        if args.dxfile:
            print(f"Attempting to extract samples from DNAnexus file: {args.dxfile}")
            temp_file_created_path = self._extract_samples_from_dx_file(args.dxfile)
            if temp_file_created_path:
                sample_file_path = temp_file_created_path
            else:
                print(f"Error: Failed to extract samples from DNAnexus file {args.dxfile}. Cannot proceed.")
                return
        elif args.file:
            sample_file_path = args.file
            print(f"Using local sample file: {sample_file_path}")
        elif args.sample:
            samples_to_process = [args.sample]
            print(f"Processing single sample: {args.sample}")

        if sample_file_path:
            if not os.path.isfile(sample_file_path):
                print(f"Error: Sample file '{sample_file_path}' not found!")
                if temp_file_created_path and os.path.exists(temp_file_created_path):
                    os.unlink(temp_file_created_path)
                return
            try:
                with open(sample_file_path, 'r') as f_samples:
                    samples_to_process = [line.strip() for line in f_samples if line.strip() and not line.startswith('#')]
                print(f"Read {len(samples_to_process)} samples from file: {sample_file_path}")
            except IOError as e:
                print(f"Error: Could not read sample file '{sample_file_path}': {e}")
                if temp_file_created_path and os.path.exists(temp_file_created_path):
                    os.unlink(temp_file_created_path)
                return

        if not samples_to_process:
            print("No samples to process.")
        else:
            total_samples = len(samples_to_process)
            for i, sample_name_raw in enumerate(samples_to_process, 1):
                sample_name = sample_name_raw.strip().split(',')[0] # Take first part if CSV-like
                print(f"\n--- [{i}/{total_samples}] Processing: {sample_name} ---")
                if self._process_sample(sample_name, output_filename, failures_csv_file, project_id_to_use, project_name_to_use):
                    processed_count += 1
                else:
                    failed_count += 1
                print("--------------------------------------------")

        # 7. Clean up temporary file if created
        if temp_file_created_path and os.path.exists(temp_file_created_path):
            try:
                os.unlink(temp_file_created_path)
                print(f"Cleaned up temporary sample file: {temp_file_created_path}")
            except OSError as e:
                print(f"Warning: Could not delete temporary file {temp_file_created_path}: {e}")

        # 8. Print Summary
        print("\n========= Workflow Generation Summary =========")
        print(f"  Output script: {os.path.abspath(output_filename)}")
        print(f"  Total samples for which commands were generated: {processed_count}")
        print(f"  Samples that failed pre-submission checks: {failed_count}")
        if failed_count > 0 or (os.path.exists(failures_csv_file) and os.path.getsize(failures_csv_file) > len("sample_name,failure_reason\n") +1):
            print(f"  Failures log: {os.path.abspath(failures_csv_file)}")
        else:
            if os.path.exists(failures_csv_file):
                 try: os.unlink(failures_csv_file)
                 except: pass

        print(f"\nTo execute the generated DNAnexus commands, run:\n  bash {os.path.abspath(output_filename)}")
        print("==============================================")