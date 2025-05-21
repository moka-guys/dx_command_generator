#!/usr/bin/env python3

import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime
from typing import List, Tuple, Optional, Any
from modules.dx_command_generator import DXCommandGenerator

class CP2WorkflowGenerator(DXCommandGenerator):
    """Generates commands for CP2 workflow"""

    def __init__(self):
        super().__init__()
        self.cp2_workflow_id = self.config_values.get('workflow')
        self.common_data_project = self.config_values.get('common_data_project')

    @property
    def name(self) -> str:
        return "CP2 Workflow"

    @property
    def description(self) -> str:
        return "Generate DNAnexus CP2 workflow commands using the RunManifest.csv file"

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
            dxfile_id = input("Enter DNAnexus file ID (e.g., file-xxxx): ").strip()
            
            if not dxfile_id:
                print("Error: No file ID provided.")
                return None
            
            if not dxfile_id.startswith("file-"):
                print("Error: Invalid DNAnexus file ID format. Must start with 'file-'")
                return None

            # Create a simple object to hold arguments
            class ArgsNamespace:
                def __init__(self, **kwargs):
                    self.__dict__.update(kwargs)
            
            project_id, project_name = self._detect_project_info(dxfile_id)
            
            if not project_id or not project_name:
                print("Error: Could not detect project information from the provided file.")
                return None

            print(f"\nDetected Project Information:")
            print(f"  Project ID: {project_id}")
            print(f"  Project Name: {project_name}")

            args_dict = {
                "dxfile": dxfile_id,
                "sample": None,
                "file": None,
                "project": project_id,
                "project_name": project_name,
                "output": f"{project_name.replace(' ', '_')}_workflow_cmds.sh",
                "failures": "failures.csv"
            }
            
            return ArgsNamespace(**args_dict)

        except EOFError:
            print("\nInput cancelled during configuration. Exiting workflow generation.")
            return None
        except KeyboardInterrupt:
            print("\nConfiguration interrupted. Exiting workflow generation.")
            return None

    def _extract_samples_from_dx_file(self, dx_file_id: str) -> Optional[str]:
        """Extract sample names from a DNAnexus file, returns path to temporary file"""
        print(f"Fetching samples from DNAnexus file: {dx_file_id}")
        temp_file_path = ""

        try:
            with tempfile.NamedTemporaryFile(delete=False, mode='w+t', suffix=".txt") as temp_f:
                temp_file_path = temp_f.name

            dx_cat_cmd = ["dx", "cat", dx_file_id]
            print(f"Executing: {' '.join(dx_cat_cmd)}")
            dx_cat_output = subprocess.check_output(dx_cat_cmd, text=True, stderr=subprocess.PIPE)

            samples = []
            for line in dx_cat_output.splitlines():
                line = line.strip()
                match = re.match(r"^(NGS\d+[A-Za-z0-9_.-]*)(?:,.*)?", line)
                if match:
                    sample_name = match.group(1)
                    samples.append(sample_name)

            if not samples:
                print(f"Error: No samples found in the DNAnexus file '{dx_file_id}'. The file might be empty or not in the expected format (e.g., one sample identifier per line, or CSV with sample in first column, starting with NGS).")
                os.unlink(temp_file_path)
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

        batch_pool_match = re.search(r'(NGS\d+[A-Za-z0-9]*?)(?:_|$)', sample_name)
        batch = batch_pool_match.group(1) if batch_pool_match else None

        if not batch:
            msg = f"Could not detect batch information (starting with NGS) from sample name '{sample_name}'."
            print(f"  Warning: {msg}")
            with open(failures_csv, 'a') as f:
                f.write(f'"{sample_name}","{msg}"\n')
            return False

        # Bed files (as per original script, these are hardcoded for this workflow, but use common_data_project from config)
        variant_bed = f"{self.common_data_project}:/Data/BED/Pan5272_data.bed"
        coverage_bed = f"{self.common_data_project}:/Data/BED/Pan5272_sambamba.bed"

        prs_skip = "true"
        if r_number == "R134":
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
        if "NA12878" in sample_name:
            cnv_stage_skip = "false"
            print("  Info: vcf_eval will be enabled for NA12878 control sample.")
        else:
            print("  Info: vcf_eval will be skipped.")

        # Generate single-line command
        # Use self.cp2_workflow_id from config
        run_command = (
            f"dx run {self.cp2_workflow_id} --priority high -y --name \"{sample_name}\" "
            f"-istage-Ff0P5Jj0GYKY717pKX3vX8Z3.reads=\"${{PROJECT_ID}}:/${{PROJECT_NAME}}/Samples/{sample_name}_R1.fastq.gz\" "
            f"-istage-Ff0P5Jj0GYKY717pKX3vX8Z3.reads=\"${{PROJECT_ID}}:/${{PROJECT_NAME}}/Samples/{sample_name}_R2.fastq.gz\" "
            f"-istage-Ff0P73j0GYKX41VkF3j62F9j.reads_fastqgzs=\"${{PROJECT_ID}}:/${{PROJECT_NAME}}/Samples/{sample_name}_R1.fastq.gz\" "
            f"-istage-Ff0P73j0GYKX41VkF3j62F9j.reads2_fastqgzs=\"${{PROJECT_ID}}:/${{PROJECT_NAME}}/Samples/{sample_name}_R2.fastq.gz\" "
            "-istage-Ff0P73j0GYKX41VkF3j62F9j.output_metrics=true "
            "-istage-Ff0P73j0GYKX41VkF3j62F9j.germline_algo=Haplotyper "
            f"-istage-Ff0P73j0GYKX41VkF3j62F9j.sample=\"{sample_name}\" "
            "-istage-Ff0P73j0GYKX41VkF3j62F9j.output_gvcf=true "
            "-istage-Ff0P73j0GYKX41VkF3j62F9j.gvcftyper_algo_options='--genotype_model multinomial' "
            f"-istage-G77VfJ803JGy589J21p7Jkqj.bedfile=\"{variant_bed}\" "
            "-istage-Ff0P5pQ0GYKVBB0g1FG27BV8.Capture_panel=Hybridisation "
            f"-istage-Ff0P5pQ0GYKVBB0g1FG27BV8.vendor_exome_bedfile=\"{variant_bed}\" "
            "-istage-Ff0P82Q0GYKQ4j8b4gXzjqxX.coverage_level=30 "
            f"-istage-Ff0P82Q0GYKQ4j8b4gXzjqxX.sambamba_bed=\"{coverage_bed}\" "
            f"-istage-GK8G6p803JGx48f74jf16Kjx.skip={cnv_stage_skip} "
            f"-istage-GK8G6p803JGx48f74jf16Kjx.prefix=\"{sample_name}\" "
            f"-istage-GK8G6p803JGx48f74jf16Kjx.panel_bed=\"{variant_bed}\" "
            f"-istage-GK8G6k003JGx48f74jf16Kjv.skip={prs_skip} {polyedge_params} "
            f"--dest=\"${{PROJECT_ID}}\" --brief --auth \"${{AUTH_TOKEN}}\" -y\n"
        )

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
        if not args.project:
            print("Error: No project ID available. Cannot proceed without a valid DNAnexus project ID.")
            return
            
        project_id_to_use = args.project
        project_name_to_use = args.project_name or "UNKNOWN_PROJECT"
        sample_file_path = None
        temp_file_created_path = None

        print(f"Using Project ID: {project_id_to_use}")
        print(f"Using Project Name: {project_name_to_use}")

        output_filename = args.output
        print(f"Using output script filename: {output_filename}")

        if not self._initialize_output_file(output_filename, project_id_to_use, project_name_to_use, "CP2 Workflow Commands"):
            return

        failures_csv_file = args.failures if args.failures else "failures.csv"
        try:
            with open(failures_csv_file, 'w') as f:
                f.write("sample_name,failure_reason\n")
            print(f"Initialized failures log: {failures_csv_file}")
        except IOError as e:
            print(f"Warning: Could not initialize failures CSV {failures_csv_file}: {e}")

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
                sample_name = sample_name_raw.strip().split(',')[0]
                print(f"\n--- [{i}/{total_samples}] Processing: {sample_name} ---")
                if self._process_sample(sample_name, output_filename, failures_csv_file, project_id_to_use, project_name_to_use):
                    processed_count += 1
                else:
                    failed_count += 1
                print("--------------------------------------------")

        if temp_file_created_path and os.path.exists(temp_file_created_path):
            try:
                os.unlink(temp_file_created_path)
            except OSError as e:
                print(f"Warning: Could not delete temporary file {temp_file_created_path}: {e}")

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