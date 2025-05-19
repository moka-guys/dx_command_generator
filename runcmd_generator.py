#!/usr/bin/env python3

import argparse
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple


def show_usage():
    """Display detailed usage information"""
    print("DNAnexus Genomics Run Command Generator")
    print("This script generates run commands for genomic analysis workflows based on sample names")
    print("and can automatically extract samples from a DNAnexus file")
    print("\nUsage: python dnanexus_command_generator.py [options]")
    print("\nOptions:")
    print("  -s, --sample SAMPLE_NAME   : Single sample name")
    print("  -f, --file FILE            : File containing a list of sample names (one per line)")
    print("  -d, --dxfile FILE_ID       : DNAnexus file ID to extract sample names from")
    print("  -o, --output OUTPUT_FILE   : Output file to write commands (default: {project_name}_workflow_cmds.sh)")
    print("  -p, --project PROJECT_ID   : Override automatic project ID detection")
    print("  -e, --failures FAILURES_CSV: Output file for failed samples (default: failures.csv)")
    print("  -h, --help                 : Display this help message")
    print("\nNote: Either -s, -f, or -d must be provided")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="DNAnexus Genomics Run Command Generator", add_help=False)
    parser.add_argument("-s", "--sample", help="Single sample name")
    parser.add_argument("-f", "--file", help="File containing list of sample names")
    parser.add_argument("-d", "--dxfile", help="DNAnexus file ID to extract sample names from")
    parser.add_argument("-o", "--output", help="Output file (default: {project_name}_workflow_cmds.sh)")
    parser.add_argument("-p", "--project", help="Override automatic project ID detection")
    parser.add_argument("-e", "--failures", default="failures.csv", help="Failed samples output (default: failures.csv)")
    parser.add_argument("-h", "--help", action="store_true", help="Display help message")
    
    args = parser.parse_args()
    
    # Show help if requested or if no input sources provided
    if args.help or (not args.sample and not args.file and not args.dxfile):
        show_usage()
        sys.exit(1)
        
    return args


def detect_project_info(dx_file_id):
    """Detect project information from DNAnexus file ID"""
    project_id = ""
    project_name = ""
    
    print(f"Extracting project information from DNAnexus file {dx_file_id}...")
    
    try:
        # Get project ID and name from dx describe
        dx_describe = subprocess.check_output(["dx", "describe", dx_file_id], text=True)
        
        # Extract project ID
        project_id_match = re.search(r"Project\s+(project-[a-zA-Z0-9]+)", dx_describe)
        if project_id_match:
            project_id = project_id_match.group(1)
            print(f"Detected Project ID: {project_id}")
        else:
            print("Warning: Could not detect Project ID from dx describe output")
        
        # Extract folder path to get project name
        folder_path_match = re.search(r"Folder\s+([^\n]+)", dx_describe)
        if folder_path_match:
            folder_path = folder_path_match.group(1)
            # Remove the leading / and extract the project name
            project_name = folder_path.lstrip('/').split('/')[0]
            print(f"Detected Project Name: {project_name}")
        else:
            print("Warning: Could not detect folder path from dx describe output")
            
    except subprocess.CalledProcessError:
        print(f"Error: Failed to execute 'dx describe {dx_file_id}'. Please check your DNAnexus credentials.")
        sys.exit(1)
        
    return project_id, project_name


def get_auth_token():
    """Get authentication token from file or use placeholder"""
    auth_token = ""
    auth_token_path = "/usr/local/src/mokaguys/.dnanexus_auth_token"
    
    if os.path.isfile(auth_token_path):
        with open(auth_token_path, 'r') as f:
            auth_token = f.read().strip()
        print(f"Successfully read auth token from {auth_token_path}")
    else:
        auth_token = "{AUTH}"
        print(f"Warning: Auth token file not found at {auth_token_path}. Using placeholder.")
        
    return auth_token


def extract_samples_from_dx_file(dx_file_id):
    """Extract sample names from a DNAnexus file"""
    print(f"Fetching samples from DNAnexus file: {dx_file_id}")
    
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w+t')
    
    try:
        # Download the file content
        dx_cat = subprocess.check_output(["dx", "cat", dx_file_id], text=True)
        
        # Extract sample names
        samples = []
        for line in dx_cat.splitlines():
            if re.match(r"^NGS\d+", line):
                sample_name = line.split(',')[0]
                samples.append(sample_name)
                temp_file.write(f"{sample_name}\n")
        
        temp_file.close()
        
        if not samples:
            print("Error: No samples found in the DNAnexus file. The file format may be incorrect.")
            os.unlink(temp_file.name)
            sys.exit(1)
            
        print(f"Found {len(samples)} samples in the DNAnexus file.")
        return temp_file.name
        
    except subprocess.CalledProcessError:
        print(f"Error: Failed to execute 'dx cat {dx_file_id}'. Please check your DNAnexus credentials.")
        os.unlink(temp_file.name)
        sys.exit(1)


def lookup_bed_files(run_id, sample_name):
    """Look up bed files based on run ID and sample name"""
    # Check if this is a CP2 sample (contains CP2 but not VCP2)
    if "CP2" in sample_name and "VCP2" not in sample_name:
        return "CP2", "Pan5272_data.bed", "Pan5272_sambamba.bed"
    
    # Search for the run ID in the lookup table for VCP1 and WES
    for line in lookup_table.strip().split('\n'):
        if line.startswith(f"{run_id}|"):
            fields = line.split('|')
            if len(fields) >= 4:
                panel = fields[1]
                variant_bed = fields[2]
                coverage_bed = fields[3]
                return panel, variant_bed, coverage_bed
    
    # Run ID not found
    print(f"Error: Run ID '{run_id}' not found in lookup table!")
    return None


def process_sample(sample_name, output_file, failures_csv, project_id, project_name):
    """Process a single sample and generate run command"""
    print(f"Processing sample: {sample_name}")
    
    # Extract R number using regex
    r_number_match = re.search(r'R\d+(?:\.\d+)?', sample_name)
    r_number = r_number_match.group(0) if r_number_match else None
    
    # Check if this is a WES sample without R number
    if not r_number and re.search(r'SingletonWES|WES', sample_name):
        r_number = "WES"
        print("Detected WES sample without standard R number, using special WES configuration")
    elif not r_number:
        print(f"Warning: Could not extract R number from sample name: {sample_name}")
        print("Expected format: *R[number]* or *SingletonWES* or *WES*")
        with open(failures_csv, 'a') as f:
            f.write(f'{sample_name},"Missing R number"\n')
        return False
    
    # Extract Pan code using regex
    pan_code_match = re.search(r'Pan\d+', sample_name)
    pan_code = pan_code_match.group(0) if pan_code_match else None
    
    if not pan_code:
        print(f"Warning: Could not extract Pan code from sample name: {sample_name}")
        print("Expected format: *Pan[number]*")
        with open(failures_csv, 'a') as f:
            f.write(f'{sample_name},"Missing Pan code"\n')
        return False
    
    # Extract batch and pool from sample name
    batch_pool_match = re.search(r'NGS\d+[A-Z]+\d+', sample_name)
    batch = batch_pool_match.group(0) if batch_pool_match else "NGS650FFV06POOL2"
    
    if not batch_pool_match:
        print("Warning: Could not detect batch information from sample name")
        print("Using fallback: NGS650FFV06POOL2")
    
    # Use standard bed files for all samples
    variant_bed = "Pan5272_data.bed"
    coverage_bed = "Pan5272_sambamba.bed"
    
    # Rule: PRS should always be skipped except for R134 samples
    prs_skip = "false" if r_number == "R134" else "true"
    if r_number == "R134":
        print("PRS analysis enabled for R134 sample")
    
    # Rule: R210/R211 samples need polyedge
    polyedge_params = ""
    if r_number in ["R210", "R211"]:
        polyedge_params = "-istage-GK8G6kj03JGyVGvk2Q44KQG1.gene=MSH2 -istage-GK8G6kj03JGyVGvk2Q44KQG1.chrom=2 -istage-GK8G6kj03JGyVGvk2Q44KQG1.poly_start=47641559 -istage-GK8G6kj03JGyVGvk2Q44KQG1.poly_end=47641586 -istage-GK8G6kj03JGyVGvk2Q44KQG1.skip=false"
        print(f"PolyEdge analysis enabled for {r_number} sample")
    else:
        print(f"PolyEdge analysis disabled for {r_number} sample")
    
    # Rule: NA12878 samples should enable the GK8G6p803JGx48f74jf16Kjx stage
    cnv_stage_skip = "false" if "NA12878" in sample_name else "true"
    if "NA12878" in sample_name:
        print("vcf_eval enabled for NA12878 control sample")
    
    # Generate the run command and append it to the output file
    run_command = f"""JOB_ID=$(dx run project-ByfFPz00jy1fk6PjpZ95F27J:workflow-Gzj03g80jy1XbKzZY4yz7JXZ --priority high -y --name {sample_name} -istage-Ff0P5Jj0GYKY717pKX3vX8Z3.reads=${{PROJECT_ID}}:/{project_name}/Samples/{sample_name}_R1.fastq.gz -istage-Ff0P5Jj0GYKY717pKX3vX8Z3.reads=${{PROJECT_ID}}:/{project_name}/Samples/{sample_name}_R2.fastq.gz -istage-Ff0P73j0GYKX41VkF3j62F9j.reads_fastqgzs=${{PROJECT_ID}}:/{project_name}/Samples/{sample_name}_R1.fastq.gz -istage-Ff0P73j0GYKX41VkF3j62F9j.reads2_fastqgzs=${{PROJECT_ID}}:/{project_name}/Samples/{sample_name}_R2.fastq.gz -istage-Ff0P73j0GYKX41VkF3j62F9j.output_metrics=true -istage-Ff0P73j0GYKX41VkF3j62F9j.germline_algo=Haplotyper -istage-Ff0P73j0GYKX41VkF3j62F9j.sample={sample_name} -istage-Ff0P73j0GYKX41VkF3j62F9j.output_gvcf=true -istage-Ff0P73j0GYKX41VkF3j62F9j.gvcftyper_algo_options='--genotype_model multinomial' -istage-G77VfJ803JGy589J21p7Jkqj.bedfile=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{variant_bed} -istage-Ff0P5pQ0GYKVBB0g1FG27BV8.Capture_panel=Hybridisation -istage-Ff0P5pQ0GYKVBB0g1FG27BV8.vendor_exome_bedfile=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{variant_bed} -istage-Ff0P82Q0GYKQ4j8b4gXzjqxX.coverage_level=30 -istage-Ff0P82Q0GYKQ4j8b4gXzjqxX.sambamba_bed=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{coverage_bed} -istage-GK8G6p803JGx48f74jf16Kjx.skip={cnv_stage_skip} -istage-GK8G6p803JGx48f74jf16Kjx.prefix={sample_name} -istage-GK8G6p803JGx48f74jf16Kjx.panel_bed=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{variant_bed} -istage-GK8G6k003JGx48f74jf16Kjv.skip={prs_skip} {polyedge_params} --dest=${{PROJECT_ID}} --brief --auth ${{AUTH}})
DEPENDS_LIST="${{DEPENDS_LIST}} -d ${{JOB_ID}} "
DEPENDS_LIST_SENTIEON="${{DEPENDS_LIST_SENTIEON}} -d ${{JOB_ID}} "
"""
    
    with open(output_file, 'a') as f:
        f.write(run_command + "\n")
    
    # Print configuration summary to console
    print(f"✓ Added run command for {sample_name}")
    print(f"  - Run ID: {r_number}")
    print(f"  - Batch: {batch}")
    print(f"  - Variant Calling BED: {variant_bed}")
    print(f"  - Coverage BED: {coverage_bed}")
    print("")
    
    return True


class CommandGenerator(ABC):
    """Abstract base class for command generators"""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the command generator"""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """Return a description of what this command generator does"""
        pass
    
    @abstractmethod
    def generate(self) -> None:
        """Generate the commands"""
        pass


class CP2WorkflowGenerator(CommandGenerator):
    """Generates commands for CP2 workflow"""
    
    def __init__(self):
        self.auth_token_path = "/usr/local/src/mokaguys/.dnanexus_auth_token"
    
    @property
    def name(self) -> str:
        return "CP2 Workflow"
    
    @property
    def description(self) -> str:
        return "Generate DNAnexus genomics workflow commands for CP2 analysis"
    
    def generate(self) -> None:
        args = self._parse_arguments()
        self._process_workflow(args)
    
    def _parse_arguments(self):
        """Parse command line arguments specific to CP2 workflow"""
        print("\nCP2 Workflow Configuration:")
        print("---------------------------")
        
        sample_input = input("Enter sample name, file path, or DNAnexus file ID (or press Enter for interactive mode): ").strip()
        
        args = type('Args', (), {})()  # Create a simple object to hold arguments
        
        if not sample_input:
            # Interactive mode
            input_type = self._get_menu_choice([
                "Single sample name",
                "File containing sample names",
                "DNAnexus file ID"
            ], "Select input type")
            
            if input_type == 1:
                args.sample = input("Enter sample name: ").strip()
                args.file = None
                args.dxfile = None
            elif input_type == 2:
                args.file = input("Enter file path: ").strip()
                args.sample = None
                args.dxfile = None
            else:
                args.dxfile = input("Enter DNAnexus file ID: ").strip()
                args.sample = None
                args.file = None
        else:
            # Try to determine input type automatically
            if os.path.isfile(sample_input):
                args.file = sample_input
                args.sample = None
                args.dxfile = None
            elif sample_input.startswith("file-"):
                args.dxfile = sample_input
                args.sample = None
                args.file = None
            else:
                args.sample = sample_input
                args.file = None
                args.dxfile = None
        
        # Get output file name (but we'll set the default later after project detection)
        args.output = input("Enter output file name (or press Enter for default): ").strip()
        
        # Get project ID (optional)
        args.project = input("Enter project ID (or press Enter to auto-detect): ").strip()
        
        # Set default failures file
        args.failures = "failures.csv"
        
        return args
    
    def _get_menu_choice(self, options: List[str], prompt: str) -> int:
        """Display a menu and get user choice"""
        print(f"\n{prompt}:")
        for i, option in enumerate(options, 1):
            print(f"{i}. {option}")
        
        while True:
            try:
                choice = int(input("\nEnter your choice (number): "))
                if 1 <= choice <= len(options):
                    return choice
                print("Invalid choice. Please try again.")
            except ValueError:
                print("Please enter a number.")
    
    def _detect_project_info(self, dx_file_id: str) -> Tuple[str, str]:
        """Detect project information from DNAnexus file ID"""
        project_id = ""
        project_name = ""
        
        print(f"Extracting project information from DNAnexus file {dx_file_id}...")
        
        try:
            dx_describe = subprocess.check_output(["dx", "describe", dx_file_id], text=True)
            
            project_id_match = re.search(r"Project\s+(project-[a-zA-Z0-9]+)", dx_describe)
            if project_id_match:
                project_id = project_id_match.group(1)
                print(f"Detected Project ID: {project_id}")
            
            folder_path_match = re.search(r"Folder\s+([^\n]+)", dx_describe)
            if folder_path_match:
                folder_path = folder_path_match.group(1)
                project_name = folder_path.lstrip('/').split('/')[0]
                print(f"Detected Project Name: {project_name}")
                
        except subprocess.CalledProcessError:
            print(f"Error: Failed to execute 'dx describe {dx_file_id}'")
            
        return project_id, project_name
    
    def _get_auth_token(self) -> str:
        """Get authentication token from file or use placeholder"""
        if os.path.isfile(self.auth_token_path):
            with open(self.auth_token_path, 'r') as f:
                auth_token = f.read().strip()
            print(f"Successfully read auth token from {self.auth_token_path}")
        else:
            auth_token = "{AUTH}"
            print(f"Warning: Auth token file not found at {self.auth_token_path}. Using placeholder.")
        
        return auth_token
    
    def _extract_samples_from_dx_file(self, dx_file_id: str) -> str:
        """Extract sample names from a DNAnexus file"""
        print(f"Fetching samples from DNAnexus file: {dx_file_id}")
        
        temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w+t')
        
        try:
            dx_cat = subprocess.check_output(["dx", "cat", dx_file_id], text=True)
            
            samples = []
            for line in dx_cat.splitlines():
                if re.match(r"^NGS\d+", line):
                    sample_name = line.split(',')[0]
                    samples.append(sample_name)
                    temp_file.write(f"{sample_name}\n")
            
            temp_file.close()
            
            if not samples:
                print("Error: No samples found in the DNAnexus file")
                os.unlink(temp_file.name)
                sys.exit(1)
                
            print(f"Found {len(samples)} samples in the DNAnexus file.")
            return temp_file.name
            
        except subprocess.CalledProcessError:
            print(f"Error: Failed to execute 'dx cat {dx_file_id}'")
            os.unlink(temp_file.name)
            sys.exit(1)
    
    def _process_sample(self, sample_name: str, output_file: str, failures_csv: str, 
                       project_id: str, project_name: str) -> bool:
        """Process a single sample and generate run command"""
        print(f"Processing sample: {sample_name}")
        
        # Extract R number
        r_number_match = re.search(r'R\d+(?:\.\d+)?', sample_name)
        r_number = r_number_match.group(0) if r_number_match else None
        
        # Check if WES sample
        if not r_number and re.search(r'SingletonWES|WES', sample_name):
            r_number = "WES"
            print("Detected WES sample without standard R number")
        elif not r_number:
            print(f"Warning: Could not extract R number from sample name: {sample_name}")
            with open(failures_csv, 'a') as f:
                f.write(f'{sample_name},"Missing R number"\n')
            return False
        
        # Extract Pan code
        pan_code_match = re.search(r'Pan\d+', sample_name)
        pan_code = pan_code_match.group(0) if pan_code_match else None
        
        if not pan_code:
            print(f"Warning: Could not extract Pan code from sample name: {sample_name}")
            with open(failures_csv, 'a') as f:
                f.write(f'{sample_name},"Missing Pan code"\n')
            return False
        
        # Extract batch and pool
        batch_pool_match = re.search(r'NGS\d+[A-Z]+\d+', sample_name)
        batch = batch_pool_match.group(0) if batch_pool_match else "NGS650FFV06POOL2"
        
        # Use standard bed files
        variant_bed = "Pan5272_data.bed"
        coverage_bed = "Pan5272_sambamba.bed"
        
        # Configure PRS
        prs_skip = "false" if r_number == "R134" else "true"
        
        # Configure polyedge
        polyedge_params = ""
        if r_number in ["R210", "R211"]:
            polyedge_params = "-istage-GK8G6kj03JGyVGvk2Q44KQG1.gene=MSH2 -istage-GK8G6kj03JGyVGvk2Q44KQG1.chrom=2 -istage-GK8G6kj03JGyVGvk2Q44KQG1.poly_start=47641559 -istage-GK8G6kj03JGyVGvk2Q44KQG1.poly_end=47641586 -istage-GK8G6kj03JGyVGvk2Q44KQG1.skip=false"
        
        # Configure CNV stage
        cnv_stage_skip = "false" if "NA12878" in sample_name else "true"
        
        # Generate run command
        run_command = f"""JOB_ID=$(dx run project-ByfFPz00jy1fk6PjpZ95F27J:workflow-Gzj03g80jy1XbKzZY4yz7JXZ --priority high -y --name {sample_name} -istage-Ff0P5Jj0GYKY717pKX3vX8Z3.reads=${{PROJECT_ID}}:/{project_name}/Samples/{sample_name}_R1.fastq.gz -istage-Ff0P5Jj0GYKY717pKX3vX8Z3.reads=${{PROJECT_ID}}:/{project_name}/Samples/{sample_name}_R2.fastq.gz -istage-Ff0P73j0GYKX41VkF3j62F9j.reads_fastqgzs=${{PROJECT_ID}}:/{project_name}/Samples/{sample_name}_R1.fastq.gz -istage-Ff0P73j0GYKX41VkF3j62F9j.reads2_fastqgzs=${{PROJECT_ID}}:/{project_name}/Samples/{sample_name}_R2.fastq.gz -istage-Ff0P73j0GYKX41VkF3j62F9j.output_metrics=true -istage-Ff0P73j0GYKX41VkF3j62F9j.germline_algo=Haplotyper -istage-Ff0P73j0GYKX41VkF3j62F9j.sample={sample_name} -istage-Ff0P73j0GYKX41VkF3j62F9j.output_gvcf=true -istage-Ff0P73j0GYKX41VkF3j62F9j.gvcftyper_algo_options='--genotype_model multinomial' -istage-G77VfJ803JGy589J21p7Jkqj.bedfile=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{variant_bed} -istage-Ff0P5pQ0GYKVBB0g1FG27BV8.Capture_panel=Hybridisation -istage-Ff0P5pQ0GYKVBB0g1FG27BV8.vendor_exome_bedfile=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{variant_bed} -istage-Ff0P82Q0GYKQ4j8b4gXzjqxX.coverage_level=30 -istage-Ff0P82Q0GYKQ4j8b4gXzjqxX.sambamba_bed=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{coverage_bed} -istage-GK8G6p803JGx48f74jf16Kjx.skip={cnv_stage_skip} -istage-GK8G6p803JGx48f74jf16Kjx.prefix={sample_name} -istage-GK8G6p803JGx48f74jf16Kjx.panel_bed=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/{variant_bed} -istage-GK8G6k003JGx48f74jf16Kjv.skip={prs_skip} {polyedge_params} --dest=${{PROJECT_ID}} --brief --auth ${{AUTH}})
DEPENDS_LIST="${{DEPENDS_LIST}} -d ${{JOB_ID}} "
DEPENDS_LIST_SENTIEON="${{DEPENDS_LIST_SENTIEON}} -d ${{JOB_ID}} "
"""
        
        with open(output_file, 'a') as f:
            f.write(run_command + "\n")
        
        print(f"✓ Added run command for {sample_name}")
        print(f"  - Run ID: {r_number}")
        print(f"  - Batch: {batch}")
        print(f"  - Variant Calling BED: {variant_bed}")
        print(f"  - Coverage BED: {coverage_bed}")
        print("")
        
        return True
    
    def _process_workflow(self, args):
        """Process the CP2 workflow with the given arguments"""
        project_id = ""
        project_name = ""
        sample_file = None
        temp_file_created = False
        
        # Detect project information
        if args.dxfile:
            project_id, project_name = self._detect_project_info(args.dxfile)
        
        # Use provided project ID if specified
        if args.project:
            project_id = args.project
            print(f"Using provided Project ID: {project_id}")
        
        # Use placeholder if project ID is still empty
        if not project_id:
            project_id = "{PROJECT_ID}"
            print(f"Using placeholder Project ID: {project_id}")
            
        # If project name is still empty, try to get it from project ID
        if not project_name and project_id and project_id != "{PROJECT_ID}":
            try:
                dx_describe = subprocess.check_output(["dx", "describe", project_id], text=True)
                name_match = re.search(r"Name\s+([^\n]+)", dx_describe)
                if name_match:
                    project_name = name_match.group(1)
                    print(f"Detected Project Name from ID: {project_name}")
            except subprocess.CalledProcessError:
                print("Warning: Could not detect project name from project ID")
        
        # Set default output filename if not specified
        if not args.output:
            if project_name:
                args.output = f"{project_name}_workflow_cmds.sh"
            else:
                args.output = "workflow_cmds.sh"
                print("Warning: Could not determine project name, using generic output filename")
            print(f"Using default output filename: {args.output}")
        
        # Get auth token
        auth_token = self._get_auth_token()
        
        # Initialize output files
        with open(args.output, 'w') as f:
            f.write(f"""#!/bin/bash
# Generated run commands
# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

AUTH={auth_token}
PROJECT_ID={project_id}
PROJECT_NAME={project_name}
DEPENDS_LIST=''

""")
        
        # Make output file executable
        os.chmod(args.output, 0o755)
        
        # Initialize failures CSV
        with open(args.failures, 'w') as f:
            f.write("sample_name,failure_reason\n")
        
        # Process samples
        processed = 0
        failed = 0
        
        # Extract samples from DNAnexus file if provided
        if args.dxfile:
            sample_file = self._extract_samples_from_dx_file(args.dxfile)
            temp_file_created = True
        elif args.file:
            sample_file = args.file
        
        # Process single sample if provided
        if args.sample:
            if self._process_sample(args.sample, args.output, args.failures, project_id, project_name):
                processed += 1
            else:
                failed += 1
        
        # Process samples from file
        if sample_file:
            if not os.path.isfile(sample_file):
                print(f"Error: Sample file '{sample_file}' not found!")
                sys.exit(1)
            
            with open(sample_file, 'r') as f:
                samples = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            total_samples = len(samples)
            print(f"Processing {total_samples} samples from file: {sample_file}")
            
            for i, sample in enumerate(samples, 1):
                print(f"[{i}/{total_samples}] Processing sample: {sample}")
                if self._process_sample(sample, args.output, args.failures, project_id, project_name):
                    processed += 1
                else:
                    failed += 1
                print("--------------------------------------------")
        
        # Add MultiQC footer
        with open(args.output, 'a') as f:
            f.write("""
# Run MultiQC after all sample jobs complete
echo "Running MultiQC for all processed samples..."
JOB_ID=$(dx run project-ByfFPz00jy1fk6PjpZ95F27J:applet-GXqBzg00jy1pXkQVkY027QqV --priority high -y --name MultiQC -iproject_for_multiqc=${PROJECT_NAME} -icoverage_level=30 ${DEPENDS_LIST} --dest=${PROJECT_ID} --brief --auth ${AUTH})
echo "MultiQC job submitted: $JOB_ID"
echo "All jobs have been submitted successfully."
""")
        
        # Clean up temporary file if created
        if temp_file_created and os.path.exists(sample_file):
            os.unlink(sample_file)
        
        # Print summary
        print("\nSummary:")
        print(f"- Total samples processed: {processed}")
        print(f"- Failed samples: {failed}")
        print(f"- Output file: {args.output}")
        print(f"- Failures file: {args.failures}")
        
        print(f"\nThe generated script '{args.output}' is now ready to use.")
        print(f"You can run it with: bash {args.output}")
        
        if os.path.getsize(args.failures) > len("sample_name,failure_reason\n"):
            print(f"Failed samples have been recorded in: {args.failures}")


def main():
    """Main function to run the command generator"""
    generators = [
        CP2WorkflowGenerator(),
        # Add more generators here as they're implemented
    ]
    
    print("DNAnexus Run Command Generator")
    print("=============================")
    print("Available command types:")
    
    for i, generator in enumerate(generators, 1):
        print(f"\n{i}. {generator.name}")
        print(f"   {generator.description}")
    
    while True:
        try:
            choice = int(input("\nSelect command type (number) or 0 to exit: "))
            if choice == 0:
                print("Exiting...")
                sys.exit(0)
            if 1 <= choice <= len(generators):
                generators[choice - 1].generate()
                break
            print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a number.")


if __name__ == "__main__":
    main()