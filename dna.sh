#!/bin/bash

# DNAnexus Genomics Run Command Generator
# This script generates run commands for genomic analysis workflows based on sample names
# and can automatically extract samples from a DNAnexus file

# Display usage information
function show_usage {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -s SAMPLE_NAME  : Single sample name (e.g., NGS650FFV06_56_325053_BM_M_CP2R184Via_Pan4121)"
    echo "  -f FILE         : File containing a list of sample names (one per line)"
    echo "  -d FILE_ID      : DNAnexus file ID to extract sample names from (e.g., file-J090Xx80zG4g7xbz2pY7y9ZZ)"
    echo "  -o OUTPUT_FILE  : Output file to write commands (default: commands.sh)"
    echo "  -p PROJECT_ID   : Override automatic project ID detection"
    echo "  -e FAILURES_CSV : Output file for failed samples (default: failures.csv)"
    echo "  -h              : Display this help message"
    echo ""
    echo "Note: Either -s, -f, or -d must be provided"
    echo "      The script will automatically extract R number and Pan code from each sample name"
    exit 1
}

# Set default values
OUTPUT_FILE="commands.sh"
FAILURES_CSV="failures.csv"  # New default for failures CSV

# Process command line parameters
while getopts "s:f:d:o:p:e:h" opt; do
    case $opt in
        s) SAMPLE_NAME="$OPTARG" ;;
        f) SAMPLE_FILE="$OPTARG" ;;
        d) DX_FILE_ID="$OPTARG" ;;
        o) OUTPUT_FILE="$OPTARG" ;;
        p) OVERRIDE_PROJECT_ID="$OPTARG" ;;
        e) FAILURES_CSV="$OPTARG" ;;  # New option for failures output file
        h) show_usage ;;
        \?) echo "Invalid option: -$OPTARG" >&2; show_usage ;;
        :) echo "Option -$OPTARG requires an argument." >&2; show_usage ;;
    esac
done

# Check if at least one sample source is provided
if [ -z "$SAMPLE_NAME" ] && [ -z "$SAMPLE_FILE" ] && [ -z "$DX_FILE_ID" ]; then
    echo "Error: Either a sample name (-s), a file with sample names (-f), or a DNAnexus file ID (-d) must be provided!"
    show_usage
fi

# Try to auto-detect project information
PROJECT_ID=""
PROJECT_NAME=""

if [ -n "$DX_FILE_ID" ]; then
    echo "Extracting project information from DNAnexus file $DX_FILE_ID..."
    
    # Get project ID and name from dx describe
    dx_describe=$(dx describe "$DX_FILE_ID")
    if [ $? -ne 0 ]; then
        echo "Error: Failed to execute 'dx describe $DX_FILE_ID'. Please check your DNAnexus credentials."
        exit 1
    fi
    
    # Extract project ID
    PROJECT_ID=$(echo "$dx_describe" | grep -oP "Project\s+\K(project-[a-zA-Z0-9]+)")
    if [ -n "$PROJECT_ID" ]; then
        echo "Detected Project ID: $PROJECT_ID"
    else
        echo "Warning: Could not detect Project ID from dx describe output"
    fi
    
    # Extract folder path to get project name
    FOLDER_PATH=$(echo "$dx_describe" | grep -oP "Folder\s+\K([^\n]+)")
    if [ -n "$FOLDER_PATH" ]; then
        # Remove the leading / and extract the project name
        PROJECT_NAME=$(echo "$FOLDER_PATH" | sed 's/^\///' | cut -d'/' -f1)
        echo "Detected Project Name: $PROJECT_NAME"
    else
        echo "Warning: Could not detect folder path from dx describe output"
    fi
fi

# If project ID was provided as a parameter, use it
if [ -n "$OVERRIDE_PROJECT_ID" ]; then
    PROJECT_ID="$OVERRIDE_PROJECT_ID"
    echo "Using provided Project ID: $PROJECT_ID"
fi

# If project ID is still empty, use placeholder
if [ -z "$PROJECT_ID" ]; then
    PROJECT_ID="{PROJECT_ID}"
    echo "Using placeholder Project ID: $PROJECT_ID"
fi

# Try to get auth token
AUTH_TOKEN=""
AUTH_TOKEN_PATH="/usr/local/src/mokaguys/.dnanexus_auth_token"

if [ -f "$AUTH_TOKEN_PATH" ]; then
    AUTH_TOKEN=$(cat "$AUTH_TOKEN_PATH")
    echo "Successfully read auth token from $AUTH_TOKEN_PATH"
else
    AUTH_TOKEN="{AUTH}"
    echo "Warning: Auth token file not found at $AUTH_TOKEN_PATH. Using placeholder."
fi

# Create or clear the output file
> "$OUTPUT_FILE"
# Create or clear the failures CSV with header
echo "sample_name,failure_reason" > "$FAILURES_CSV"

# Add header to output file
cat << EOF > "$OUTPUT_FILE"
#!/bin/bash
# Generated run commands
# Generated on: $(date)

AUTH=$AUTH_TOKEN
PROJECT_ID=$PROJECT_ID
PROJECT_NAME=$PROJECT_NAME
DEPENDS_LIST=''
DEPENDS_LIST_SENTIEON=''

EOF

# Make the output file executable
chmod +x "$OUTPUT_FILE"

# Create a temporary file for sample names
TEMP_SAMPLE_FILE=$(mktemp)

# If a DNAnexus file ID is provided, extract samples from it
if [ -n "$DX_FILE_ID" ]; then
    echo "Fetching samples from DNAnexus file: $DX_FILE_ID"
    
    # Extract samples from DNAnexus file
    dx cat "$DX_FILE_ID" | grep -P "^NGS\d+" | cut -d',' -f1 > "$TEMP_SAMPLE_FILE"
    
    # Check if we got any samples
    sample_count=$(wc -l < "$TEMP_SAMPLE_FILE")
    if [ "$sample_count" -eq 0 ]; then
        echo "Error: No samples found in the DNAnexus file. The file format may be incorrect."
        rm "$TEMP_SAMPLE_FILE"
        exit 1
    fi
    
    echo "Found $sample_count samples in the DNAnexus file."
    SAMPLE_FILE="$TEMP_SAMPLE_FILE"
fi

# Define a function to process a single sample
function process_sample {
    local sample_name="$1"
    echo "Processing sample: $sample_name"
    
    # Extract R number using grep
    R_NUMBER=$(echo "$sample_name" | grep -oP 'R\d+')
    
    # Check if this is a WES sample without R number
    if [ -z "$R_NUMBER" ] && [[ "$sample_name" =~ SingletonWES|WES ]]; then
        R_NUMBER="WES"
        echo "Detected WES sample without standard R number, using special WES configuration"
    elif [ -z "$R_NUMBER" ]; then
        echo "Warning: Could not extract R number from sample name: $sample_name"
        echo "Expected format: *R[number]* or *SingletonWES* or *WES*"
        echo "$sample_name,\"Missing R number\"" >> "$FAILURES_CSV"
        return 1
    fi
    
    # Extract Pan code using grep
    PAN_CODE=$(echo "$sample_name" | grep -oP 'Pan\d+')
    if [ -z "$PAN_CODE" ]; then
        echo "Warning: Could not extract Pan code from sample name: $sample_name"
        echo "Expected format: *Pan[number]*"
        echo "$sample_name,\"Missing Pan code\"" >> "$FAILURES_CSV"
        return 1
    fi
    
    # Extract batch and pool from sample name
    BATCH_POOL=$(echo $sample_name | grep -oP 'NGS\d+[A-Z]+\d+')
    if [ -n "$BATCH_POOL" ]; then
        BATCH=$(echo $BATCH_POOL)
    else
        echo "Warning: Could not detect batch information from sample name"
        # Fallback to a static path
        BATCH="NGS650FFV06POOL2"
    fi
    
    # Lookup bed files for the specified run ID
    bed_files=$(lookup_bed_files "$R_NUMBER")
    if [ $? -ne 0 ]; then
        echo "$bed_files" # This will print the error message
        echo "$sample_name,\"Run ID '$R_NUMBER' not found in lookup table\"" >> "$FAILURES_CSV"
        return 1
    fi
    
    IFS='|' read -r panel variant_bed coverage_bed ed_bed cnv_bed <<< "$bed_files"
    
    # Determine skip value for CNV analysis and PRS based on the run ID
    if [ -z "$cnv_bed" ]; then
        cnv_skip="true"
        echo "Notice: CNV bed file not found for $R_NUMBER. Setting CNV analysis to skip."
    else
        cnv_skip="false"
    fi
    
    # Rule: PRS (stage GK8G6k003JGx48f74jf16Kjv) should always be skipped except for R134 samples
    prs_skip="true"
    if [ "$R_NUMBER" == "R134" ]; then
        prs_skip="false"
        echo "PRS analysis enabled for R134 sample"
    fi
    
    # Rule: R210/R211 samples need polyedge
    polyedge_params=""
    if [ "$R_NUMBER" == "R210" ] || [ "$R_NUMBER" == "R211" ]; then
        polyedge_params="-istage-GK8G6kj03JGyVGvk2Q44KQG1.gene=MSH2 -istage-GK8G6kj03JGyVGvk2Q44KQG1.chrom=2 -istage-GK8G6kj03JGyVGvk2Q44KQG1.poly_start=47641559 -istage-GK8G6kj03JGyVGvk2Q44KQG1.poly_end=47641586 -istage-GK8G6kj03JGyVGvk2Q44KQG1.skip=false"
        echo "PolyEdge analysis enabled for $R_NUMBER sample"
    else
        echo "PolyEdge analysis disabled for $R_NUMBER sample"
    fi
    
    # Rule: NA12878 samples should enable the GK8G6p803JGx48f74jf16Kjx stage
    cnv_stage_skip="true"
    if [[ "$sample_name" == *"NA12878"* ]]; then
        cnv_stage_skip="false"
        echo "CNV stage enabled for NA12878 control sample"
    fi
    
    # Generate the run command and append it to the output file
    cat << EOF >> "$OUTPUT_FILE"
JOB_ID=\$(dx run project-ByfFPz00jy1fk6PjpZ95F27J:workflow-Gzj03g80jy1XbKzZY4yz7JXZ --priority high -y --name $sample_name -istage-Ff0P5Jj0GYKY717pKX3vX8Z3.reads=\${PROJECT_ID}:/$PROJECT_NAME/Samples/${sample_name}_R1.fastq.gz -istage-Ff0P5Jj0GYKY717pKX3vX8Z3.reads=\${PROJECT_ID}:/$PROJECT_NAME/Samples/${sample_name}_R2.fastq.gz -istage-Ff0P73j0GYKX41VkF3j62F9j.reads_fastqgzs=\${PROJECT_ID}:/$PROJECT_NAME/Samples/${sample_name}_R1.fastq.gz -istage-Ff0P73j0GYKX41VkF3j62F9j.reads2_fastqgzs=\${PROJECT_ID}:/$PROJECT_NAME/Samples/${sample_name}_R2.fastq.gz -istage-Ff0P73j0GYKX41VkF3j62F9j.output_metrics=true -istage-Ff0P73j0GYKX41VkF3j62F9j.germline_algo=Haplotyper -istage-Ff0P73j0GYKX41VkF3j62F9j.sample=$sample_name -istage-Ff0P73j0GYKX41VkF3j62F9j.output_gvcf=true -istage-Ff0P73j0GYKX41VkF3j62F9j.gvcftyper_algo_options='--genotype_model multinomial' -istage-G77VfJ803JGy589J21p7Jkqj.bedfile=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/$variant_bed -istage-Ff0P5pQ0GYKVBB0g1FG27BV8.Capture_panel=Hybridisation -istage-Ff0P5pQ0GYKVBB0g1FG27BV8.vendor_exome_bedfile=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/$variant_bed -istage-Ff0P82Q0GYKQ4j8b4gXzjqxX.coverage_level=30 -istage-Ff0P82Q0GYKQ4j8b4gXzjqxX.sambamba_bed=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/$coverage_bed -istage-GK8G6p803JGx48f74jf16Kjx.skip=$cnv_stage_skip -istage-GK8G6p803JGx48f74jf16Kjx.prefix=$sample_name -istage-GK8G6p803JGx48f74jf16Kjx.panel_bed=project-ByfFPz00jy1fk6PjpZ95F27J:/Data/BED/$variant_bed -istage-GK8G6k003JGx48f74jf16Kjv.skip=$prs_skip $polyedge_params --dest=\${PROJECT_ID} --brief --auth \${AUTH})
DEPENDS_LIST="\${DEPENDS_LIST} -d \${JOB_ID} "
DEPENDS_LIST_SENTIEON="\${DEPENDS_LIST_SENTIEON} -d \${JOB_ID} "
EOF
    
    # Print configuration summary to console
    echo "✓ Added run command for $sample_name"
    echo "  - Run ID: $R_NUMBER"
    echo "  - Panel: $panel"
    echo "  - Batch: $BATCH"
    echo "  - Variant Calling BED: $variant_bed"
    echo "  - Coverage BED: $coverage_bed"
    echo "  - CNV Analysis: $([ "$cnv_skip" == "true" ] && echo "SKIPPED" || echo "ENABLED")"
    echo ""
    
    return 0
}

# Define a function to look up bed files based on run ID
function lookup_bed_files {
    local run_id="$1"
    
    # Define bed files lookup table based on the data provided
    # Format: run_id|panel|variant_calling_bed|coverage_bed|ed_readcount_bed|cnv_calling_bed
    lookup_table=$(cat << 'EOT'
R25|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|||
R66|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5240_cnv.bed
R73|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4622_cnv.bed
R78|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed||
R79|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed||
R81|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5273_CNV.bed
R90|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5252_CNV.bed
R97|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5251_CNV.bed
R112|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4985_CNV.bed
R115|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4986_CNV.bed
R116|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4987_cnv.bed
R117|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4988_cnv.bed
R118|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4989_cnv.bed
R119|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4990_cnv.bed
R120|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4991_cnv.bed
R121|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4708_cnv.bed
R122|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4992_cnv.bed
R123|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4993_cnv.bed
R124|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4994_cnv.bed
R134|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan5215_cnv.bed
R163|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5268_cnv.bed
R164|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5267_cnv.bed
R165|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5266_CNV.bed
R166|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5265_cnv.bed
R167|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5264_cnv.bed
R184|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|Pan5208_exomedepth.bed|Pan4703_cnv.bed
R207|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5250_cnv.bed
R208|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5249_cnv.bed
R210|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5248_cnv.bed
R211|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5247_cnv.bed
R227|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5246_CNV.bed
R229|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5245_CNV.bed
R230|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed||
R236|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5262_CNV.bed
R237|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5261_CNV.bed
R255|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5260_CNV.bed
R259|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5244_CNV.bed
R326|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5259_CNV.bed
R332|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5258_CNV.bed
R337|VCP1|Pan4398data.bed|Pan4397dataSambamba.bed|||
R414|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5243_CNV.bed
R424|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5257_CNV.bed
R430|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5242_CNV.bed
R444.1|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5269_CNV.bed
R444.2|CP2|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5279_exomeDepth.bed|Pan5256_CNV.bed
WES|WES|Pan5272_data.bed|Pan5272_sambamba.bed|Pan5272_exomeDepth.bed|Pan5272_cnv.bed
EOT
)

    # Lookup bed files for the specified run ID
    bed_files_line=$(echo "$lookup_table" | grep -P "^$run_id\\|")
    
    if [ -z "$bed_files_line" ]; then
        echo "Error: Run ID '$run_id' not found in lookup table!" >&2
        return 1
    fi
    
    # Extract bed file information
    IFS='|' read -r _ panel variant_bed coverage_bed ed_bed cnv_bed <<< "$bed_files_line"
    
    # Return the bed files
    echo "$panel|$variant_bed|$coverage_bed|$ed_bed|$cnv_bed"
    return 0
}

# Process samples
if [ -n "$SAMPLE_NAME" ]; then
    # Process a single sample
    process_sample "$SAMPLE_NAME"
    if [ $? -eq 0 ]; then
        echo "Command for sample '$SAMPLE_NAME' has been written to '$OUTPUT_FILE'"
    else
        echo "Failed to process sample '$SAMPLE_NAME'"
    fi
fi

if [ -n "$SAMPLE_FILE" ]; then
    # Process samples from file
    if [ ! -f "$SAMPLE_FILE" ]; then
        echo "Error: Sample file '$SAMPLE_FILE' not found!"
        exit 1
    fi
    
    echo "Processing samples from file: $SAMPLE_FILE"
    total_samples=$(wc -l < "$SAMPLE_FILE")
    processed=0
    failed=0
    
    while IFS= read -r sample || [ -n "$sample" ]; do
        # Skip empty lines and comments
        if [ -z "$sample" ] || [[ "$sample" == \#* ]]; then
            continue
        fi
        
        echo "[$((processed+1))/$total_samples] Processing sample: $sample"
        process_sample "$sample"
        if [ $? -eq 0 ]; then
            processed=$((processed+1))
        else
            failed=$((failed+1))
        fi
        echo "--------------------------------------------"
    done < "$SAMPLE_FILE"
    
    echo ""
    echo "Summary:"
    echo "- Total samples processed: $processed"
    echo "- Failed samples: $failed"
    echo "- Output file: $OUTPUT_FILE"
    echo "- Failures file: $FAILURES_CSV"
fi

# Add footer to the output file with MultiQC job
cat << 'EOF' >> "$OUTPUT_FILE"

# Run MultiQC after all sample jobs complete
echo "Running MultiQC for all processed samples..."
JOB_ID=$(dx run project-ByfFPz00jy1fk6PjpZ95F27J:applet-GXqBzg00jy1pXkQVkY027QqV --priority high -y --name MultiQC -iproject_for_multiqc=${PROJECT_NAME} -icoverage_level=30 ${DEPENDS_LIST} --dest=${PROJECT_ID} --brief --auth ${AUTH})
echo "MultiQC job submitted: $JOB_ID"
echo "All jobs have been submitted successfully."
EOF

# Clean up temporary file if it exists
if [ -n "$DX_FILE_ID" ] && [ -f "$TEMP_SAMPLE_FILE" ]; then
    rm "$TEMP_SAMPLE_FILE"
fi

echo ""
echo "The generated script '$OUTPUT_FILE' is now ready to use."
echo "You can run it with: bash $OUTPUT_FILE"
if [ -f "$FAILURES_CSV" ] && [ $(wc -l < "$FAILURES_CSV") -gt 1 ]; then
    echo "Failed samples have been recorded in: $FAILURES_CSV"
fi