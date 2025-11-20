#!/bin/bash

# VLLM Medical QA Evaluation Runner
# This script runs the medical QA evaluation with comprehensive analysis and cross-language adjustment capabilities

set -e  # Exit on error

# Default parameters
MODEL_NAME=""
DATASET_PATH=""
CROSS_DATASET_PATH="" # Language cross comparison for token analysis
OUTPUT_DIR=""
DATASET_LANGUAGE="english"
SUBSET_SIZE=10000
MAX_MODEL_LEN=2048
TEMPERATURE=0.0  # Use the temperature recommended by the model card (0.0 is deterministic)
MAX_TOKENS=250
GPU_MEMORY_UTIL=0.9
SEED=42
TENSOR_PARALLEL_SIZE=1
ENABLE_TOPIC_BATCH_ANALYSIS=false
ENABLE_DIFFICULTY_BATCH_ANALYSIS=false

# Dataset paths - Update these with your actual dataset locations
MEDQA_DATASET_PATH="/path/to/MedQA.jsonl"  # MedQA English dataset
VM14K_DATASET_PATH="/path/to/VM14K.jsonl"  # VM14K Vietnamese dataset

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to print usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run VLLM Medical QA Evaluation with comprehensive analysis, visualizations, and cross-language performance normalization

REQUIRED OPTIONS:
    -m, --model MODEL_NAME           Model name (e.g., Qwen/Qwen3-4B-Instruct-2507)
    -d, --dataset DATASET_PATH       Path to JSONL dataset file or preset name
                                     Presets: medqa, vm14k

CROSS-LANGUAGE ANALYSIS OPTIONS:
    -c, --cross-dataset PATH         Path to cross-language dataset for adjustment factor calculation
                                     Use this to enable fair performance comparison between languages
                                     Example: When evaluating Vietnamese, provide English dataset path
    --auto-cross                     Automatically use the other preset dataset for cross-language analysis
                                     If -d is medqa, uses vm14k as cross-dataset (and vice versa)

OPTIONAL PARAMETERS:
    -o, --output OUTPUT_DIR          Output directory (auto-generated if not specified)
    -l, --language LANGUAGE          Dataset language: english|vietnamese (default: english)
                                     IMPORTANT: Set this correctly for proper adjustment calculations
    -s, --subset SUBSET_SIZE         Number of questions to evaluate (default: 10000, 0 = all)
    --max-len MAX_MODEL_LEN          Maximum model context length (default: 2048)
    --temperature TEMPERATURE        Sampling temperature (default: 0.0)
                                     0.0 is deterministic and recommended for QA tasks
    --max-tokens MAX_TOKENS          Maximum tokens per response (default: 250)
    --gpu-memory GPU_MEMORY_UTIL     GPU memory utilization (default: 0.9)
    --seed SEED                      Random seed for reproducibility (default: 42)
    --tensor-parallel SIZE           Number of GPUs for tensor parallelism (default: 1)

ANALYSIS OPTIONS:
    --enable-topic-batch             Enable topic-based batch token analysis
                                     (adds significant inference time)
    --enable-difficulty-batch        Enable difficulty-based batch token analysis
                                     (adds significant inference time)
    --enable-all-batch               Enable both topic and difficulty batch analysis

OTHER OPTIONS:
    -h, --help                       Show this help message

PRESET DATASET NAMES:
    medqa                            Uses: $MEDQA_DATASET_PATH (MedQA English)
    vm14k                            Uses: $VM14K_DATASET_PATH (VM14K Vietnamese)

OUTPUT FILES GENERATED:
    • model_output.txt                      - Detailed question-by-question results
    • infer_result.txt                      - Summary statistics and performance metrics
    • topics_ranked_by_accuracy.txt         - Topics ranked by accuracy
    • language_adjustment_factors.txt       - Cross-language adjustment factors (when cross-dataset provided)
    • 10+ visualization PNG files           - Performance charts and graphs

CROSS-LANGUAGE ANALYSIS WORKFLOW:
    For proper cross-language comparison, we recommend this workflow:
    
    Step 1: Run English (MedQA) evaluation first
    $0 -m MODEL -d medqa -l english --enable-all-batch
    
    Step 2: Run Vietnamese (VM14K) evaluation with cross-dataset
    $0 -m MODEL -d vm14k -l vietnamese -c medqa --enable-all-batch
    
    This generates language adjustment factors and normalizes Vietnamese performance
    to English-equivalent metrics for fair comparison.

EXAMPLES:

    # Basic evaluation with MedQA dataset (no cross-language analysis)
    $0 -m Qwen/Qwen3-4B-Instruct-2507 -d medqa -l english

    # Vietnamese evaluation WITH cross-language adjustment (RECOMMENDED)
    $0 -m Qwen/Qwen3-4B-Instruct-2507 -d vm14k -l vietnamese -c medqa --enable-all-batch

    # Auto cross-dataset mode (automatically selects the other preset)
    $0 -m Qwen/Qwen3-4B-Instruct-2507 -d vm14k -l vietnamese --auto-cross

    # Full evaluation with custom dataset and cross-dataset paths
    $0 -m Qwen/Qwen3-4B-Instruct-2507 \\
       -d /path/to/vietnamese.jsonl \\
       -c /path/to/english.jsonl \\
       -l vietnamese \\
       --enable-all-batch

    # English evaluation with cross-dataset (generates factors for reference)
    $0 -m Qwen/Qwen3-4B-Instruct-2507 -d medqa -l english -c vm14k

    # Multi-GPU evaluation with cross-language analysis
    $0 -m meta-llama/Llama-2-70b-chat-hf \\
       -d vm14k -l vietnamese -c medqa \\
       --tensor-parallel 4 --gpu-memory 0.95

    # Evaluate subset with cross-language analysis
    $0 -m Qwen/Qwen3-4B-Instruct-2507 -d vm14k -l vietnamese -c medqa -s 1000

NOTES:
    • Cross-language analysis requires both English and Vietnamese datasets
    • English is always used as the baseline (adjustment factor = 1.0)
    • Vietnamese metrics are adjusted to English-equivalent for fair comparison
    • Language adjustment factors are topic-specific and difficulty-specific
    • Both raw and adjusted metrics are preserved in output files
EOF
    exit 1
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to print colored messages
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to print section header
print_header() {
    local message=$1
    print_message "$GREEN" "\n$message"
    print_message "$GREEN" "$(printf '=%.0s' {1..80})"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--model)
            MODEL_NAME="$2"
            shift 2
            ;;
        -d|--dataset)
            INPUT_DATASET="$2"
            # Handle preset dataset names
            if [ "$INPUT_DATASET" = "medqa" ]; then
                DATASET_PATH="$MEDQA_DATASET_PATH"
                PRESET_DATASET="medqa"
            elif [ "$INPUT_DATASET" = "vm14k" ]; then
                DATASET_PATH="$VM14K_DATASET_PATH"
                PRESET_DATASET="vm14k"
            else
                DATASET_PATH="$INPUT_DATASET"
                PRESET_DATASET=""
            fi
            shift 2
            ;;
        -c|--cross-dataset)
            INPUT_CROSS_DATASET="$2"
            # Handle preset dataset names
            if [ "$INPUT_CROSS_DATASET" = "medqa" ]; then
                CROSS_DATASET_PATH="$MEDQA_DATASET_PATH"
            elif [ "$INPUT_CROSS_DATASET" = "vm14k" ]; then
                CROSS_DATASET_PATH="$VM14K_DATASET_PATH"
            else
                CROSS_DATASET_PATH="$INPUT_CROSS_DATASET"
            fi
            shift 2
            ;;
        --auto-cross)
            AUTO_CROSS=true
            shift
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -l|--language)
            DATASET_LANGUAGE="$2"
            shift 2
            ;;
        -s|--subset)
            SUBSET_SIZE="$2"
            shift 2
            ;;
        --max-len)
            MAX_MODEL_LEN="$2"
            shift 2
            ;;
        --temperature)
            TEMPERATURE="$2"
            shift 2
            ;;
        --max-tokens)
            MAX_TOKENS="$2"
            shift 2
            ;;
        --gpu-memory)
            GPU_MEMORY_UTIL="$2"
            shift 2
            ;;
        --seed)
            SEED="$2"
            shift 2
            ;;
        --tensor-parallel)
            TENSOR_PARALLEL_SIZE="$2"
            shift 2
            ;;
        --enable-topic-batch)
            ENABLE_TOPIC_BATCH_ANALYSIS=true
            shift
            ;;
        --enable-difficulty-batch)
            ENABLE_DIFFICULTY_BATCH_ANALYSIS=true
            shift
            ;;
        --enable-all-batch)
            ENABLE_TOPIC_BATCH_ANALYSIS=true
            ENABLE_DIFFICULTY_BATCH_ANALYSIS=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            print_message "$RED" "Unknown option: $1"
            usage
            ;;
    esac
done

# Handle auto-cross mode
if [ "$AUTO_CROSS" = true ]; then
    if [ "$PRESET_DATASET" = "medqa" ]; then
        CROSS_DATASET_PATH="$VM14K_DATASET_PATH"
        print_message "$CYAN" "Auto-cross mode: Using VM14K as cross-dataset"
    elif [ "$PRESET_DATASET" = "vm14k" ]; then
        CROSS_DATASET_PATH="$MEDQA_DATASET_PATH"
        print_message "$CYAN" "Auto-cross mode: Using MedQA as cross-dataset"
    else
        print_message "$YELLOW" "Warning: --auto-cross only works with preset datasets (medqa, vm14k)"
        CROSS_DATASET_PATH=""
    fi
fi

# Validate required parameters
if [ -z "$MODEL_NAME" ]; then
    print_message "$RED" "Error: Model name is required!"
    echo ""
    usage
fi

if [ -z "$DATASET_PATH" ]; then
    print_message "$RED" "Error: Dataset path is required!"
    echo ""
    usage
fi

if [ ! -f "$DATASET_PATH" ]; then
    print_message "$RED" "Error: Dataset file not found: $DATASET_PATH"
    exit 1
fi

# Validate cross-dataset if provided
if [ -n "$CROSS_DATASET_PATH" ] && [ ! -f "$CROSS_DATASET_PATH" ]; then
    print_message "$RED" "Error: Cross-dataset file not found: $CROSS_DATASET_PATH"
    exit 1
fi

# Check for required commands
print_header "Checking Dependencies"

if ! command_exists python3; then
    print_message "$RED" "Error: python3 is not installed!"
    exit 1
fi
print_message "$GREEN" "✓ Python3 found"

if ! command_exists nvidia-smi; then
    print_message "$YELLOW" "⚠ nvidia-smi not found. GPU may not be available."
else
    print_message "$GREEN" "✓ nvidia-smi found"
fi

# Check Python packages
print_message "$BLUE" "\nVerifying Python packages..."
MISSING_PACKAGES=()

python3 -c "import vllm" 2>/dev/null || MISSING_PACKAGES+=("vllm")
python3 -c "import torch" 2>/dev/null || MISSING_PACKAGES+=("torch")
python3 -c "import transformers" 2>/dev/null || MISSING_PACKAGES+=("transformers")
python3 -c "import pandas" 2>/dev/null || MISSING_PACKAGES+=("pandas")
python3 -c "import matplotlib" 2>/dev/null || MISSING_PACKAGES+=("matplotlib")
python3 -c "import numpy" 2>/dev/null || MISSING_PACKAGES+=("numpy")

if [ ${#MISSING_PACKAGES[@]} -ne 0 ]; then
    print_message "$RED" "Error: Missing required Python packages: ${MISSING_PACKAGES[*]}"
    print_message "$YELLOW" "Please install: pip install vllm torch transformers pandas matplotlib numpy tqdm"
    exit 1
fi
print_message "$GREEN" "✓ All required packages found"

# Display GPU information
if command_exists nvidia-smi; then
    print_header "GPU Information"
    nvidia-smi --query-gpu=index,name,memory.total,memory.free,utilization.gpu --format=csv,noheader | \
        awk -F', ' '{printf "GPU %s: %s | Total: %s | Free: %s | Util: %s\n", $1, $2, $3, $4, $5}'
fi

# Check if evaluation script exists
if [ ! -f "vllm_qa_evaluation.py" ]; then
    print_message "$RED" "Error: vllm_qa_evaluation.py not found in current directory!"
    exit 1
fi

# Create output directory if specified
if [ -n "$OUTPUT_DIR" ]; then
    mkdir -p "$OUTPUT_DIR"
fi

# Print configuration
print_header "Evaluation Configuration"
echo ""
echo "Model:                      $MODEL_NAME"
echo "Dataset:                    $DATASET_PATH"
echo "Language:                   $DATASET_LANGUAGE"
if [ -n "$CROSS_DATASET_PATH" ]; then
    print_message "$CYAN" "Cross-Dataset:              $CROSS_DATASET_PATH"
    print_message "$CYAN" "Cross-Language Analysis:    ENABLED ✓"
else
    echo "Cross-Dataset:              [not provided]"
    echo "Cross-Language Analysis:    disabled"
fi
if [ -n "$OUTPUT_DIR" ]; then
    echo "Output Directory:           $OUTPUT_DIR"
else
    echo "Output Directory:           [auto-generated]"
fi
echo "Subset Size:                $SUBSET_SIZE $([ "$SUBSET_SIZE" -eq 0 ] && echo '(all questions)' || echo 'questions')"
echo "Max Model Length:           $MAX_MODEL_LEN"
echo "Temperature:                $TEMPERATURE $([ "$TEMPERATURE" = "0.0" ] && echo '(deterministic)' || echo '')"
echo "Max Tokens:                 $MAX_TOKENS"
echo "GPU Memory Utilization:     $GPU_MEMORY_UTIL"
echo "Tensor Parallel Size:       $TENSOR_PARALLEL_SIZE $([ "$TENSOR_PARALLEL_SIZE" -gt 1 ] && echo 'GPUs' || echo 'GPU')"
echo "Random Seed:                $SEED"
echo ""
print_message "$YELLOW" "Analysis Options:"
echo "Topic Batch Analysis:       $([ "$ENABLE_TOPIC_BATCH_ANALYSIS" = true ] && echo 'ENABLED ⚠ (adds inference time)' || echo 'disabled')"
echo "Difficulty Batch Analysis:  $([ "$ENABLE_DIFFICULTY_BATCH_ANALYSIS" = true ] && echo 'ENABLED ⚠ (adds inference time)' || echo 'disabled')"
echo ""

# Cross-language analysis notes
if [ -n "$CROSS_DATASET_PATH" ]; then
    print_message "$CYAN" "Cross-Language Analysis Features:"
    echo "  • Stratified adjustment factors by topic and difficulty"
    echo "  • Token efficiency normalization between languages"
    echo "  • Fair performance comparison metrics"
    if [ "$DATASET_LANGUAGE" = "vietnamese" ]; then
        echo "  • Vietnamese metrics will be adjusted to English-equivalent"
    else
        echo "  • English baseline established (adjustment factor = 1.0)"
    fi
    echo "  • Generates: language_adjustment_factors.txt"
    echo ""
fi

if [ "$ENABLE_TOPIC_BATCH_ANALYSIS" = true ] || [ "$ENABLE_DIFFICULTY_BATCH_ANALYSIS" = true ]; then
    print_message "$YELLOW" "Note: Batch analysis is enabled. This will significantly increase evaluation time."
    print_message "$YELLOW" "      The model will run additional inference passes for detailed token metrics."
    echo ""
fi

# Confirm before proceeding
print_message "$BLUE" "Proceed with evaluation? (y/n) "
read -p "" -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_message "$YELLOW" "Evaluation cancelled."
    exit 0
fi

# Build command
CMD="python3 vllm_qa_evaluation.py \
    --model_name \"$MODEL_NAME\" \
    --dataset_path \"$DATASET_PATH\" \
    --dataset_language \"$DATASET_LANGUAGE\" \
    --subset_size $SUBSET_SIZE \
    --max_model_len $MAX_MODEL_LEN \
    --temperature $TEMPERATURE \
    --max_tokens $MAX_TOKENS \
    --gpu_memory_utilization $GPU_MEMORY_UTIL \
    --seed $SEED \
    --tensor_parallel_size $TENSOR_PARALLEL_SIZE"

if [ -n "$CROSS_DATASET_PATH" ]; then
    CMD="$CMD --cross_dataset_path \"$CROSS_DATASET_PATH\""
fi

if [ -n "$OUTPUT_DIR" ]; then
    CMD="$CMD --output_dir \"$OUTPUT_DIR\""
fi

if [ "$ENABLE_TOPIC_BATCH_ANALYSIS" = true ]; then
    CMD="$CMD --enable_topic_batch_analysis"
fi

if [ "$ENABLE_DIFFICULTY_BATCH_ANALYSIS" = true ]; then
    CMD="$CMD --enable_difficulty_batch_analysis"
fi

# Log start time
START_TIME=$(date +%s)
print_header "Starting Evaluation"
print_message "$GREEN" "Start Time: $(date)"
echo ""

# Run evaluation with output
eval $CMD

# Check if evaluation was successful
EXIT_CODE=$?
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
DURATION_MIN=$((DURATION / 60))
DURATION_SEC=$((DURATION % 60))

if [ $EXIT_CODE -eq 0 ]; then
    print_header "Evaluation Completed Successfully!"
    echo ""
    echo "Duration: ${DURATION_MIN}m ${DURATION_SEC}s (${DURATION} seconds)"
    
    # Determine output directory
    if [ -z "$OUTPUT_DIR" ]; then
        DATASET_NAME=$(basename "$DATASET_PATH" .jsonl)
        MODEL_SHORT=$(echo "$MODEL_NAME" | sed 's/\//-/g')
        OUTPUT_DIR="$(dirname "$DATASET_PATH")/${MODEL_SHORT}_${DATASET_NAME}"
    fi
    
    echo "Results saved to: $OUTPUT_DIR"
    echo ""
    
    # Display output files
    if [ -d "$OUTPUT_DIR" ]; then
        print_message "$GREEN" "Output files:"
        ls -lh "$OUTPUT_DIR" | grep -v "^total" | awk '{printf "  %-50s %10s\n", $9, $5}'
        echo ""
        
        # Highlight language adjustment factors if present
        if [ -f "$OUTPUT_DIR/language_adjustment_factors.txt" ]; then
            print_message "$CYAN" "✓ Language adjustment factors generated!"
            print_message "$CYAN" "  See: $OUTPUT_DIR/language_adjustment_factors.txt"
            echo ""
        fi
        
        # Display quick summary
        if [ -f "$OUTPUT_DIR/infer_result.txt" ]; then
            print_header "Quick Summary"
            grep -E "(Overall Accuracy|Total Questions|Correct:|Deviation Rate|Tokens per Second|Language-Adjusted)" "$OUTPUT_DIR/infer_result.txt" | head -15
            echo ""
            print_message "$BLUE" "Full results available in: $OUTPUT_DIR/infer_result.txt"
            echo ""
        fi
        
        # List visualization files
        PNG_COUNT=$(find "$OUTPUT_DIR" -name "*.png" 2>/dev/null | wc -l)
        if [ "$PNG_COUNT" -gt 0 ]; then
            print_message "$GREEN" "Generated $PNG_COUNT visualization plots"
            echo ""
        fi
    fi
    
else
    print_header "Evaluation Failed!"
    print_message "$RED" "Exit code: $EXIT_CODE"
    print_message "$YELLOW" "Duration before failure: ${DURATION_MIN}m ${DURATION_SEC}s"
    echo ""
    exit 1
fi
