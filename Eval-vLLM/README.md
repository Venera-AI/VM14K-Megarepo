# VLLM Medical QA Evaluation with Cross-Language Analysis

> A comprehensive evaluation framework for testing large language models on medical question-answering benchmarks using vLLM for efficient batch inference with advanced cross-language performance normalization.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![vLLM](https://img.shields.io/badge/vLLM-0.6.0+-green.svg)](https://github.com/vllm-project/vllm)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Cross-Language Analysis Workflow](#cross-language-analysis-workflow)
- [Usage](#usage)
- [Configuration](#configuration)
- [Dataset Format](#dataset-format)
- [Output Files](#output-files)
- [Stratified Language Adjustment](#stratified-language-adjustment)
- [Performance](#performance)
- [Troubleshooting](#troubleshooting)


---

## Overview

This evaluation system assesses language model performance on medical QA datasets with support for:

- ✅ Multiple-choice medical questions
- 📊 Topic-based accuracy analysis
- 📈 Difficulty-level performance metrics
- 🔢 Token usage statistics
- 🌍 Bilingual support (English and Vietnamese)
- ⚖️ **Cross-language performance normalization with stratified adjustment factors**
- 📉 Comprehensive visualizations (10+ charts)
- 🎯 Response deviation/hallucination detection

---

## Features

### Core Capabilities

- **Stratified Language Adjustment** - Sophisticated cross-language performance normalization using topic-specific and difficulty-specific adjustment factors for fair comparison between English and Vietnamese datasets
- **Comprehensive Evaluation** - Measures overall accuracy with detailed breakdowns by medical topic and difficulty level
- **Response Quality Analysis** - Identifies and quantifies response deviations (hallucinations) where the model generates invalid answers
- **Performance Metrics** - Calculates token generation speed (tokens per second) with batch analysis by topic and difficulty
- **Batch Processing** - Efficient batch inference using vLLM with configurable batch sizes and GPU utilization
- **Detailed Logging** - Generates comprehensive output files with individual question analysis, ranked topic performance, and token statistics
- **Option Shuffling** - Prevents position bias by randomly shuffling answer choices
- **Cross-Dataset Integration** - Seamlessly loads and analyzes multiple datasets for adjustment factor calculation

### Advanced Features

- **StratifiedLanguageAdjuster Class** - ~200 lines of sophisticated adjustment logic
- **Topic-Difficulty Stratification** - Calculates adjustment factors at three levels: topic-only, difficulty-only, and combined
- **Confidence Scoring** - Each adjustment factor includes confidence scores based on sample sizes
- **Fallback Mechanisms** - Gracefully handles insufficient data with intelligent fallbacks
- **Language-Adjusted Metrics** - Both raw and adjusted metrics preserved in all outputs
- **Enhanced Visualizations** - All charts include language adjustment context when applicable

---

## Requirements

### Hardware Requirements

- CUDA-compatible GPU (tested with single GPU setup)
- Minimum 16GB GPU memory (recommended: 24GB+)
- 32GB+ system RAM (48GB+ recommended for cross-language analysis)

### Software Requirements

- Python 3.8+
- CUDA 11.8 or higher
- PyTorch 2.3.0+
- vLLM 0.6.0+

---

## Installation

1. **Clone the repository**
```bash
git clone https://github.com/Venera-AI/VM14K-Megarepo/Eval-vLLM.git
cd Eval-vLLM
```

2. **Install dependencies**
```bash
pip install "vllm>=0.6.0" "torch>=2.3.0" transformers pandas numpy matplotlib tqdm
```

3. **Configure environment variables** (optional)
```bash
export HF_HUB_ENABLE_HF_TRANSFER=1
export TRITON_DISABLE_LINE_INFO=1
export CUDA_VISIBLE_DEVICES=0
```

---

## Quick Start

### Using the Shell Script (Recommended)

```bash
# Make script executable
chmod +x run_evaluation.sh

# Run with default settings (no cross-language analysis)
./run_evaluation.sh -m Qwen/Qwen3-4B-Instruct-2507 -d medqa -l english

# Run with cross-language analysis (RECOMMENDED for fair comparison)
./run_evaluation.sh -m Qwen/Qwen3-4B-Instruct-2507 -d vm14k -l vietnamese --auto-cross
```

### Using Python Directly

```bash
# Basic evaluation
python vllm_qa_evaluation.py \
  --model_name "Qwen/Qwen3-4B-Instruct-2507" \
  --dataset_path "/Data/MedQA.jsonl" \
  --dataset_language "english"

# With cross-language analysis
python vllm_qa_evaluation.py \
  --model_name "Qwen/Qwen3-4B-Instruct-2507" \
  --dataset_path "/Data/VM14K.jsonl" \
  --cross_dataset_path "/Data/MedQA.jsonl" \
  --dataset_language "vietnamese" \
  --enable_topic_batch_analysis \
  --enable_difficulty_batch_analysis
```

---

## Cross-Language Analysis Workflow

### ⚠️ IMPORTANT: Dataset Evaluation Order

**For proper cross-language analysis with language adjustment factors and comprehensive token analysis, you MUST evaluate datasets in this specific order:**

### Step 1: Evaluate English (MedQA) Dataset First

```bash
./run_evaluation.sh \
  -m Qwen/Qwen3-4B-Instruct-2507 \
  -d medqa \
  -l english \
  --enable-all-batch
```

**Why English first?**
- Establishes the English baseline (adjustment factor = 1.0)
- Generates reference token metrics for comparison
- Creates baseline performance data
- English is used as the reference language for all adjustments

**Output:** 
- Standard evaluation results
- Topic and difficulty performance metrics
- Token generation statistics
- Visualization charts

### Step 2: Evaluate Vietnamese (VM14K) Dataset with Cross-Dataset

```bash
./run_evaluation.sh \
  -m Qwen/Qwen3-4B-Instruct-2507 \
  -d vm14k \
  -l vietnamese \
  -c medqa \
  --enable-all-batch
```

**Why second with cross-dataset?**
- Uses English dataset as reference for adjustment calculations
- Calculates topic-specific and difficulty-specific adjustment factors
- Normalizes Vietnamese performance to English-equivalent metrics
- Enables fair cross-language comparison

**Output:** 
- Standard evaluation results
- **`language_adjustment_factors.txt`** - Complete adjustment factor analysis
- **Language-adjusted metrics** in all text outputs (infer_result.txt, topics_ranked_by_accuracy.txt)
- Enhanced visualizations with adjustment context
- Both raw and adjusted performance metrics

### What Happens Without This Order?

❌ **Running Vietnamese First or Without Cross-Dataset:**
- No language adjustment factors generated
- Cannot fairly compare performance with English
- Missing normalized token efficiency metrics
- Vietnamese appears artificially faster (due to fewer tokens per question)

✅ **Running in Correct Order:**
- Fair performance comparison enabled
- Adjustment factors account for linguistic differences
- Token efficiency properly normalized
- Accurate cross-language performance metrics

### Alternative: Auto-Cross Mode (Simplified)

```bash
# Step 1: English (same as above)
./run_evaluation.sh -m MODEL -d medqa -l english --enable-all-batch

# Step 2: Vietnamese with auto-cross (automatically uses medqa as cross-dataset)
./run_evaluation.sh -m MODEL -d vm14k -l vietnamese --auto-cross --enable-all-batch
```

### Manual Paths (Non-Preset Datasets)

```bash
# Step 1: English evaluation
python vllm_qa_evaluation.py \
  --model_name MODEL \
  --dataset_path /Data/MedQA.jsonl \
  --dataset_language english \
  --enable_topic_batch_analysis \
  --enable_difficulty_batch_analysis

# Step 2: Vietnamese with cross-dataset
python vllm_qa_evaluation.py \
  --model_name MODEL \
  --dataset_path /Data/VM14K.jsonl \
  --cross_dataset_path /Data/MedQA.jsonl \
  --dataset_language vietnamese \
  --enable_topic_batch_analysis \
  --enable_difficulty_batch_analysis
```

---

## Usage

### Basic Example (Single Language)

```bash
# Evaluate English only
./run_evaluation.sh \
  -m Qwen/Qwen3-4B-Instruct-2507 \
  -d /Data/MedQA.jsonl \
  -l english \
  -s 10000
```

### Cross-Language Example (Recommended)

```bash
# Step 1: English
./run_evaluation.sh \
  -m Qwen/Qwen3-4B-Instruct-2507 \
  -d /Data/MedQA.jsonl \
  -l english \
  --enable-all-batch

# Step 2: Vietnamese with cross-analysis
./run_evaluation.sh \
  -m Qwen/Qwen3-4B-Instruct-2507 \
  -d /Data/VM14K.jsonl \
  -c /Data/MedQA.jsonl \
  -l vietnamese \
  --enable-all-batch
```

### Multi-GPU Example

```bash
./run_evaluation.sh \
  -m meta-llama/Llama-2-70b-chat-hf \
  -d medqa \
  -l english \
  --tensor-parallel 4 \
  --gpu-memory 0.95
```

---

## Configuration

### Model Configuration

The default model is **Qwen/Qwen3-4B-Instruct-2507** with the following settings:

| Parameter | Default Value | Description |
|-----------|--------------|-------------|
| `tensor_parallel_size` | 1 | Number of GPUs for tensor parallelism |
| `gpu_memory_utilization` | 0.90 | Fraction of GPU memory to use |
| `max_model_len` | 2048 | Maximum context length |

### Sampling Parameters

| Parameter | Default Value | Description |
|-----------|--------------|-------------|
| `max_tokens` | 250 | Maximum tokens per response |
| `temperature` | 0.0 | Sampling temperature (0 = deterministic) |
| `seed` | 42 | Random seed for reproducibility |

### Command-Line Arguments

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `--model_name` | str | **Required** | HuggingFace model identifier |
| `--dataset_path` | str | **Required** | Path to JSONL dataset file |
| `--cross_dataset_path` | str | None | Path to cross-language dataset for adjustment factors |
| `--output_dir` | str | auto | Directory for output files |
| `--dataset_language` | str | english | Dataset language (english/vietnamese) |
| `--subset_size` | int | 10000 | Number of questions to evaluate (0 = all) |
| `--max_model_len` | int | 2048 | Maximum model context length |
| `--temperature` | float | 0.0 | Sampling temperature |
| `--max_tokens` | int | 250 | Max tokens per response |
| `--gpu_memory_utilization` | float | 0.9 | GPU memory usage fraction |
| `--seed` | int | 42 | Random seed |
| `--tensor_parallel_size` | int | 1 | Number of GPUs for parallelism |
| `--enable_topic_batch_analysis` | flag | False | Enable topic-based batch analysis |
| `--enable_difficulty_batch_analysis` | flag | False | Enable difficulty-based batch analysis |

### Shell Script Options

```bash
# Required
-m, --model MODEL_NAME          # Model to evaluate
-d, --dataset DATASET_PATH      # Dataset file or preset (medqa, vm14k)

# Cross-Language
-c, --cross-dataset PATH        # Cross-language dataset for adjustments
--auto-cross                    # Auto-select cross-dataset (for presets)

# Optional
-o, --output OUTPUT_DIR         # Output directory
-l, --language LANGUAGE         # Dataset language (english|vietnamese)
-s, --subset SUBSET_SIZE        # Number of questions

# Analysis
--enable-topic-batch            # Topic batch analysis
--enable-difficulty-batch       # Difficulty batch analysis
--enable-all-batch              # Both batch analyses

# Help
-h, --help                      # Show help message
```

---

## Dataset Format

### Supported Datasets

1. **MedQA (English)** - Multiple-choice medical questions covering various medical topics
2. **VM14K (Vietnamese)** - Vietnamese medical questions dataset

### Required Fields

```json
{
  "question": "A 45-year-old patient presents with chest pain. What is the most appropriate initial test?",
  "options": {
    "A": "Chest X-ray",
    "B": "ECG",
    "C": "CT scan",
    "D": "Blood test"
  },
  "answer_index": "B",
  "medical_topic": "Cardiology",
  "difficulty_level": "Moderate"
}
```

### Dataset Preparation

1. Prepare your dataset in JSONL format (one JSON object per line)
2. Ensure all required fields are present:
   - `question`: The medical question text
   - `options`: Dictionary or list of answer choices (A, B, C, D, etc.)
   - `answer_index`: Index or letter of correct answer
   - `medical_topic`: Medical specialty/topic category
   - `difficulty_level`: Question difficulty (Easy, Moderate, Hard, Challenging)
3. Place the dataset file in an accessible location
4. For cross-language analysis, prepare both English and Vietnamese versions

---

## Output Files

The evaluation generates several output files in the specified output directory:

### Standard Output Files

| File | Description |
|------|-------------|
| `model_output.txt` | Detailed question-by-question results with model responses |
| `infer_result.txt` | Comprehensive performance analysis with topic/difficulty breakdowns |
| `topics_ranked_by_accuracy.txt` | Medical topics ranked by model accuracy |

### Cross-Language Analysis Files (When Cross-Dataset Provided)

| File | Description |
|------|-------------|
| `language_adjustment_factors.txt` | **Complete adjustment factor analysis** - Topic-level, difficulty-level, and combined factors with sample sizes |

### Enhanced Content (Vietnamese with Cross-Dataset)

All standard files contain **additional language-adjusted metrics**:
- Raw performance metrics (original)
- Language-adjusted performance metrics (normalized to English)
- Adjustment factors used
- Confidence scores

### Visualizations (10+ PNG files)

- `topic_performance_accuracy.png` - Accuracy by medical topic
- `difficulty_performance.png` - Accuracy by difficulty level
- `deviation_rate_by_difficulty.png` - Hallucination rate analysis
- `deviation_rate_by_topic.png` - Topic-specific deviation rates
- `batch_tokens_per_second_by_topics.png` - Token generation speed by topic
- `batch_tokens_per_second_by_difficulty.png` - Token speed by difficulty
- `performance_dashboard.png` - Comprehensive dashboard view
- Additional charts for topic-difficulty combinations

---

## Stratified Language Adjustment

The Stratified Language Adjustment system enables fair performance comparisons between English and Vietnamese medical datasets by accounting for inherent linguistic differences.

### Why Language Adjustment is Needed

Different languages have varying complexities when processed by language models:

- **Token efficiency varies** - Vietnamese medical questions often use fewer tokens than English equivalents while conveying the same medical information
- **Semantic density differences** - Some languages pack more meaning into fewer tokens
- **Sentence structure differences** - Affects processing time and computational requirements
- **Medical terminology translation** - Different verbosity patterns across languages
- **Character encoding efficiency** - Tokenization efficiency varies significantly between languages

**Without adjustment:** Vietnamese appears artificially faster simply because questions use fewer tokens, not because the model performs better.

**With adjustment:** Performance is normalized to reflect actual reasoning capability independent of language token efficiency.

### How It Works

#### 1. Stratified Factor Calculation

Adjustment factors are calculated at three levels:

- **Topic-level factors** - For each medical topic (Cardiology, Neurology, Pharmacology, etc.)
  - Requires 5+ samples per topic in each language
  
- **Difficulty-level factors** - For each difficulty tier (Easy, Moderate, Hard, Challenging)
  - Requires 5+ samples per difficulty in each language
  
- **Topic-Difficulty combinations** - Most granular level combining both
  - Requires 3+ samples per combination in each language
  - Provides most accurate adjustments

#### 2. Language Metrics Analysis

The system compares Vietnamese vs English samples using multiple metrics:

```
Key metrics calculated:
  token_ratio = vietnamese_avg_tokens / english_avg_tokens
  question_length_ratio = vietnamese_avg_chars / english_avg_chars
  combined_factor = (token_ratio + length_ratio) / 2
```

#### 3. Adjustment Factor Application

The factors are applied to normalize Vietnamese performance metrics:

```
Adjusted Performance Metrics:
  adjusted_tokens_per_second = raw_tokens_per_second × adjustment_factor
  adjusted_questions_per_second = raw_questions_per_second ÷ adjustment_factor
```

#### 4. Fallback Hierarchy

When insufficient data exists for a specific combination:

1. **Best**: Use topic-difficulty specific factor (requires 3+ samples)
2. **Good**: Use topic-only factor (requires 5+ samples)
3. **Acceptable**: Use difficulty-only factor (requires 5+ samples)
4. **Default**: No adjustment (factor = 1.0)

### Example Adjustment

**Real-world scenario:**

```
Cardiology Topic, Moderate Difficulty:
  English samples: 850 questions, avg 180 tokens per question
  Vietnamese samples: 1200 questions, avg 110 tokens per question
  
  Token ratio: 110/180 = 0.611
  Length ratio: (vietnamese avg chars) / (english avg chars) = 0.650
  Combined factor: (0.611 + 0.650) / 2 = 0.631
  
  Raw Vietnamese performance: 140 questions/second
  Adjusted performance: 140 ÷ 0.631 = 221.9 questions/second
  
  Interpretation: When accounting for token efficiency, Vietnamese 
  requires ~58% more computational effort than raw metrics suggest
```

### Generated Factor File

The `language_adjustment_factors.txt` contains:

```
Topic: Cardiology
  Token Ratio (VI/ENG): 0.611
  Combined Factor: 0.631
  Sample Sizes - ENG: 850, VI: 1200
  Confidence: 1.0 (85 samples)

Difficulty: Moderate
  Token Ratio (VI/ENG): 0.623
  Combined Factor: 0.645
  Sample Sizes - ENG: 2500, VI: 3200
  Confidence: 1.0 (250 samples)

Topic-Difficulty: Cardiology + Moderate
  Token Ratio (VI/ENG): 0.608
  Combined Factor: 0.628
  Sample Sizes - ENG: 420, VI: 580
  Confidence: 1.0 (42 samples)
```

---

## Performance

### Typical Metrics

| Metric | Value | Notes |
|--------|-------|-------|
| Processing speed | ~200 Q/s | With vLLM optimization |
| GPU memory | 8-12GB | For 4B parameter models |
| Evaluation time | 5-10 min | For 10K questions |
| Cross-dataset loading | +30 sec | One-time calculation |
| Batch analysis overhead | +2-3 min | Per batch type enabled |

### Performance Impact of Cross-Language Analysis

| Component | Memory | Time | Notes |
|-----------|--------|------|-------|
| Base evaluation | 8-12GB | 5-10 min | Standard |
| Cross-dataset loading | +500MB | +5 sec | One-time |
| Factor calculation | +10MB | +30 sec | One-time |
| Adjusted metrics | +50MB | +2 sec | During analysis |
| **Total overhead** | **+560MB** | **+37 sec** | **~10% increase** |

### Answer Extraction Methods

The system uses multiple fallback methods to extract answers:

1. **Exact pattern matching** - "Final Answer: A"
2. **Contextual keywords** - "The answer is A", "Correct answer: A"
3. **Parenthetical** - "(A)" or "[A]"
4. **Direct letter** - First valid letter (A-H) found
5. **None detection** - Identifies response deviations

### Batch Analysis Benefits

When enabled via `--enable-all-batch`:

- **Topic-based analysis** - Reveals which medical specialties generate tokens faster
- **Difficulty-based analysis** - Shows how question complexity affects speed
- **Combined insights** - Identifies optimization opportunities

**Trade-off:** Adds 2-3 minutes per analysis type, but provides valuable performance insights.

---

## Troubleshooting

### Out of Memory Errors

```bash
# Reduce GPU memory utilization
--gpu_memory_utilization 0.7

# Decrease maximum model length
--max_model_len 1024

# Reduce subset size
--subset_size 5000

# Use single batch analysis instead of both
--enable-topic-batch  # Instead of --enable-all-batch
```

### Cross-Dataset Not Found

```bash
# Check file paths
ls -l /Data/MedQA.jsonl
ls -l /Data/VM14K.jsonl

# Use absolute paths
./run_evaluation.sh -m MODEL -d /full/Data/VM14K.jsonl -c /full/Data/MedQA.jsonl
```

### Missing Language Adjustment Factors

**Problem:** `language_adjustment_factors.txt` not generated

**Solutions:**
1. Ensure you provided `--cross_dataset_path`
2. Check cross-dataset has sufficient samples (100+ recommended)
3. Verify cross-dataset has matching fields (medical_topic, difficulty_level)

### Slow Performance

```bash
# Check CUDA
python -c "import torch; print(torch.cuda.is_available())"

# Monitor GPU
watch -n 1 nvidia-smi

# Reduce context if not needed
--max_model_len 1024

# Disable batch analysis for speed
# Don't use --enable-all-batch
```

### Import Errors

```bash
# Reinstall vLLM
pip install --upgrade --force-reinstall vllm

# Check PyTorch CUDA
python -c "import torch; print(torch.version.cuda)"

# Verify all packages
pip install --upgrade transformers pandas numpy matplotlib tqdm
```

### Incorrect Language Labels in Output

**Problem:** Vietnamese output shows English metrics or vice versa

**Solution:** Always specify `--dataset_language` correctly:

```bash
# Correct
-l english  # When evaluating English dataset
-l vietnamese  # When evaluating Vietnamese dataset

# Incorrect (will cause wrong adjustments)
-l vietnamese  # But evaluating English dataset
```

---

## Best Practices

### 1. Always Follow Evaluation Order

✅ **Correct:**
```bash
# First: English
./run_evaluation.sh -m MODEL -d medqa -l english

# Second: Vietnamese with cross-dataset
./run_evaluation.sh -m MODEL -d vm14k -l vietnamese -c medqa
```

❌ **Incorrect:**
```bash
# Vietnamese without cross-dataset or before English
./run_evaluation.sh -m MODEL -d vm14k -l vietnamese
```

### 2. Use Batch Analysis for Comprehensive Results

```bash
# Recommended for production evaluation
--enable-all-batch

# Use for quick testing only
# (no batch analysis flags)
```

### 3. Verify Dataset Quality

```bash
# Check sample count
wc -l dataset.jsonl

# Verify required fields
head -1 dataset.jsonl | jq 'keys'

# Should show: ["question", "options", "answer_index", "medical_topic", "difficulty_level"]
```

### 4. Monitor Resource Usage

```bash
# Before starting
nvidia-smi
free -h

# During evaluation (separate terminal)
watch -n 2 nvidia-smi
```

---

## Quick Reference Card

### Most Common Commands

```bash
# 1. English evaluation (run first)
./run_evaluation.sh -m Qwen/Qwen3-4B-Instruct-2507 -d medqa -l english --enable-all-batch

# 2. Vietnamese with cross-language analysis (run second)
./run_evaluation.sh -m Qwen/Qwen3-4B-Instruct-2507 -d vm14k -l vietnamese -c medqa --enable-all-batch

# 3. Quick test (small subset)
./run_evaluation.sh -m Qwen/Qwen3-4B-Instruct-2507 -d medqa -l english -s 1000

# 4. Multi-GPU
./run_evaluation.sh -m MODEL -d medqa --tensor-parallel 4
```

### Expected Output Files

**English Only:**
- model_output.txt
- infer_result.txt
- topics_ranked_by_accuracy.txt
- 10+ PNG visualization files

**Vietnamese with Cross-Dataset (adds):**
- ✨ language_adjustment_factors.txt
- Enhanced metrics in all text files
- Adjustment context in visualizations

---

**Remember:** For proper cross-language token analysis, always run MedQA (English) first, then VM14K (Vietnamese) with cross-dataset enabled!
