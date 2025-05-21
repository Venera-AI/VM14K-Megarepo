import argparse
import csv
import json
import os
import sys
from typing import List, Tuple, Dict, Any

from vllm import LLM, SamplingParams
from vllm.sampling_params import GuidedDecodingParams


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run model evaluation on multiple-choice questions")
    parser.add_argument("model", type=str, help="Path or name of the model to use")
    return parser.parse_args()


def create_input_output_config(model_name: str) -> List[Tuple[str, str, int]]:
    """Create configuration for input/output files and evaluation passes."""
    return [
        (
            f"data-processed-shuffled{i}.jsonl",
            f"{model_name}/data-processed-shuffled{i}.jsonl",
            3 if i == 0 else 1,
        )
        for i in range(3)
    ]


def format_option(json_data: str or Dict[str, Any]) -> str:
    """Format a question and its options into a prompt string."""
    if isinstance(json_data, str):
        data = json.loads(json_data)
    else:
        data = json_data
    
    result = f"{data['question']} Choose the correct option from these answers: \n"
    for idx, option in enumerate(data["options"]):
        result += f"{chr(ord('A') + idx)}. {option}\n"
    
    return result + OUTPUT_GUIDE


def initialize_model(model_name: str) -> Tuple[LLM, SamplingParams]:
    """Initialize the language model and its sampling parameters."""
    llm = LLM(
        model=model_name,
        tensor_parallel_size=1,
        gpu_memory_utilization=0.97,
        max_num_seqs=300,
        max_model_len=2000,
        dtype="bfloat16",
    )
    
    sampling_params = llm.get_default_sampling_params()
    
    # Set up guided decoding for models that support it
    if not ("gemma" in model_name or "phi" in model_name):
        print("Using guided decoding")
        guided_decoding_params = GuidedDecodingParams(choice=["A", "B", "C", "D", "E", "F", "G"])
        sampling_params.max_tokens = 1024
        sampling_params.guided_decoding = guided_decoding_params
    
    print(sampling_params)
    return llm, sampling_params


def process_file(
    llm: LLM, 
    sampling_params: SamplingParams, 
    input_file: str, 
    output_file: str, 
    num_passes: int
) -> None:
    """Process a single input file and save the model's predictions."""
    # Read inputs
    with open(input_file) as f:
        inputs = [
            [{"role": "user", "content": format_option(json.loads(row))}] 
            for row in f
        ]
        # Uncomment to process a subset for testing
        # inputs = inputs[:100]
    
    # Run model
    sampling_params.n = num_passes
    outputs = llm.chat(messages=inputs, sampling_params=sampling_params)
    
    # Save results
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    results = [(answer.text.strip() for answer in output.outputs) for output in outputs]
    
    with open(output_file, "w") as f:
        writer = csv.writer(f)
        writer.writerow([f"answer{i}" for i in range(num_passes)])
        writer.writerows(results)


def test_model_on_example(llm: LLM, sampling_params: SamplingParams) -> None:
    """Test the model on a single example."""
    example_question = """
    Kết quả dịch màng phổi của một bệnh nhân là dịch tiết, 200 tế bào, ADA 4000 UI/L phát biểu nào sau đây là đúng:
    Choose the correct option from these answers:
    A. ADA cao nên có thể chẩn đoán lao màng phổi và cho thuốc lao ngay.
    B. Chưa thể loại trừ ung thư màng phổi.
    C. Chưa thể loại trừ mủ màng phổi.
    D. Tất cả đều sai.

    Only response with 1 character
    Example:
    A
    """
    
    response = llm.chat(
        messages=[{"role": "user", "content": example_question}],
        sampling_params=sampling_params,
    )[0].outputs[0].text
    
    print(f"Example response: {response}")


def main() -> None:
    """Main function to run the evaluation."""
    args = parse_arguments()
    
    # Configure input/output files
    io_config = create_input_output_config(args.model)
    
    # Initialize model
    llm, sampling_params = initialize_model(args.model)
    
    test_model_on_example(llm, sampling_params)
    
    # Process all files
    for input_file, output_file, num_passes in io_config:
        print(f"Processing {input_file} -> {output_file} with {num_passes} passes")
        process_file(llm, sampling_params, input_file, output_file, num_passes)


OUTPUT_GUIDE = """
Only response with 1 character
Example:
A"""


if __name__ == "__main__":
    main()
    sys.exit(0)