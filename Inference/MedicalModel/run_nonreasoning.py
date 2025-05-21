# %%
import sys

# %%
assert len(sys.argv) == 2
model = sys.argv[1]
ios = [
    (
        f"data-processed-shuffled{i}.jsonl",
        f"{model}/data-processed-shuffled{i}.jsonl",
        3 if i == 0 else 1,
    )
    for i in range(3)
]

# %%
output_guide = """

Only response with 1 character
Example:
A"""

# %%
import json


# Khuc nay chuyen tu csv thanh cau hoi
def format_option(json_str):
    if isinstance(json_str, str):
        tmp = json.loads(json_str)
    else:
        tmp = json_str
    result = f"""{tmp["question"]}
Choose the correct option from these answers:
"""
    for idx, option in enumerate(tmp["options"]):
        result += f"{chr(ord('A') + idx)}. {option}\n"

    return result + output_guide


# %%
from vllm import LLM, SamplingParams
from vllm.sampling_params import GuidedDecodingParams

llm = LLM(
    model=model,
    tensor_parallel_size=1,
    gpu_memory_utilization=0.97,
    max_num_seqs=300,
    max_model_len=2000,
    dtype="bfloat16",
)

# %%
guided_decoding_params = GuidedDecodingParams(
    choice=["A", "B", "C", "D", "E", "F", "G"]
)
sampling_params = llm.get_default_sampling_params()
if not ("gemma" in model or "phi" in model):
    print("Using guided decoding")
    sampling_params.max_tokens = 1024
    sampling_params.guided_decoding = guided_decoding_params
print(sampling_params)

# %%
print(
    llm.chat(
        messages=[
            {
                "role": "user",
                "content": "Kết quả dịch màng phổi của một bệnh nhân là dịch tiết, 200 tế bào, ADA 4000 UI/L phát biểu nào sau đây là đúng:\nChoose the correct option from these answers:\nA. ADA cao nên có thể chẩn đoán lao màng phổi và cho thuốc lao ngay.\nB. Chưa thể loại trừ ung thư màng phổi.\nC. Chưa thể loại trừ mủ màng phổi.\nD. Tất cả đều sai.\n\n\nOnly response with 1 character\nExample:\nA",
            }
        ],
        sampling_params=sampling_params,
    )[0]
    .outputs[0]
    .text
)

# %%
import csv
import os
import json

for input_file, output_file, num_pass in ios:
    with open(input_file) as f:
        inputs = [
            [{"role": "user", "content": format_option(json.loads(row))}] for row in f
        ]
        # inputs = inputs[:100]
    # for i in range(num_pass):

    sampling_params.n = num_pass
    outputs = llm.chat(
        messages=inputs,
        sampling_params=sampling_params,
    )
    output_path = output_file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    results = [(answer.text.strip() for answer in output.outputs) for output in outputs]
    with open(output_path, "w") as f:
        writer = csv.writer(f)
        writer.writerow([f"answer{i}" for i in range(num_pass)])
        writer.writerows(results)

exit(0)
