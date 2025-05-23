# %%
import sys

# %%
assert len(sys.argv) == 2
model = sys.argv[1]
ios = [
    (
        f"data-processed-shuffled{i}.jsonl",
        f"{model}/data-processed-shuffled{i}.jsonl",
        1,
    )
    for i in range(3)
]
is_huatuo = model == "FreedomIntelligence/HuatuoGPT-o1-8B"
is_r1_weak = model == "deepseek-ai/DeepSeek-R1-Distill-Llama-8B"

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
    max_num_seqs=250,
    max_model_len=8000,
)

# %%
guided_decoding_params = GuidedDecodingParams(
    choice=["A", "B", "C", "D", "E", "F", "G"]
)
huatuo_start_final_response = "## Final Response\n\n"
r1_end_of_think = "</think>"
sampling_params = llm.get_default_sampling_params()
sampling_params.max_tokens = 8000
chat_template = None
# sampling_params.guided_decoding = guided_decoding_params

if is_huatuo:
    sampling_params.stop = [huatuo_start_final_response]
if is_r1_weak:
    sampling_params.stop = [r1_end_of_think]
    # Removed content.split('</think>')[-1]
    chat_template = "{% if not add_generation_prompt is defined %}{% set add_generation_prompt = false %}{% endif %}{% set ns = namespace(is_first=false, is_tool=false, is_output_first=true, system_prompt='') %}{%- for message in messages %}{%- if message['role'] == 'system' %}{% set ns.system_prompt = message['content'] %}{%- endif %}{%- endfor %}{{bos_token}}{{ns.system_prompt}}{%- for message in messages %}{%- if message['role'] == 'user' %}{%- set ns.is_tool = false -%}{{'<｜User｜>' + message['content']}}{%- endif %}{%- if message['role'] == 'assistant' and message['content'] is none %}{%- set ns.is_tool = false -%}{%- for tool in message['tool_calls']%}{%- if not ns.is_first %}{{'<｜Assistant｜><｜tool▁calls▁begin｜><｜tool▁call▁begin｜>' + tool['type'] + '<｜tool▁sep｜>' + tool['function']['name'] + '\\n' + '```json' + '\\n' + tool['function']['arguments'] + '\\n' + '```' + '<｜tool▁call▁end｜>'}}{%- set ns.is_first = true -%}{%- else %}{{'\\n' + '<｜tool▁call▁begin｜>' + tool['type'] + '<｜tool▁sep｜>' + tool['function']['name'] + '\\n' + '```json' + '\\n' + tool['function']['arguments'] + '\\n' + '```' + '<｜tool▁call▁end｜>'}}{{'<｜tool▁calls▁end｜><｜end▁of▁sentence｜>'}}{%- endif %}{%- endfor %}{%- endif %}{%- if message['role'] == 'assistant' and message['content'] is not none %}{%- if ns.is_tool %}{{'<｜tool▁outputs▁end｜>' + message['content'] + '<｜end▁of▁sentence｜>'}}{%- set ns.is_tool = false -%}{%- else %}{% set content = message['content'] %}{{'<｜Assistant｜>' + content + '<｜end▁of▁sentence｜>'}}{%- endif %}{%- endif %}{%- if message['role'] == 'tool' %}{%- set ns.is_tool = true -%}{%- if ns.is_output_first %}{{'<｜tool▁outputs▁begin｜><｜tool▁output▁begin｜>' + message['content'] + '<｜tool▁output▁end｜>'}}{%- set ns.is_output_first = false %}{%- else %}{{'\\n<｜tool▁output▁begin｜>' + message['content'] + '<｜tool▁output▁end｜>'}}{%- endif %}{%- endif %}{%- endfor -%}{% if ns.is_tool %}{{'<｜tool▁outputs▁end｜>'}}{% endif %}{% if add_generation_prompt and not ns.is_tool %}{{'<｜Assistant｜><think>\\n'}}{% endif %}"

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
    if is_huatuo or is_r1_weak:
        thoughts = []
        final_response_guide = (
            huatuo_start_final_response if is_huatuo else r1_end_of_think
        )
        final_response_guide += "So the final choice (A, B, C, D,...) is: "
        for question, output in zip(inputs, outputs):
            # Expand output (num_pass != 1) => len(thoughts) == len(inputs) * num_pass
            thoughts.extend(
                question
                + [{"role": "assistant", "content": answer.text + final_response_guide}]
                for answer in output.outputs
            )
        sampling_params2 = sampling_params.clone()
        sampling_params2.n = 1
        sampling_params2.guided_decoding = guided_decoding_params

        outputs = llm.chat(
            messages=thoughts,
            sampling_params=sampling_params2,
            add_generation_prompt=False,
            continue_final_message=True,
        )
        results_expanded = [
            (output.outputs[0].text.strip(), inp[-1]["content"])
            for inp, output in zip(thoughts, outputs)
        ]
        results = [[] for _ in range(len(inputs))]
        for i, result in enumerate(results_expanded):
            results[i // num_pass].extend(result)
        with open(output_path, "w") as f:
            writer = csv.writer(f)
            header = []
            for i in range(num_pass):
                header.append(f"answer{i}")
                header.append(f"thought{i}")
            writer.writerow(header)
            writer.writerows(results)
    else:
        results = [
            (answer.text.strip() for answer in output.outputs) for output in outputs
        ]
        with open(output_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow([f"answer{i}" for i in range(num_pass)])
            writer.writerows(results)

exit(0)
