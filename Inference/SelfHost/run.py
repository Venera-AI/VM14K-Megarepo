# %%
import sys

models = [
    ("microsoft/phi-4", False),
    ("OpenMeditron/Meditron3-8B", False),
    ("TsinghuaC3I/Llama-3.1-8B-UltraMedical", False),
    ("google/gemma-3-12b-it", False),
    ("google/gemma-3-27b-it", False),
    ("OpenMeditron/Meditron3-70B", False),
    ("TsinghuaC3I/Llama-3-70B-UltraMedical", False),
    ("unsloth/Llama-4-Scout-17B-16E-Instruct", False),
    ("deepseek-ai/DeepSeek-R1-Distill-Llama-8B", True),
    ("FreedomIntelligence/HuatuoGPT-o1-8B", True),
    ("Qwen/Qwen3-30B-A3B", True),
    ("Qwen/Qwen3-32B", True),
]

import subprocess

for model, reasoning in models:
    print(model)
    command = [
        sys.executable,
        "run_reasoning.py" if reasoning else "run_nonreasoning.py",
        model,
    ]
    command = list(map(str, command))
    subprocess.run(command)

subprocess.run(
    [
        'find . -type f -name "*.jsonl" -print0 | tar -czvf Archive.tar.gz --null -T -'
    ],
    shell=True,
)
