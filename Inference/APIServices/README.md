TODO:

- [x] Add DeepSeek R1
- [x] Add Gemini: [gemini/batch-prediction/intro_batch_prediction.ipynb](https://github.com/GoogleCloudPlatform/generative-ai/blob/main/gemini/batch-prediction/intro_batch_prediction.ipynb)
- [x] Test inference pipeline for Azure (w/o and w/ formatted output - json_object and json_schema).
- [x] Add inference pipeline for Bedrock.
- [x] Clean code + refactor + write doc
- [x] Add inference pipeline for Groq (w/o and w/ formatted output).

Follow each step of this:
1. Setup environment:
    1. Installing [uv by astral](https://docs.astral.sh/uv/getting-started/installation/):
        ```bash
        curl -LsSf https://astral.sh/uv/install.sh | sh
        ```
        OR
        ```bash
        pip install uv
        ```
    1. Create environment:
        ```bash
        uv venv
        ```
    1. Activate venv:
        ```bash
        source .venv/bin/activate
        ```
    1. Install dependencies (from `pyproject.toml`):
        ```bash
        uv sync
        ```
1. Environment variable: create `.env` with api keys. See example: `.env.example`.
    - **Groq** uses `GROQ_API`
    - **Azure OpenAI** uses `AZURE_ENDPOINT` and `AZURE_OPENAI_API`
    - **AWS Bedrock** uses `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_ROLE_ARN`
    - **Gemini** uses `GEMINI_API_KEY`
    - **DeepSeek-R1** on Azure uses  `DEEPSEEK_ENDPOINT` and `DEEPSEEK_API_KEY`
1. Running:
    - Please raw data in `raw_data/`. E.g: `raw_data/data-processed-shuffled0.jsonl`
    - Write support function to format data as input data for services must be a list of strings of questions and answers.
    - Current testing for Groq and AWS: `batch_run.py`. use cmd:
    ```bash
    uv run batch_run.py
    ```
    - Prepared input file (jsonl) for batch in saved in `batch_in/<service_name>/`
    - Results and output are saved in `batch_out/<service_name>/`