import os
import time
from typing import List, Dict
from dotenv import load_dotenv, find_dotenv
from eval import eval_acc
from prompts.deepseekPrompt import deepseekPrompt
from prompts.generalPrompt import generalPrompt

load_dotenv(find_dotenv())
from services import (
    BatchInference,
    GroqInference,
    AzureInference,
    AWSInference,
    GeminiInference,
)
from model import ResponseSchema
from services.deepseek import DeepseekInference


def get_service(
    serviceName: str,
    api_key: str,
    model_name: str,
    system_prompt: str,
    response_format,
    k:int= 1,
    azure_endpoint: str = None,
    aws_access_key_id: str = None,
    awsRoleArn: str = None,
    awsRegion: str = None,
):
    """
    Args:
        serviceName: service for inference: groq, azure (gpt), aws for batch OR gemini, azure (deepseek) for single call
        api_key: secret key to use for each service.
        model_name: name of LLM to use
        system_prompt: prompt with system role
        response_format: None or Dict or Pydantic schema (see examples below)
        azure_endpoint: (Azure only) endpoint link
        aws_access_key_id: (AWS only) key id to use AWS service
        awsRoleArn: (AWS only) contact root user for more info (relating to IAM)
        awsRegion: (AWS only) currently we are using Tokyo (ap-northeast-1)
    Note:
        Azure: "model_name" is also "azure_deployment" name on Azure AI Foundry
        AWS: "api_key" is also "AWS_SECRET_ACCESS_KEY"
        response_format:
            Groq: only support JSON mode ("json" keyword must include in either system prompt or request). Use response_format={"type": "json_object"}
            Azure (gpt): support JSON mode and JSON schema (recommended). Use response_format={"type": "json_schema", "json_schema": {"name": "schemaName", "schema": PydanticSchema.model_json_schema()}}
            Azure (deepseek): NO support structured output (response_format=None). Recommended using system prompt. E.g: system_prompt="Return ONLY the answer wrapped in XML tags: <answer>string</answer>"
            AWS bedrock: NO support structured output(response_format=None). Recommended using system prompt. E.g: system_prompt="Return your response as a valid JSON object for each question that strictly follows this structure: {'answer': string}. For example: {'answer': 'A'}"
            Gemini: support JSON schema (recommended). To increase inference speed, we recommend include multiple prompts and use response_format=list[PydanticSchema]
        Non batch models:
            Azure (deepseek): recommended self.batch_size=20 (check ./services/azure.py)
            Gemini: recommended self.batch_size=10 or 20 (check ./services/gemini.py)
    Example:
        # groq
        service = get_service(serviceName = 'groq',
                            api_key = os.getenv("GROQ_API"),
                            model_name = "meta-llama/llama-4-maverick-17b-128e-instruct",
                            system_prompt = myPrompt,
                            response_format = {"type": "json_object"})

        # azure (gpt)
        service = get_service(serviceName = 'azure',
                            api_key = os.getenv("AZURE_OPENAI_API"),
                            model_name = "gpt-4o-globalBatch",
                            system_prompt = myPrompt,
                            response_format = {"type": "json_schema",
                                        "json_schema": {"name": "mySchema", "schema": ResponseSchema.model_json_schema()}},
                            azure_endpoint=os.getenv("AZURE_ENDPOINT"))

        # azure (deepseek)
        service = get_service(serviceName = 'azure',
                            api_key = os.getenv("DEEPSEEK_API_KEY"),
                            model_name = "DeepSeek-R1",
                            system_prompt = deepseekPrompt,
                            response_format = None,
                            azure_endpoint=os.getenv("DEEPSEEK_ENDPOINT"))

        # aws bedrock
        service = get_service(serviceName = 'aws',
                            api_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
                            model_name = "anthropic.claude-3-5-sonnet-20240620-v1:0",
                            system_prompt = myPrompt,
                            response_format = None,
                            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
                            awsRoleArn = os.getenv("AWS_ROLE_ARN"),
                            awsRegion = "ap-northeast-1")

        # gemini
        service = get_service(serviceName = 'gemini',
                            api_key = os.getenv("GEMINI_API_KEY"),
                            model_name = "gemini-2.0-flash",
                            system_prompt = myPrompt,
                            response_format = list[ResponseSchema])
    """
    if serviceName == "groq":
        return GroqInference(
            api_key=api_key,
            model_name=model_name,
            system_prompt=system_prompt,
            response_format=response_format,
        )
    elif serviceName == "azure":
        return AzureInference(
            api_key=api_key,
            model_name=model_name,  # Quan: "gpt-4o-globalBatch"
            system_prompt=system_prompt,
            response_format=response_format,
            azure_endpoint=azure_endpoint,
        )
    elif serviceName == "aws":
        assert (
            (aws_access_key_id is not None)
            and (awsRoleArn is not None)
            and (awsRegion is not None)
        ), f"Either aws_access_key_id, awsRoleArn, awsRegion is NONE. They must have value"
        return AWSInference(
            api_key=api_key,
            model_name=model_name,
            system_prompt=system_prompt,
            response_format=response_format,
            aws_access_key_id=aws_access_key_id,
            roleArn=awsRoleArn,
            region=awsRegion,
        )
    elif serviceName == "gemini":
        return GeminiInference(
            api_key=api_key,
            model_name=model_name,
            system_prompt=system_prompt,
            response_format=response_format,
        )
    elif serviceName == "deepseek":
        return DeepseekInference(
            api_key=os.getenv("DEEPSEEK_KEY"),
            model_name=model_name,
            system_prompt=system_prompt,
            response_format=response_format,
            k=k,
        )
    else:
        raise Exception("Unrecognize service")


def format_option(datum: dict, isDefaultID: bool = False):
    """Need to rewrite depending on your data
    Args:
        isDefaultID: if default, just return string. Otherwise follow this format: {"id": string, 'content': string}
    """
    item = f"""Question: {datum["question"]}\nChose the correct option from these answers:\n"""
    for i, opt in enumerate(datum["options"]):
        item += f"{chr(ord('A') + i)}: {opt}\n"
    if isDefaultID:
        return item  # to use default custom_id
    else:
        return {"id": datum["id"], "content": item}  # to asign custom_id = datum's id


if __name__ == "__main__":
    # data = BatchInference.readJSONL("raw_data/data-processed-shuffled0.jsonl")
    # data = BatchInference.readJSONL('raw_data/data-processed-shuffled1.jsonl')
    data = BatchInference.readJSONL('raw_data/data-processed-shuffled2.jsonl')

    data_formatted = [format_option(datum) for datum in data]

    pass_at_k = 1
    service = get_service(
        serviceName="deepseek",
        api_key=os.getenv("GROQ_API"),
        k=pass_at_k,
        model_name="deepseek-reasoner",
        system_prompt=deepseekPrompt,
        response_format={"type": "json_object"},
    )
    start_time = time.time()  
    service.run_batch(data_formatted=data_formatted)
    end_time = time.time()  
    elapsed_time = end_time - start_time  

    print(f"Time elapsed: {elapsed_time:.2f} seconds")
    # BatchInference.clean_files("batch_in/groq/*.jsonl")
    # BatchInference.clean_files("batch_out/groq/*.json")
