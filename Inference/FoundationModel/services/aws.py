import json
from pathlib import Path
from typing import List, Dict
import boto3
from botocore.client import BaseClient
from .abstract import BatchInference

class AWSInference(BatchInference):

    def __init__(self, api_key: str, model_name: str, system_prompt: str, response_format, aws_access_key_id: str, roleArn: str, region: str='ap-northeast-1'):
        """
        response_format: MUST set to None (not available on AWS Bedrock)
        """
        assert response_format is None, f"response_format is not available on AWS Bedrock. response_format={response_format}"
        super().__init__(api_key, model_name, system_prompt, "aws", response_format)
        self.region = region
        self.jobName = f"batchJob-{self.uuid}"
        self.bucket_name = Path('batch-inference-0')
        self.input_prefix = Path('batch_infer_in')
        self.output_prefix = Path('batch_infer_out')

        self.__aws_access_key_id = aws_access_key_id
        self.__aws_secret_access_key = api_key
        self.__roleArn = roleArn
        
        self.inputDataConfig=({
            "s3InputDataConfig": {
                "s3Uri": f"s3://{self.bucket_name}/{self.input_prefix}/{self.batch_input_file}"
            }
        })

        self.outputDataConfig=({
            "s3OutputDataConfig": {
                "s3Uri": f"s3://{self.bucket_name}/{self.output_prefix}/"
            }
        })

        self.__init_client()

    def __init_client(self) -> None:
        self.__aws_session = boto3.Session(
            aws_access_key_id=self.__aws_access_key_id,
            aws_secret_access_key=self.__aws_secret_access_key,
            region_name=self.region
        )
        self.__bedrock: BaseClient = self.__aws_session.client('bedrock')
        self.__s3: BaseClient = self.__aws_session.client('s3')
        
    def prepareData(self, data: List[str|Dict]) -> None:
        """Prepare data following the format of the model.
        """
        output = []
        for i, datum in enumerate(data):
            if isinstance(datum, Dict):
                i = datum.get('id', i)
                datum = datum.get('content', datum)
            body = {
                "anthropic_version": "bedrock-2023-05-31", # https://ap-northeast-1.console.aws.amazon.com/bedrock/home?region=ap-northeast-1#/model-catalog/serverless/anthropic.claude-3-5-sonnet-20240620-v1:0
                "system": self.system_prompt, # https://docs.anthropic.com/en/docs/build-with-claude/prompt-engineering/system-prompts
                "messages": [
                    {"role": "user", "content": datum}
                ],
                "max_tokens": 1024,
            }
            aws_input = {
                "recordId": f"task_{i}",
                "modelInput": body
            }

            output.append(aws_input)
        self.formatted_data = output

    def upload_file_to_server(self) -> None:
        """Upload Your Batch File
        """
        path = self.input_dir / self.batch_input_file
        object_name = self.input_prefix / self.batch_input_file
        try:
            self.__s3.upload_file(str(path), str(self.bucket_name), str(object_name))
            print(f"Successfully uploaded [{path}] to [{self.bucket_name}/{object_name}]")
        except Exception as e:
            print(f"Error uploading {path} to S3: {e}")

    def create_batch_job(self) -> None:
        """Create Your Batch Job
        """
        
        response = self.__bedrock.create_model_invocation_job(
            roleArn=self.__roleArn,
            modelId=self.model_name,
            jobName=self.jobName,
            inputDataConfig=self.inputDataConfig,
            outputDataConfig=self.outputDataConfig
        )
        self.jobArn = response.get('jobArn')
        self.job_id = self.jobArn.split('/')[1]

        BatchInference.writeJSONL(response, self.output_dir / f"batch_result_job_{self.uuid}.json")
        print(f"Creating batch job for file {self.inputDataConfig} on AWS. Created job_id: {self.job_id}")

    def get_batch_job_status(self) -> None:
        """Check Batch Status
        """
        assert getattr(self, 'jobArn', None) is not None, "'jobArn' is required. Please create_batch_job."
        if getattr(self, 'job_id', None) is None:
            self.job_id = self.jobArn.split('/')[1]
        
        response = self.__bedrock.get_model_invocation_job(jobIdentifier=self.jobArn)
        self.job_status = response['status'].lower()

        BatchInference.writeJSONL(response, self.output_dir / f"batch_result_status_{self.uuid}.json")
        print(f"Getting batch job status for job {self.job_id} on AWS") 

    def download_batch_file(self) -> None:
        """Retrieve Batch Results
        """
        assert getattr(self, 'job_id', None) is not None, "'job_id' is required. Please create_batch_job."

        prefix = self.output_prefix / self.job_id
        object_key = prefix / f"{self.batch_input_file}.out"

        response = self.__s3.get_object(Bucket=str(self.bucket_name), Key=str(object_key))
        json_data = response.pop('Body').read().decode('utf-8')

        output_data = []
        try:
            for line in json_data.splitlines():
                data = json.loads(line)
                
                item = {
                    "id": data["modelOutput"]["id"],
                    "custom_id": data["recordId"],
                    "response": {
                        "status_code": response["ResponseMetadata"]["HTTPStatusCode"],
                        "body": {
                            "object": data["modelOutput"]["type"],
                            "model": data["modelOutput"]["model"],
                            "choices": [
                                {"index":0,"message":{"role":"assistant","content": data['modelOutput']['content'][0]['text']},"finish_reason":data['modelOutput']["stop_reason"]}
                            ],
                            "usage": data["modelOutput"]["usage"]
                        }
                    }
                }
                
                output_data.append(item)
            
            output_file = self.output_dir / self.batch_output_file
            BatchInference.writeJSONL(output_data, output_file)
            print(f"Successfully read {len(output_data)} JSON objects from S3.")

            BatchInference.writeJSONL(response, self.output_dir / f"batch_result_download_{self.uuid}.json")
            print(f"Downloading batch file for job {self.job_id} on AWS as {output_file}")
        except Exception as e:
            print(f"Error decoding JSON in line: {e}")
