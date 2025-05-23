import json
from pathlib import Path
from typing import List, Dict
from groq import Groq
from .abstract import BatchInference

class GroqInference(BatchInference):
    # https://console.groq.com/docs/api-reference
    def __init__(self, api_key: str, model_name: str, system_prompt: str, response_format):
        """
        Args:
            response_format: Response format to use. Either {"type": "json_object"} or None.
        """
        assert (response_format is None) or (response_format == {"type": "json_object"}), f'Groq only support JSON mode: {{"type": "json_object"}} or set to "None". response_format={response_format}'
        super().__init__(api_key, model_name, system_prompt, "groq", response_format)
        self.__client: Groq = None

        self.init_client()

    def init_client(self) -> None:
        self.__client = Groq(api_key=self._api_key)

    def prepareData(self, data: List[str|Dict]) -> None:
        """Prepare data following the format of the model.
        Args:
            data: List of data to prepare.
        """
        output = []
        for i, datum in enumerate(data):
            if isinstance(datum, Dict):
                i = datum.get('id', i)
                datum = datum.get('content', datum)
            groq_input = {
                "custom_id": f"task_{i}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": self.model_name,
                    "messages": [
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": datum}
                    ]
                },
            }

            if self.response_format is not None:
                assert any("json" in message["content"].lower() for message in groq_input["body"]["messages"]), "To use 'response_format', 'messages' must contain the word 'json'"
                groq_input["body"]["response_format"] = self.response_format

            output.append(groq_input)
        self.formatted_data = output

    def upload_file_to_server(self) -> None:
        """Upload Your Batch File
        info: https://console.groq.com/docs/batch
        """
        response = self.__client.files.create(file=open(self.input_dir / self.batch_input_file, "rb"), purpose="batch")
        response = json.loads(response.to_json())
        self.file_id = response['id']
        
        BatchInference.writeJSONL(response, self.output_dir / f"batch_result_upload_{self.uuid}.json")
        print(f"Uploaded file {self.batch_input_file} to Groq. File_id: {self.file_id}")
    
    def create_batch_job(self) -> None:
        """Create Your Batch Job
        """
        assert getattr(self, 'file_id', None) is not None, "'file_id' is required. Please upload_file_to_server."
        response = self.__client.batches.create(
            input_file_id=self.file_id,
            completion_window="24h",
            endpoint="/v1/chat/completions",
        )
        response = json.loads(response.to_json())
        self.job_id = response['id']

        BatchInference.writeJSONL(response, self.output_dir / f"batch_result_job_{self.uuid}.json")
        print(f"Creating batch job for file {self.file_id} on Groq. Created job_id: {self.job_id}")

    def get_batch_job_status(self) -> None:
        """Check Batch Status
        """
        assert getattr(self, 'job_id', None) is not None, "'job_id' is required. Please create_batch_job."
        response = self.__client.batches.retrieve(self.job_id)
        response = json.loads(response.to_json())
        self.output_file_id = response.get('output_file_id', None)
        self.job_status = response['status'].lower()

        BatchInference.writeJSONL(response, self.output_dir / f"batch_result_status_{self.uuid}.json")
        print(f"Getting batch job status for job {self.job_id} on Groq. Output file_id: {self.output_file_id}") 

    def download_batch_file(self) -> None:
        """Retrieve Batch Results
        """
        assert getattr(self, 'output_file_id', None) is not None, "'output_file_id' is required. Please wait for the batch job to complete. You can get it from 'get_batch_job_status()'."
        output_file = self.output_dir / self.batch_output_file

        response = self.__client.files.content(self.output_file_id)
        response.write_to_file(output_file)

        print(f"Downloading batch file for job {self.job_id} on Groq as {output_file}")
