import json
import time
from pathlib import Path
from typing import List, Dict
from google import genai
from google.genai import types
from .abstract import BatchInference

class GeminiInference(BatchInference):
    # https://ai.google.dev/gemini-api/docs
    def __init__(self, api_key: str, model_name: str, system_prompt: str, response_format):
        """
        Args:
            response_format: Response format to use. Either {"type": "json_object"} or None.
        """
        super().__init__(api_key, model_name, system_prompt, "gemini", response_format)
        self.__client = None
        self.batch_size = 20

        self.init_client(model_name)

    def init_client(self, model_name) -> None:
        self.__client = genai.Client(api_key=self._api_key)

    def prepareData(self, data: List[str|Dict]) -> None:
        """Prepare data following the format of the model.
        Args:
            data: List of data to prepare.
        """
        inputs = []
        for i, datum in enumerate(data):
            if isinstance(datum, Dict):
                i = datum.get('id', i)
                datum = datum.get('content', datum)
            
            item = {
                "custom_id": f"task_{i}",
                "body": {
                    "model": self.model_name,
                    "messages": [
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": datum},
                    ]
                }
            }
            
            if self.response_format is not None:
                item["body"]["response_format"] = self.response_format

            inputs.append(item)
        self.formatted_data = inputs

    def upload_file_to_server(self) -> None:
        """Upload Your Batch File
        """
        print(f"No need to upload_file_to_server")
    
    def batch_to_datum(self, id, status_code, batch_id, batch_out):
        outputs = []
        for idx, out in zip(batch_id, batch_out):
            gemini_output = {
                "id": f"{id}",
                "custom_id": f"{idx}",
                "response": {
                    "status_code": status_code,
                    "body": {
                        "model": self.model_name,
                        "choices": [
                            {
                                "message": {
                                    "content": out
                                },
                            },
                        ]
                    }
                }
            }
            outputs.append(gemini_output)
        return outputs
    
    def create_batch_job(self) -> None:
        """Create Your Batch Job
        """
        output = []
        MAX_SUCCESS_FAIL = 3
        MAX_RETRIES = 5
        RETRY_DELAY = 2
        data_size = len(self.formatted_data)
        batches = [
            self.formatted_data[i: i + self.batch_size]
            for i in range(0, data_size, self.batch_size)
        ]
        for i, batch in enumerate(batches):
            print(f"{i}", end='-')
            n_successive_fail=0
            time.sleep(0.1)
            batch_id = [datum['custom_id'] for datum in batch]
            questions = "\n\n".join([datum['body']['messages'][1]['content'] for datum in batch])
            for attempt in range(MAX_RETRIES):
                response = self.get_llm_responese(prompt=questions)

                if response.get("success"):
                    try:
                        raw_data = response.get("data", "[]")
                        if isinstance(raw_data, str):
                            try:
                                json_list = json.loads(raw_data)
                                assert len(json_list) == len(batch_id), f"must match len: len(json_list) ({len(json_list)}) != len(batch_id) ({len(batch_id)})"
                                output.extend(self.batch_to_datum(i, 200, batch_id, json_list))
                                n_successive_fail=0
                                break
                            except json.JSONDecodeError as e:
                                raise ValueError(f"Error parsing JSON string: {e}")
                        else:
                            json_list = raw_data
                        
                        if not isinstance(json_list, list):
                                raise ValueError(f"Expected a list of JSON objects, got: {type(json_list)}")
                    
                    except Exception as e:
                        n_successive_fail += 1
                        print("Error processing data:", e)
                        if n_successive_fail>=MAX_SUCCESS_FAIL:
                            print("Face continuous failure. Continue next batch")
                            output.extend(self.batch_to_datum(i, 400, batch_id, ['']*len(batch_id)))
                            break
                else:
                    print(f"Attempt {attempt + 1} failed. Retrying...")
                    time.sleep(RETRY_DELAY)
            else:
                print("Failed to get a successful response after maximum retries.")
                output.extend(self.batch_to_datum(i, 400, batch_id, ['']*len(batch_id)))
                n_successive_fail+=1
                if n_successive_fail>=MAX_SUCCESS_FAIL:
                    print("may be rate limited")
        print()
        self.output_data = output

    def get_llm_responese(self, prompt: str):
        # https://ai.google.dev/gemini-api/docs/structured-output
        try:
            if "gemini" in self.model_name:
                response = self.__client.models.generate_content(
                    model='gemini-2.0-flash',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=self.system_prompt,
                        response_mime_type='application/json',
                        response_schema=self.response_format,
                    ),
                )                
                return {"success": True, "data": response.text}
            elif self.model == "llama":
                response = self.client.chat(
                    model="llama3.2",
                    messages=[{"role": "user", "content": prompt}],
                )
                return {"success": True, "data": response["message"]["content"]}
            else:
                raise Exception("Invalid Model")
        except Exception as e:
            print("error in get_llm_responese", e)
            with open("error.log", "a") as f:
                f.write(f"{str(e)}\n")
            return {"success": False}

    def get_batch_job_status(self) -> None:
        """Check Batch Status
        """
        self.job_status = 'completed'

    def download_batch_file(self) -> None:
        """Retrieve Batch Results
        """
        output_file = self.output_dir / self.batch_output_file
        BatchInference.writeJSONL(self.output_data, output_file)
        print(f'saved results to {output_file}')
