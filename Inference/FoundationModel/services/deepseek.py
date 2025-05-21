import json
import time
from pathlib import Path
from typing import List, Dict
from google import genai
from google.genai import types
from .abstract import BatchInference
from openai import OpenAI
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from concurrent.futures import ProcessPoolExecutor
from time import sleep
from functools import lru_cache

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("api_calls.log"), logging.StreamHandler()],
)


class DeepseekInference(BatchInference):
    def __init__(
        self, api_key: str, model_name: str, system_prompt: str, response_format,k=1
    ):
        """
        Args:
            response_format: Response format to use. Either {"type": "json_object"} or None.
        """
        super().__init__(api_key, model_name, system_prompt, service=model_name, response_format=response_format,k=k)
        self.__client = None
        self.batch_size = 20
        self._api_key = api_key
        self.init_client(model_name)
        self.model = model_name

    def init_client(self, model_name) -> None:
        self._client = OpenAI(
            api_key=self._api_key, base_url="https://api.deepseek.com"
        )

    def exec_task(self, data) -> None:

        batch_questions = data["batch_questions"]

        MAX_SUCCESS_FAIL = 3
        MAX_RETRIES = 3
        try:
            n_successive_fail = 0
            questions = "\n\n".join(
                [q["content"] for q in batch_questions]
            )
            for attempt in range(MAX_RETRIES):

                response = self.get_llm_responese(prompt=questions)

                if response.get("success"):
                    try:
                        raw_data = response.get("data", "[]")
                        if isinstance(raw_data, str):
                            try:
                                raw_data = (
                                    raw_data.replace("```json", "")
                                    .replace("```", "")
                                    .strip()
                                )
                                json_list = json.loads(raw_data)
                                n_successive_fail = 0
                            except json.JSONDecodeError as e:
                                raise ValueError(f"Error parsing JSON string: {e}")
                        else:
                            json_list = raw_data
                        return {"success": True, "data": json_list}

                    except Exception as e:
                        n_successive_fail += 1
                        time.sleep(0.1)
                        logging.error(
                            f"Continuous failure . Error: {str(e)}"
                        )
                        if n_successive_fail >= MAX_SUCCESS_FAIL:
                            return {"success": False, "error": str(e)}
        except Exception as e:
            logging.error(f"Failed to get data': {str(e)}")
            return {"success": False, "error": str(e)}

    def prepareData(self, data: List[str | Dict]) -> None:
        if type(data) == dict:
            data = [data]
        self.formatted_data = data
    
    def upload_file_to_server(self) -> None:
        """Upload Your Batch File"""
        print(f"No need to upload_file_to_server")

    def batch_to_datum(self, id, status_code, batch_id, batch_out):
       pass
    def run_batch(self, data_formatted:list):
        print("Begin processing batches...")
        # MAX_WORKERS = 5
        self.save_data(data=data_formatted)
        output_paths=[os.path.join("batch_out",self.service,f"{self.uuid}_epoch_{i}.jsonl") for i in range(self.k)]
        for p in output_paths:
            self.create_batch_job(p) 
            
        # with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        #         futures = {executor.submit(self.create_batch_job, p): p for p in output_paths}
        #         for future in as_completed(futures):
        #             path = futures[future]
        #             try:
        #                 result = future.result()  # This blocks until task completes
        #                 logging.info(f"Completed {path}: {result}")
        #             except Exception as e:
        #                 logging.error(f"Task {path} crashed: {e}")
        print("Finished processing all batches.")

    def create_batch_job(self,output_path:str="") -> None:
        """Create Your Batch Job"""
        data_size = len(self.formatted_data)
        if not os.path.exists(output_path):
            with open(output_path, "w") as f:
                pass
        batches = []
        for i in range(0, data_size, self.batch_size):
            batches.append(
                {
                    "batch_questions": self.formatted_data[i : i + self.batch_size],
                }
            )
        results = []
        MAX_THREADS = 20
        REQUEST_DELAY = 1
        max_counter = 10
        save_counter = 10
        number_processed = 0
        try:
            with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                futures = {executor.submit(self.exec_task, batch): batch for batch in batches}
                
                for future in as_completed(futures):
                    batch = futures[future]
                    
                    try:
                        result = future.result()
                        q_ids = [q["id"] for q in batch["batch_questions"]]
                        results.extend(
                            {"qid": q, "answer": r["answer"]} 
                            for q, r in zip(q_ids, result["data"]["answers"])
                        )
                        
                        number_processed += 1
                        print(f"Processed {number_processed}/{len(batches)} batches")
                        
                        save_counter -= 1
                        if save_counter <= 0:
                            with open(output_path, "a") as f:
                                f.writelines(json.dumps(r) + "\n" for r in results)
                            results.clear()
                            save_counter = max_counter
                            
                    except KeyboardInterrupt:
                        print("\nCancelling...")
                        for f in futures: f.cancel()
                        break
                        
                    except Exception as e:
                        logging.error(f"Error: {e}")
                        sleep(1)
                        futures[executor.submit(self.exec_task, batch)] = batch
                        
        finally:
            print("save path", output_path)
            if results:  # Save any remaining results
                with open(output_path, "a") as f:
                    f.writelines(json.dumps(r) + "\n" for r in results)
            
   

    # @lru_cache(maxsize=1000)
    def get_llm_responese(self, prompt: str):
        # https://ai.google.dev/gemini-api/docs/structured-output
        try:
            if "gemini" in self.model_name:
                response = self.__client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=self.system_prompt,
                        response_mime_type="application/json",
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
            elif self.model == "deepseek-chat":
                response = self._client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {
                            "role": "system",
                            "content": self.system_prompt,
                        },
                        {"role": "user", "content": prompt},
                    ],
                    stream=False,
                )
                return {"success": True, "data": response.choices[0].message.content}
            elif self.model=="deepseek-reasoner":
                response = self._client.chat.completions.create(
                    model="deepseek-reasoner",
                    messages=[
                        {
                            "role": "system",
                            "content": self.system_prompt,
                        },
                        {"role": "user", "content": prompt},
                    ],
                    max_tokens=6400,
                    stream=False,
                )
                logging.info(f"Deepseek Reasoner response: {response.choices[0].message.content}")
                return {"success": True, "data": response.choices[0].message.content}
            
            else:

                raise Exception("Invalid Model")
        except Exception as e:
            print("error in get_llm_responese", e)
            with open("error.log", "a") as f:
                f.write(f"{str(e)}\n")
            return {"success": False}

    def get_batch_job_status(self) -> None:
        """Check Batch Status"""
        self.job_status = "completed"

    def download_batch_file(self) -> None:
        """Retrieve Batch Results"""
        self.output_path = self.output_dir / self.batch_self.output_path
        BatchInference.writeJSONL(self.output_data, self.output_path)
        print(f"saved results to {self.output_path}")
