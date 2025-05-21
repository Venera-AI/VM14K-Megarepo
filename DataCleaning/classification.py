import ollama
import json

import io
import os
import pandas as pd
import difflib
import datetime
import random
from google import genai
import time

# import google.generativeai as genai
import json
import os
import pandas as pd
import difflib
import datetime
import random
from prompts import gemini_prompt
from dotenv import load_dotenv
import json
load_dotenv()
from pydantic import BaseModel
from typing import List, Dict, Optional
from pydantic import BaseModel
from typing import List, Dict
from google.genai.types import GenerateContentConfig
from pydantic import ConfigDict

global category

class Answer(BaseModel):
    model_config = ConfigDict(extra="ignore")
    optionA: str
    optionB: str
    optionC: str
    optionD: str

class MedicalQuestion(BaseModel):
    model_config = ConfigDict(extra="ignore")
    question: str
    answer:Answer
    correctAnswer: str
    medicalTopic: List[str]
    difficultLevel: str
    regularFormat: bool
    questionID: str

class MedicalQuestionList(BaseModel):
    model_config = ConfigDict(extra="ignore")
    batch: List[MedicalQuestion]


class LLM:
    def __init__(self, model):
        if model == "gemini":
            self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
            self.sys_prompt = gemini_prompt

        self.model = model

    def get_llm_responese(self, prompt):
        try:
            
            
            if self.model == "gemini":
                key=os.getenv("GEMINI_API_KEY")
                client = genai.Client(api_key=key)
                response = client.models.generate_content(
                    model='gemini-2.0-flash',
                    contents=f"""
                    INSTRUCTION: {self.sys_prompt}
                    question: {prompt}
                    
                    """,
                    config={
                        'response_mime_type': 'application/json',
                        'response_schema': list[MedicalQuestion],
                    },
                )                
                return {"success": True, "data": response.text}
            elif self.model == "llama":
                response = self.client.chat(
                    model="llama3.2",
                    messages=[{"role": "user", "content": prompt}],
                )
                return response["message"]["content"]
            else:
                raise Exception("Invalid Model")
        except Exception as e:
            print("error in get_llm_responese", e)
            with open("error.log", "a") as f:
                f.write(f"{str(e)}\n")
            return {
                "success": False,
            }

def add_to_json(file, new_item):
   with open(file, 'r+', encoding='utf-8') as f:
    data = f.read()
    
    if not data.strip():
        f.seek(0)  
        f.write('[\n')  
        f.write(json.dumps(new_item, ensure_ascii=False)) 
        f.write('\n')  
        f.write(']')  
    else:
        data = data.strip().rstrip(']') 
        
        if len(data) > 1:
            data += ','

        data += '\n' + json.dumps(new_item, ensure_ascii=False) + '\n]'
        
        f.seek(0)  
        f.write(data)


class Classifier:
   
    def __init__(
        self,
        model,
        csv_files,
        save_dir,
        num_questions_samples=2,
        n_questions_per_request=2,
    ):
        self.columns = [
            "question",
            "optionA",
            "optionB",
            "optionC",
            "optionD",
            "correctAnswer",
            "medicalTopic",
            "difficultyLevel",
            "regularFormat",
            "questionID",
        ]
        self.save_dir = save_dir
        self.csv_files = csv_files
        self.model = LLM(model)
        self.n_questions_per_request = n_questions_per_request
        self.num_questions_samples = num_questions_samples
        processed_files = [f for f in os.listdir(save_dir) if  f.startswith('PIPELINE') and os.path.isfile(os.path.join(dst_folder, f))]
        self.target_iden=[f.split(".")[0] for f in processed_files]
        self.init_files()
    
    def check_finish_proccess(self,src_file, dst_file):
        src_data=pd.read_csv(src_file)
        with open(dst_file, "r") as f:
                dst_data = json.load(f)
        if len(dst_data) / len(src_data) < 0.9:
            return False
        return True
    
    def init_files(self):
        progress_file = os.path.join(self.save_dir, "progress.json")
        if not os.path.exists(progress_file):
            with open(progress_file, "w") as f:
                json.dump({}, f)
        with open(progress_file, "r") as f:
            progress = json.load(f)
        for file_path in self.csv_files:
            src_iden=file_path.split("/")[-1].split(".")[0]
            if src_iden not in progress:
                progress[src_iden]=0
            dst_file=os.path.join(self.save_dir,f"{src_iden}.json")
            if not os.path.exists(dst_file):
                with open(dst_file, "w") as f:
                    f.write("")
        with open(progress_file, "w") as f:
            json.dump(progress, f)
        
        
            
    def run_classification(self):
        progress_file = os.path.join(self.save_dir, "progress.json")
        with open(progress_file, "r") as f: 
            progress = json.load(f)

        for file_path in self.csv_files:
            process_idx=0
            print("processing file", file_path)
            src_iden=file_path.split("/")[-1].split(".")[0]
            dst_file=os.path.join(self.save_dir,f"{src_iden}.json")
            process_idx=progress[src_iden]
            df = pd.read_csv(file_path)

            if self.num_questions_samples == -1:
                sample_idx = range(len(df))
            else:
                sample_idx = [i for i in range(min(self.num_questions_samples, len(df) - 1))]

            data = df.iloc[sample_idx][["question", "answer"]]
            batches = [
                data[i: i + self.n_questions_per_request]
                for i in range(process_idx, len(data), self.n_questions_per_request)
            ]

            save_path = dst_file
            if not os.path.exists(save_path):
                with open(save_path, "w") as f:
                    f.write("")

            n_successive_fail=0
            MAX_SUCCESS_FAIL=3
            MAX_RETRIES = 5
            RETRY_DELAY = 2  
            for i, batch in enumerate(batches):
                
                questions = "\n".join(
                    batch.apply(lambda row: f"{row['question']};{row['answer']}", axis=1)
                )

                for attempt in range(MAX_RETRIES):
                    response = self.model.get_llm_responese(questions)
                   
                    if response.get("success"):
                        try:
                            raw_data = response.get("data", "[]")

                            if isinstance(raw_data, str):
                                try:
                                    json_list = json.loads(raw_data)
                                except json.JSONDecodeError as e:
                                    raise ValueError(f"Error parsing JSON string: {e}")
                            else:
                                json_list = raw_data

                            if not isinstance(json_list, list):
                                raise ValueError(f"Expected a list of JSON objects, got: {type(json_list)}")

                            df = pd.json_normalize(json_list, sep="_")

                            print("Processed batch", i)
                            for _, row in df.iterrows():
                                add_to_json(save_path, row.to_dict())
                            progress[src_iden] +=self.n_questions_per_request
                            with open(progress_file, "w") as f:
                                json.dump(progress, f) 
                            n_successive_fail=0
                            break
                          

                        except Exception as e:
                            n_successive_fail+=1
                            print("Error processing data:", e)
                            if n_successive_fail>=MAX_SUCCESS_FAIL:
                                print("Face continuous failure. Force stop script")
                                return
                            break  
                    else:
                        print(f"Attempt {attempt + 1} failed. Retrying...")
                        time.sleep(RETRY_DELAY)
                else:
                    print("Failed to get a successful response after maximum retries.")
                    n_successive_fail+=1
                    if n_successive_fail>=MAX_SUCCESS_FAIL:
                        print("may be rate limited")
                        progress[src_iden] = process_idx
                        with open(progress_file, "w") as f:
                            json.dump(progress, f)
                        return 

        print(f"Finished processing. Data saved to {self.save_dir}")
        
if __name__ == "__main__":
   
    DATA_DIR = "<>"
    os.makedirs(DATA_DIR, exist_ok=True)
    
    dst_folder = os.environ.get('dst_folder')
    print("dst_folder",dst_folder)
    if not os.path.exists(dst_folder):
        os.makedirs(dst_folder)
    csv_files = [
        os.path.join(DATA_DIR, file)
        for file in os.listdir(DATA_DIR)
        if file.endswith(".csv")
    ]
    idx = list(range(len(csv_files)))  
    idx.remove(2)  
    samples_csvs=[csv_files[i] for i in idx]
    classifier = Classifier(
        model="gemini",
        csv_files=csv_files,
        save_dir=dst_folder,
        num_questions_samples=-1,
        n_questions_per_request=25,
    )
    classifier.run_classification()

