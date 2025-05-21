import json
from csv import DictReader
from pathlib import Path
from typing import List, Literal, Optional, Dict
from abc import ABC, abstractmethod
import uuid
from datetime import datetime
import time
import os

class BatchInference(ABC):
    
    def __init__(self, api_key: str, model_name: str, system_prompt: str, service: str, response_format,k=1):
        self._api_key: str = api_key
        self.service = service
        self.model_name: str = model_name
        self.system_prompt: str = system_prompt
        self.response_format = response_format
        self.uuid: str = str(uuid.uuid4())
        self.k: int = k
        self.input_dir: Path = Path("batch_in") / self.service
        self.output_dir: Path = Path("batch_out") / self.service
        self.batch_input_file: str = f"batch_input_{self.uuid}.jsonl"
        self.batch_output_file: str = f"batch_output_{self.uuid}.jsonl"

        self.formatted_data: List = None
        self.job_status: str = None

        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Object initialized with uuid: {self.uuid}")

    @abstractmethod
    def prepareData(self, data: List, **kwargs) -> None:
        """Prepare data following the format of the model."""
        pass
    
    def save_data(self, data: List, **kwargs) -> None:
        """Save the prepared data to a file."""
        self.prepareData(data, **kwargs)
        BatchInference.writeJSONL(self.formatted_data, self.input_dir / self.batch_input_file)

    @abstractmethod
    def upload_file_to_server(self) -> None:
        """Upload file to the server."""
        pass

    @abstractmethod
    def create_batch_job(self) -> None:
        """Create a batch job."""
        pass

    @abstractmethod
    def get_batch_job_status(self) -> None:
        """Get the status of a batch job."""
        pass

    @abstractmethod
    def download_batch_file(self) -> None:
        """Download the result of a batch job."""
        pass
    
    def run_batch(self, data_formatted: List):
        self.save_data(data=data_formatted)
        
        self.upload_file_to_server()

        time.sleep(1)
        self.create_batch_job()
        
        while self.job_status not in ["completed", "failed", "cancelled", "canceled"]:
            time.sleep(30)
            self.get_batch_job_status()
        
        time.sleep(1)
        self.download_batch_file()

    @classmethod
    def writeJSONL(cls, data: List, filename: Path|str) -> None:
        """Write data to a JSONL file."""
        if isinstance(filename, str): filename = Path(filename)
        assert filename.suffix == ".jsonl" or filename.suffix == ".json", f"Must be '*.jsonl' OR '*.json' file. filename={filename}"
        with open(filename, 'w', encoding='utf-8') as f:
            try:
                if filename.suffix == ".jsonl":
                    for item in data:
                        f.write(json.dumps(item, ensure_ascii=False, cls=DateTimeEncoder) + '\n')
                else:
                    json.dump(data, f, ensure_ascii=False, indent=4, cls=DateTimeEncoder)
            except json.JSONDecodeError as e:
                raise ValueError(f"Error decoding JSON in line: {e}")

    @classmethod
    def readJSONL(cls, filename: Path|str) -> List:
        """Read data from a JSONL file."""
        if isinstance(filename, str): filename = Path(filename)
        assert filename.is_file(), f"File does not exist OR is not a file. filename={filename}"
        assert filename.suffix == ".jsonl" or filename.suffix == ".json", f"Must be '*.jsonl' OR '*.json' file. filename={filename}"
        results: List = []
        with open(filename, 'r', encoding='utf-8') as f:
            try:
                if filename.suffix == ".jsonl":
                    results = [json.loads(line) for line in f if line.strip()]
                else:
                    results = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Error decoding JSON in line: {e}")
        return results

    @classmethod
    def readCSV(cls, filename: Path) -> List:
        with open(filename) as csv_file:
            reader = DictReader(csv_file)
            rows = list(reader)
            return rows

    @classmethod
    def clean_files(cls, filepattern: str) -> None:
        path = Path(filepattern)
        directory = path.parent if path.parent != Path('') else Path('.')
        pattern = path.name
        for file_path in directory.glob(pattern):
            file_path.unlink()

    @classmethod
    def format_option(cls, json_str) -> str:
        if isinstance(json_str, str):
            tmp = json.loads(json_str)
        else:
            tmp = json_str
        result = f"""{tmp["question"]}
Chose the correct option from these answers:
"""
        for key, value in tmp.items():
            if key.startswith("option") and value.strip():
                result += f"{key[6:7]}. {value}\n"
        
        return result + """

Response in valid json that match this structure:
{
    "answer": string
}

Example:
{
    "answer": "A"
}"""

class DateTimeEncoder(json.JSONEncoder):
    """For AWS status
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)