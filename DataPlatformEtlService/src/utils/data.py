import json
from typing import Any, List, TypeVar

T = TypeVar("T", bound=Any)


class DataUtil:
    @staticmethod
    def dumps(records: List[T], json_columns) -> List[T]:
        dumped_records = []
        for record in records:
            dumped_record = record.copy()
            for column in json_columns:
                dumped_record[column] = json.dumps(record[column])
            dumped_records.append(dumped_record)
        return dumped_records

    @staticmethod
    def loads(records: List[T], json_columns) -> List[T]:
        loaded_records = []
        for record in records:
            loaded_record = record.copy()
            for column in json_columns:
                loaded_record[column] = json.loads(record[column])
            loaded_records.append(loaded_record)
        return loaded_records
