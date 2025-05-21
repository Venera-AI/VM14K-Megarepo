from typing import List


def literal_object(text, alias=""):
    alias_string = "" if alias == "" else f"[{alias}]."
    return f"{alias_string}[{text}]"


def literal_objects(list_text: List[str], alias=""):
    return [literal_object(text=text, alias=alias) for text in list_text]


class BaseQueryBuilder:
    def __init__(self, table: str):
        self.table = table
        self.full_table_name = f"{literal_object(self.table)}"
