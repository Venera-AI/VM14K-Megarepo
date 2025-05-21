from pydantic import BaseModel
from enum import Enum

class Answer(str, Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    G = "G"

class ResponseSchema(BaseModel):
    answer: Answer