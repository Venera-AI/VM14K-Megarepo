from typing import Dict
from src.dtos import BaseDTO
from src.entities import BaseEntity


BaseDict = Dict | BaseEntity | BaseDTO
