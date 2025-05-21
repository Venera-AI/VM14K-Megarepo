from .base import BaseLoader
from .hugging_face import HuggingFaceLoader


def get_loader_class(config):
    loader_type = config["type"]
    if loader_type == "huggingFace":
        return HuggingFaceLoader
    else:
        raise ValueError(f"loader type {loader_type} not exists")
