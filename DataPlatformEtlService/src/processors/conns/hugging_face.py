from huggingface_hub import HfApi

from src.utils.logger import LOGGER


class HuggingFaceConnPool:
    def __init__(self, inputs):
        self.inputs = inputs
        self.credentials = inputs["credentials"]
        self.client = None

    def get(self) -> HfApi:
        LOGGER.info(f"[PIPELINE][CONN][HUGGING_FACE] get client {self.credentials}")
        if self.client is None:
            self.client = HfApi(token=self.credentials["accessToken"])
        return self.client

    @classmethod
    def put(cls, session):
        return

    @classmethod
    def commit(cls, session):
        pass

    @classmethod
    def rollback(cls, session):
        pass
