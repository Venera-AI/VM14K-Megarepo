from google.cloud.storage import Client


class GcpStorageConnPool:
    def __init__(self, inputs):
        self.inputs = inputs
        self.credentials = self.inputs["credentials"]
        self.client = None

    def get(self) -> Client:
        if self.client is None:
            self.client = Client.from_service_account_info(self.credentials)
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
