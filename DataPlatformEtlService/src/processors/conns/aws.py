import boto3


class AwsStorageConnPool:
    def __init__(self, inputs):
        self.credentials = inputs["credentials"]
        self.inputs = inputs
        self.session = None

    def __get_client__(self):
        client = boto3.Session(
            aws_access_key_id=self.credentials["accessKeyId"],
            aws_secret_access_key=self.credentials["secretAccessKey"],
            # aws_session_token=self.credentials["aws_session_token"],
        )
        return client

    def get(self) -> boto3.Session:
        if self.session is None:
            self.session = self.__get_client__()
        return self.session

    @classmethod
    def put(cls, session):
        return

    @classmethod
    def commit(cls, session):
        pass

    @classmethod
    def rollback(cls, session):
        pass
