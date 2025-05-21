import io

from botocore.client import BaseClient


class AwsS3Util:
    @classmethod
    async def bytes_to_buffer(cls, data: bytes):
        buffer = io.BytesIO()
        buffer.write(data)
        buffer.seek(0)
        return buffer

    @classmethod
    async def list_objects_v2(cls, client: BaseClient, Bucket, Prefix=None, ContinuationToken=None):
        kwargs = {"Bucket": Bucket, "MaxKeys": 1000}
        if Prefix:
            kwargs["Prefix"] = Prefix
        if ContinuationToken:
            kwargs["ContinuationToken"] = ContinuationToken
        return client.list_objects_v2(**kwargs)

    @classmethod
    async def download_object(cls, client: BaseClient, bucket_name: str, object_key: str):
        buffer = io.BytesIO()
        client.download_fileobj(Bucket=bucket_name, Key=object_key, Fileobj=buffer)
        buffer.seek(0)
        return buffer
