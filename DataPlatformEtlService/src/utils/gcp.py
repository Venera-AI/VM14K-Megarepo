import zipfile
import io
import json
from google.cloud import storage
import re


class GCPStorageUtil:
    @classmethod
    async def upload_dict(cls, client: storage.Client, dict_data, bucket_name, destination_blob_name):
        buffer = await cls.dict_to_zipped_file(dict_data=dict_data)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_file(buffer)
        return

    @classmethod
    async def upload_image(cls, client: storage.Client, image_bytes, bucket_name, destination_blob_name):
        buffer = await cls.bytes_to_image_buffer(image_bytes=image_bytes)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_file(buffer)
        return

    @classmethod
    async def bytes_to_image_buffer(cls, image_bytes):
        # Create a BytesIO object and write the image bytes to it
        buffer = io.BytesIO()
        buffer.write(image_bytes)

        # Set the buffer's position to the beginning
        buffer.seek(0)

        return buffer

    @classmethod
    async def dict_to_zipped_file(cls, dict_data):
        """Convert a dictionary to a gzipped file object."""
        # Convert the dictionary to a JSON string
        json_data = json.dumps(dict_data)

        # Create an in-memory bytes buffer
        buffer = io.BytesIO()

        # Compress the JSON data and write it to the buffer
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            # Write the JSON data to a file inside the ZIP archive
            zip_file.writestr("data.json", json_data)

        # Move the buffer's position to the beginning
        buffer.seek(0)

        return buffer

    @classmethod
    async def pull_zip_to_json(cls, client: storage.Client, bucket_name, blob_name, select_regex_or_list):
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        contents = blob.download_as_bytes()
        buffer = io.BytesIO(contents)
        select_func = (
            re.compile(select_regex_or_list).match
            if isinstance(select_regex_or_list, str)
            else lambda x: (x in select_regex_or_list)
        )
        with zipfile.ZipFile(buffer, "r") as zip_file:
            # Dictionary to hold the extracted file contents
            extracted_files = {}
            # Extract each file into memory
            for f in zip_file.namelist():
                if select_func(f):
                    with zip_file.open(f) as extracted_file:
                        extracted_files[f] = json.loads(extracted_file.read())
        return blob, extracted_files
