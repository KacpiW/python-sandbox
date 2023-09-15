from typing import Optional, Generator
import json

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from end_to_end.config import setup_logger

logger = setup_logger(__name__, 'ete.log')


class Extractor:
    def __init__(self, s3_client: Optional[BaseClient] = None) -> None:
        """
        Initialize the Extractor object and set the S3 client.
        """
        if s3_client:
            self.s3_client = s3_client
        else:
            self.s3_client = boto3.client("s3")
        logger.debug("Extractor initialized.")

    def get_last_processed_file_name(self, bucket: str, key: str) -> Optional[str]:
        """
        Get the name of the last processed file.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            file_name = data.get("StartAfter")
            logger.info(f"Last processed file: {file_name}")
            return file_name
        except ClientError as e:
            logger.error(f"Error while getting the last processed file: {e}")
            return None

    def get_unprocessed_files(self, bucket: str, start_after: Optional[str], prefix: str = "data/") -> Generator:
        """
        Yield unprocessed files from the S3 bucket.
        """
        request_args = {'Bucket': bucket, 'Prefix': prefix}
        if start_after:
            request_args["StartAfter"] = start_after

        while True:
            response = self.s3_client.list_objects_v2(**request_args)
            if 'Contents' in response:
                for file in response["Contents"]:
                    file_obj = self.s3_client.get_object(
                        Bucket=bucket, Key=file["Key"])
                    logger.debug(f"Processing file: {file['Key']}")
                    yield file, file_obj
            if 'NextContinuationToken' not in response:
                break
            request_args['ContinuationToken'] = response['NextContinuationToken']

    def set_last_processed_file_name(self, filename: str, bucket: str, start_after: str) -> None:
        """
        Set the name of the last processed file.
        """
        data = json.dumps({"StartAfter": start_after})
        self.s3_client.put_object(Bucket=bucket, Key=filename, Body=data)
        logger.info(f"Set last processed file to: {filename}")
