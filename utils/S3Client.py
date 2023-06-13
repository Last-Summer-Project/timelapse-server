import concurrent
import io
import multiprocessing
from pathlib import Path
from typing import Optional

import boto3
import requests
from mypy_boto3_s3 import Client
import logging
from dotenv import dotenv_values
import os
from concurrent.futures import ThreadPoolExecutor


class S3Client:
    session = None
    client = None

    def __init__(self):
        env: dict = dotenv_values()
        self.session = boto3.session.Session()
        self.endpoint = os.getenv("S3_ENDPOINT_URL")
        self.client: Client = self.session.client(
            's3',
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
            config=boto3.session.Config(signature_version='s3v4'),
            verify=os.getenv("S3_VERIFY_TLS")
        )
        self.bucket = os.getenv("S3_BUCKET_NAME")

    def download_file(self, key: str, target_dir: Path) -> bool:
        download_path = target_dir / key
        logging.debug(f"Download file from s3 '{key}' to '{download_path}'")
        try:
            self.client.download_file(self.bucket, key, str(download_path.absolute()))
        except Exception as e:
            logging.error(f"Unknown error occurred during download image {key} to {download_path} : {e}")
            return False
        return True

    def upload_file(self, key: str, file: Path, bucket: Optional[str] = None) -> bool:
        bucket = self.bucket if bucket is None else bucket
        logging.debug(f"Upload file to s3 with key '{key}' from '{file}'")
        try:
            self.client.upload_file(str(file.absolute()), bucket, key)
        except Exception as e:
            logging.error(f"Unknown error occurred during upload file {key} from {file} : {e}")
            return False
        return True

    def multi_download_file(self, keys: list[str], target_dir: Path):
        logging.debug(f"Parallel download to '{target_dir}'...")

        # TODO: Fix to use boto3... boto3 is not thread-safe.
        def download_file(key):
            url = f"{self.endpoint}/{self.bucket}/{key}"
            retry_attempts = 3  # Number of times to retry the download in case of failure

            while retry_attempts > 0:
                # noinspection PyBroadException
                try:
                    response = requests.get(url, stream=True)
                    response.raise_for_status()  # Raise an exception if the response is not successful
                    with open(key, 'wb') as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                file.write(chunk)
                    logging.debug(f'Downloaded: {url}')
                    return True
                except Exception as e:
                    logging.debug(f'Error downloading {url}. Retrying... ({retry_attempts} attempts left) ({e})')
                    retry_attempts -= 1

            logging.error(f'Failed to download: {url}')
            return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            # Submit the download tasks to the executor
            futures = [executor.submit(download_file, key) for key in keys]

            # Wait for all the download tasks to complete
            concurrent.futures.wait(futures)
