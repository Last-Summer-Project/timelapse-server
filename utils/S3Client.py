import io
import multiprocessing
from pathlib import Path
from typing import Optional

import boto3
from mypy_boto3_s3 import Client
import logging
from dotenv import dotenv_values
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed


class S3Client:
    session = None
    client = None

    def __init__(self):
        env: dict = dotenv_values()
        self.session = boto3.session.Session()
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
        is_result_ok = True

        for key in keys:
            is_result_ok = is_result_ok and self.download_file(key, target_dir)
        return is_result_ok
        # TODO: ThreadPoolExecutor has some error. fix for speed up.
        # with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() // 2) as exe:
        #     futures_to_result = {exe.submit(self.download_file(key, target_dir)): key for key in keys}
        #     retires = {}
        #     for future in as_completed(futures_to_result):
        #         # if exception happened.
        #         if future.exception():
        #             # Try Re-try
        #             data = futures_to_result[future]
        #             future = exe.submit(self.download_file, data)
        #             retires[future] = data
        #             logging.debug(f"Failure, Retrying {data}...")
        #         else:
        #             logging.debug(f"Download complete with status: {future.result()}")
        #
        #     for future in as_completed(retires):
        #         if future.exception():
        #             data = retires[future]
        #             logging.error(f"Failure on retry: {data}")
        #             is_result_ok = False
        #         else:
        #             logging.debug(f"Download(retry) complete with status: {future.result()}")
        #     return is_result_ok
