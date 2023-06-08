from utils.DBClient import DBConn
from utils.S3Client import S3Client
from utils.FFMpeg import make_timelapse
from typing import Tuple, Optional
from time import sleep
from pathlib import Path
import tempfile
import logging
import sys
import uuid
from dotenv import load_dotenv

load_dotenv()


def setup_log():
    log_formatter = logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # global log level

    file_handler = logging.FileHandler("log.txt", mode="a+", encoding="utf-8")
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)


def download_and_upload(s3: S3Client, keys: list[str]) -> Tuple[bool, Optional[str]]:
    with tempfile.TemporaryDirectory() as tmpdir:
        # wrapping with Path
        tmpdir = Path(tmpdir)
        logging.debug("Sa")

        # Download file
        result = s3.multi_download_file(keys, tmpdir)
        if not result:
            return False, None

        result = make_timelapse(tmpdir, keys)
        if not result:
            return False, None

        key = (str(uuid.uuid4()) + ".mp4")
        result = s3.upload_file(key, tmpdir / "out.mp4", bucket="video-bucket")
        return result, key


def main():
    setup_log()

    db = DBConn()
    s3 = S3Client()

    logging.info("Hello, World!")

    while True:
        sec = 30
        for tid in db.get_not_started():
            logging.debug(f"Got not-started Timelapse. ID: {tid}")
            db.update_timelapse(tid, 'in_progress')

            sec = 10
            logging.debug(f"Decreasing wait timer to {sec} as we got update.")

            keys = db.get_image_urls(tid)
            if len(keys) == 0:
                logging.error("Unknown error occurred: image keys are zero. Set to done with error.")
                continue

            ret, url = download_and_upload(s3, keys)
            if not ret:
                logging.error("Unknown error occurred: processing error. Set to done with error")
                db.update_timelapse(tid, 'done', 'error.mp4')
                continue
            db.update_timelapse(tid, 'done', url)

        logging.info(f"Sleeping {sec} sec...")
        sleep(sec)


if __name__ == "__main__":
    sys.exit(main())
