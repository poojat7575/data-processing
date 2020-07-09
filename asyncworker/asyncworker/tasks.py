import os
import re
import heapq
import tempfile
import shutil
import logging

from datetime import datetime
from celery.contrib import rdb

from asyncworker.celery import app
from asyncworker.s3.client import S3Client
from asyncworker.s3.bucket import S3Bucket

logger = logging.getLogger(__name__)

BUCKET_NAME = "logs"
MINIO_URL= "http://localhost:9000"
TEMP_DIR = "/tmp"

@app.task
def merge_s3_files(start_date, end_date):
    """Async task to download files from S3, merge them and upload the result.

    The lines in the merged file should be ordered.
    """
    logging.info(F"Input params are start_date: {start_date} and end_date: {end_date}")
    start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S").date()

    log_bucket = load_log_bucket()
    required_files = get_required_files(log_bucket.get_object_names(),
                                        start_date, end_date)
    if not required_files:
        logging.error("No log file found in given date range. Nothing to process")
        return
    download_files(required_files, log_bucket)
    outfile_key, outfile_path = merge_sort_files(required_files)
    log_bucket.upload_file(outfile_path, outfile_key)
    logging.info("Task has been completed successfully.")


def load_log_bucket():
    """
    Establish connection with S3 and instantiate S3Bucket.
    :return: S3Bucket object
    """
    logging.info(F"Loading S3 bucket: {BUCKET_NAME}")
    bucket = S3Bucket(BUCKET_NAME, S3Client(MINIO_URL).client)
    return bucket


def get_required_files(all_files_in_bucket, start_date, end_date):
    """
    Filters the files in given list of files in bucket.
    The required files are those which are in range of start and end date.
    :param all_files_in_bucket: list of string, each representing file's name
    :param start_date: datetime object, start date
    :param end_date: datetime object, start date
    :return: filtered_files, list of string, each representing file's name
    """
    logging.debug("Files in bucket: %s" % str(all_files_in_bucket))
    regex = re.compile(r'^\d{4}-\d{2}-\d{2}_\d.log')
    filtered_files = [filename
                      for filename in list(filter(regex.search, all_files_in_bucket))
                      if start_date <= datetime.strptime(
                          filename[:-6:], "%Y-%m-%d").date() <= end_date]
    logging.debug("Files required for this task: %s" % str(filtered_files))
    return filtered_files

def download_files(filenames, log_bucket=None):
    import pdb; pdb.set_trace()
    if not log_bucket:
        log_bucket = load_log_bucket()
    if isinstance(filenames, list):
        for filename in filenames:
            log_bucket.download_file(filename, os.path.join(TEMP_DIR, filename))
    else:
        log_bucket.download_file(filenames, os.path.join(TEMP_DIR, filenames))

def outfile_artifacts():
    outfile_key = str(app.current_task.request.id) + ".log"
    outfile_path = os.path.join(TEMP_DIR, outfile_key)
    return outfile_key, outfile_path

def merge_sort_files(filtered_files):
    input_files = list(map(open,
                           [os.path.join(TEMP_DIR, filename)
                            for filename in filtered_files]))
    outfile_key, outfile_path = outfile_artifacts()
    sorted_file = input_files[0]
    temp_files = list()
    for input_file in input_files[1::]:
        temp_file = tempfile.NamedTemporaryFile(mode='w+t', delete=False)
        temp_files.append(temp_file)
        read_file_1, read_file_2 = True, True
        while True:
            if read_file_1:
                text_in_file_1 = sorted_file.readline()
            if read_file_2:
                text_in_file_2 = input_file.readline()
            if text_in_file_1 == "" or text_in_file_2 == "":
                break

            read_file_1, read_file_2 = False, False
            if text_in_file_1 < text_in_file_2:
                text_to_write = text_in_file_1
                read_file_1 = True
            else:
                text_to_write = text_in_file_2
                read_file_2 = True
            temp_file.write(text_to_write)
        while text_in_file_1 != "":
            temp_file.write(text_in_file_1)
            text_in_file_1 = sorted_file.readline()
        while text_in_file_2 != "":
            temp_file.write(text_in_file_2)
            text_in_file_2 = input_file.readline()
        temp_file.seek(0)
        sorted_file = temp_file
    shutil.move(sorted_file.name, outfile_path)
    sorted_file.close()
    [f.close() for f in input_files]
    [f.close() and f.unlink(f.name) for f in temp_files]
    return outfile_key, outfile_path

def merge_sort_files_with_heapq(filtered_files):
    input_files = map(open,
                 [os.path.join(TEMP_DIR, filename)
                 for filename in filtered_files])
    outfile_key, outfile_path = outfile_artifacts()
    outfile = open(outfile_path, "w")
    decorated = [(line for line in input_file)
                 for input_file in input_files]
    for line in heapq.merge(*decorated):
        outfile.writelines(line)
    return outfile_key, outfile_path


def job_status(id):
    res = app.AsyncResult(str(id))
    return res.status
