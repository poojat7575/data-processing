import botocore
import logging
from celery.contrib import rdb

logger = logging.getLogger(__name__)

class S3Bucket:

    def __init__(self, name, client):
        """
        Instantiate S3Bucket for the bucket with given name
        :param name: string, bucket name
        :param client: object of class S3Client
        """
        try:
            self.bucket = client.Bucket(name)
        except self.client.meta.client.exceptions.NoSuchBucket as bucket_error:
            logger.error(bucket_error)
            raise
        except Exception as generic_error:
            logger.error(generic_error)
            raise


    def get_object_names(self):
        """
        Fetches a list of objects in bucket
        :return a list of string, each representing an object name
        """
        object_names =  [object.key for object in self.bucket.objects.all()]
        if not object_names:
            logger.warn("Bucket is empty")
        return object_names


    def download_file(self, object_key, dest_file):
        """
        Downloads the given object file and store it in destination.
        :param object_key: string name of file to be downloaded
        :param dest_file: string path of destination file
        :return: None
        :raises: exception in case of an issue with download
        """
        try:
            self.bucket.download_file(object_key, dest_file)
            logger.info(F"File with key {object_key} has been downloaded successfully. Available at {dest_file}.")
        except botocore.exceptions.ClientError as download_error:
            logger.error(download_error)
            raise

    def upload_file(self, source_file, object_key):
        """
        uploads the given file
        :param source_file: string path of destination file
        :param object_key: string name of file to be uploaded
        :return: None
        :raises: exception in case of an issue with upload
        """
        try:
            self.bucket.upload_file(source_file, object_key)
            logger.info(F"File at {source_file} has been uploaded successfully with key {object_key}.")
        except botocore.exceptions.ClientError as upload_error:
            logger.error(upload_error)
            raise


