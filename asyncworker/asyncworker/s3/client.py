import boto3
import botocore
import os
import logging

logger = logging.getLogger(__name__)


class S3Client:

    def __init__(self, connection_url):
        """
        Establish a connection with AWS S3 service
        :param connection_url: string url of AWS S3 or compatible service
        :raises exception in case of connection issue
        """
        try:
            logging.info(F"Connection URL: {connection_url}")
            self.client = boto3.resource('s3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                endpoint_url = connection_url
            )
        except botocore.exceptions.EndpointConnectionError as connection_error:
            logger.error(connection_error)
            raise
        except botocore.exceptions.NoCredentialsError as credential_error:
            logger.error(credential_error)
            raise
        except botocore.exceptions.CredentialRetrievalError as credential_error:
            logger.error(credential_error)
            raise
        except Exception as generic_error:
            logger.error(generic_error)
            raise
        else:
            logger.info("Connection established successfully!")