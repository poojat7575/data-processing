import logging
import colander
import os

from pyramid.view import view_config
from pyramid.response import FileResponse

from asyncworker.tasks import merge_s3_files, job_status, download_files, TEMP_DIR
from .schema import schema_validator

logger = logging.getLogger(__name__)


@view_config(route_name="initiate", renderer="json", request_method="POST")
def initiate(request):
    """This function is called when a POST request is made to /initiate.

   POST input:
    {
        "start_date": "<date in ISO8601 format>",
        "end_date": "<date in ISO8601 format>"
    }
    For example:
    {
        "start_date": "2019-08-18",
        "end_date": "2019-08-25"
    }

    The function should initiate the merging of files on S3 with names between
    the given dates. The actual merging should be offloaded to the async
    executor service.

    The return data is a download ID that the /download endpoint digests:
    {
        "download_id": "<id>"
    }
    For example:
    {
        "download_id": "b0952099-3536-4ea0-a613-98509f4087cd"
    }
    """
    logger.info("Initiate called with payload: %s" % str(request.json_body))
    try:
        request_params = schema_validator(request.json_body)
    except colander.Invalid as err:
        logger.error("Bad Request: %s" % str(err))
        request.response.status_code = 400
        return dict(code=400,
                    status='error',
                    message=str(err),
                    recommendation="Expects a valid date in YYYY-MM-DD format.")

    logger.info("Passing information ahead for asynchronous processing...")
    task_result = merge_s3_files.delay(request_params['start_date'],
                                       request_params['end_date'])
    logger.info("Task id: %s" % task_result.id)
    return {"download_id": task_result.id}


@view_config(route_name="download", renderer="json")
def download(request):
    """This function is called when a GET request is made to /download.

    It accepts the download ID as a URL
    parameter and returns the merged file for download if the merging is done.
    If the merging is not done yet, the appropriate HTTP code is returned, so
    the calling client can continue polling.
    """

    download_id = request.matchdict["download_id"]
    logger.info("Download called with id: %s" % download_id)
    if job_status(download_id) == "SUCCESS":
        logger.info("Task status: SUCCESS!")
        file_name = download_id + ".log"
        download_files(file_name)
        response = FileResponse(
            os.path.join(TEMP_DIR, file_name),
            request=request
        )
        return response
    else:
        logger.warn("Task is still in progress.")
        request.response.status_code = 202
        return dict(code=202,
                    status='In-Progress',
                    message="Merging and uploading is still in progress. Please check again later.")
