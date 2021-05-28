import os
import time
from fastapi import APIRouter, BackgroundTasks, Header
from fastapi_utils import cbv
import requests
import datetime

from ...resources.error_handler import catch_internal
from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...config import ConfigClass
from ...commons.service_connection.minio_client import Minio_Client

router = APIRouter()

_API_TAG = 'v2/data-download'
_API_NAMESPACE = "api_data_download"


@cbv.cbv(router)
class APIDataDownload:
    '''
    API Data Download Class
    '''

    def __init__(self):
        self.__logger = SrvLoggerFactory('api_data_download').get_logger()


    @router.get("/download/{bucket}/object/{object_path}", tags=[_API_TAG],
                 summary="Pre download process, zip as a package if more than 1 file")
    @catch_internal(_API_NAMESPACE)
    async def data_pre_download(self, bucket:str, object_path:str):

        self.__logger.info('data_pre_download')
        self.__logger.info(ConfigClass.MINIO_ENDPOINT)

        mc = Minio_Client()

        self.__logger.info('presigned_download_url')
        presigned_download_url = mc.client.presigned_get_object(bucket, object_path, expires=datetime.timedelta(seconds=60))

        if ConfigClass.env != 'dev':
            presigned_download_url = presigned_download_url.split('//', 2)[-1]
            presigned_download_url = 'https://' + presigned_download_url

        return presigned_download_url

    