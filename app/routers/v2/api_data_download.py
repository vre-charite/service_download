from fastapi import APIRouter, BackgroundTasks, Header
from fastapi.responses import StreamingResponse
from fastapi_utils import cbv
from typing import Optional
import requests

from ...models.base_models import APIResponse, EAPIResponseCode
from ...resources.error_handler import catch_internal
from ...resources.download_token_manager import verify_dataset_version_token
from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...config import ConfigClass
from ...commons.service_connection.minio_client import Minio_Client, Minio_Client_
from ...models.models_data_download import EDataDownloadStatus, PreDataDownloadPOST, \
        PreDataDowanloadResponse, DatasetPrePOST
from app.commons.download_manager import DownloadClient

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

    @router.post("/download/pre/", tags=[_API_TAG], response_model=PreDataDowanloadResponse,
                 summary="Pre download process, zip as a package if more than 1 file")
    @catch_internal(_API_NAMESPACE)
    async def data_pre_download(self, data: PreDataDownloadPOST, background_tasks: BackgroundTasks,
            Authorization: Optional[str] = Header(None), refresh_token: Optional[str] = Header(None)):
        response = APIResponse()
        # pass the access and refresh token to minio operation
        token = {
            "at": Authorization,
            "rt": refresh_token
        }
        if data.project_code:
            status_code = data.project_code
            download_type = "project_files"
        else:
            status_code = data.dataset_geid
            download_type = "dataset_files"

        if not status_code:
            error_msg = "project_code or dataset_geid required"
            response.error_msg = error_msg
            response.code = EAPIResponseCode.bad_request
            self.__logger(error_msg)
            return response.json_response()

        files = data.files
        download_client = DownloadClient(
            files,
            token,
            data.operator,
            status_code,
            data.session_id,
            download_type=download_type
        )
        hash_code = download_client.generate_hash_code()
        status_result = download_client.set_status(EDataDownloadStatus.ZIPPING.name, payload={"hash_code": hash_code})
        download_client.logger.info(f'Starting background job for: {data.project_code} {download_client.files_to_zip}')
        background_tasks.add_task(download_client.zip_worker, hash_code)
        response.result = status_result
        response.code = EAPIResponseCode.success
        return response.json_response()

    @router.post("/dataset/download/pre", tags=[_API_TAG], summary="Download all files in a dataset")
    @catch_internal(_API_NAMESPACE)
    async def dataset_pre_download(self, data: DatasetPrePOST, background_tasks: BackgroundTasks,
            Authorization: Optional[str] = Header(None), refresh_token: Optional[str] = Header(None)):
        api_response = APIResponse()
        self.__logger.info('Called dataset download')

        # pass the access and refresh token to minio operation
        token = {
            "at": Authorization,
            "rt": refresh_token
        }
        query = {
            "start_label": "Dataset",
            "end_labels": ["File", "Folder"],
            "query": {
                "start_params": {
                    "global_entity_id": data.dataset_geid,
                },
                "end_params": {
                    "archived": False,
                }
            }
        }
        resp = requests.post(ConfigClass.NEO4J_SERVICE_V2 + "relations/query", json=query)
        nodes = resp.json()["results"]

        files = []
        for node in nodes:
            files.append({
                "geid": node["global_entity_id"],
            })

        download_client = DownloadClient(
            files,
            token,
            data.operator,
            data.dataset_geid,
            data.session_id,
            download_type="full_dataset"
        )
        hash_code = download_client.generate_hash_code()
        status_result = download_client.set_status(EDataDownloadStatus.ZIPPING.name, payload={"hash_code": hash_code})
        download_client.logger.info(f'Starting background job for: {data.dataset_geid} {download_client.files_to_zip}')
        background_tasks.add_task(download_client.zip_worker, hash_code)
        download_client.update_activity_log(
            data.dataset_geid,
            data.dataset_geid,
            "DATASET_DOWNLOAD_SUCCEED",
        )
        api_response.result = status_result
        api_response.code = EAPIResponseCode.success
        return api_response.json_response()

    @router.get("/dataset/download/{hash_code}", tags=[_API_TAG], summary="Download dataset version")
    async def download_dataset_version(self, hash_code: str,
        Authorization: Optional[str] = Header(None), refresh_token: Optional[str] = Header(None)):

        """
            Download a specific version of a dataset given a hash_code
            Please note here, this hash code api is different with other async download
            this one will use the minio client to fetch the file and directly
            send to frontend. and in /dataset/download/pre it will ONLY take the hashcode

            other api like project files will use the /pre to download from minio and zip
        """
        api_response = APIResponse()
        valid, result = verify_dataset_version_token(hash_code)
        if not valid:
            api_response.code = EAPIResponseCode.unauthorized
            api_response.error_msg = result[1]
            return api_response.json_response()

        minio_path = result["location"].split("//")[-1]
        _, bucket, file_path = tuple(minio_path.split("/", 2))
        filename = file_path.split("/")[-1]
        self.__logger.info(str(Authorization))
        self.__logger.info(str(refresh_token))

        try:
            # mc = Minio_Client_(Authorization, refresh_token)
            mc = Minio_Client()
            result = mc.client.stat_object(bucket, file_path)
            headers = {
                "Content-Length": str(result.size),
                "Content-Disposition": f"attachment; filename={filename}"
            }
            response = mc.client.get_object(bucket, file_path)
        except Exception as e:
            error_msg = f"Error getting file from minio: {str(e)}"
            self.__logger.error(error_msg)
            api_response.error_msg = error_msg
            return api_response.json_response()
        return StreamingResponse(response.stream(), headers=headers)
