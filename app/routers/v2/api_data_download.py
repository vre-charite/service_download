# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

from typing import Optional
from typing import Union

import httpx
from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Header
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse
from fastapi_utils import cbv
from sqlalchemy import MetaData
from sqlalchemy import create_engine

from app.commons.download_manager import DownloadClient
from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.commons.service_connection.minio_client import Minio_Client
from app.config import ConfigClass
from app.models.base_models import APIResponse
from app.models.base_models import EAPIResponseCode
from app.models.models_data_download import DatasetPrePOST
from app.models.models_data_download import EDataDownloadStatus
from app.models.models_data_download import PreDataDownloadPOST
from app.models.models_data_download import PreDataDownloadResponse
from app.resources.download_token_manager import verify_dataset_version_token
from app.resources.error_handler import catch_internal
from app.services.approval.client import ApprovalServiceClient

router = APIRouter()

_API_TAG = 'v2/data-download'
_API_NAMESPACE = 'api_data_download'


@cbv.cbv(router)
class APIDataDownload:
    """API Data Download Class."""

    def __init__(self):
        self.__logger = SrvLoggerFactory('api_data_download').get_logger()

    @router.post(
        '/download/pre/',
        tags=[_API_TAG],
        response_model=PreDataDownloadResponse,
        summary='Pre download process, zip as a package if more than 1 file, '
        'used in project files download and dataset single file download.',
    )
    @catch_internal(_API_NAMESPACE)
    async def data_pre_download(
        self,
        data: PreDataDownloadPOST,
        background_tasks: BackgroundTasks,
        authorization: Optional[str] = Header(None),
        refresh_token: Optional[str] = Header(None),
        # settings: Settings = Depends(get_settings),
    ) -> JSONResponse:
        response = APIResponse()
        minio_token = {
            'at': authorization,
            'rt': refresh_token,
        }

        # Determine whether it is project files download or dataset single file download
        if not data.project_code and not data.dataset_geid:
            error_msg = 'project_code or dataset_geid required'
            response.error_msg = error_msg
            response.code = EAPIResponseCode.bad_request
            self.__logger.error(error_msg)
            return response.json_response()
        if data.project_code:
            object_code = data.project_code
            object_geid = ''
            download_type = 'project_files'
        else:
            object_geid = data.dataset_geid
            download_type = 'dataset_files'
            with httpx.Client() as client:
                res = client.get(ConfigClass.NEO4J_SERVICE + 'nodes/geid/' + data.dataset_geid)
                if res.status_code != 200:
                    error_msg = 'Get dataset code error {}: {}'.format(res.status_code, res.text)
                    response.error_msg = error_msg
                    response.code = EAPIResponseCode.internal_error
                    return response.json_response()
            dataset = res.json()
            object_code = dataset[0]['code']

        file_geids_to_include = None
        if data.approval_request_id:
            url = f'postgresql://{ConfigClass.RDS_USER}:{ConfigClass.RDS_PWD}@{ConfigClass.RDS_HOST}/{ConfigClass.RDS_DBNAME}'
            engine = create_engine(url, future=True)
            metadata = MetaData(schema=ConfigClass.RDS_SCHEMA_DEFAULT)
            approval_service_client = ApprovalServiceClient(engine, metadata)
            request_approval_entities = approval_service_client.get_approval_entities(str(data.approval_request_id))
            file_geids_to_include = set(request_approval_entities.keys())
        download_client = DownloadClient(
            data.files,
            minio_token,
            data.operator,
            object_code,
            object_geid,
            data.session_id,
            download_type,
            file_geids_to_include,
        )
        hash_code = download_client.generate_hash_code()
        status_result = download_client.set_status(EDataDownloadStatus.ZIPPING.name, payload={'hash_code': hash_code})
        download_client.logger.info(f'Starting background job for: {data.project_code} {download_client.files_to_zip}')

        # start the background job for the zipping
        background_tasks.add_task(download_client.zip_worker, hash_code)
        response.result = status_result
        response.code = EAPIResponseCode.success
        return response.json_response()

    @router.post('/dataset/download/pre', tags=[_API_TAG], summary='Download all files in a dataset')
    @catch_internal(_API_NAMESPACE)
    async def dataset_pre_download(
        self,
        data: DatasetPrePOST,
        background_tasks: BackgroundTasks,
        authorization: Optional[str] = Header(None),
        refresh_token: Optional[str] = Header(None),
    ) -> JSONResponse:
        api_response = APIResponse()
        self.__logger.info('Called dataset download')

        minio_token = {
            'at': authorization,
            'rt': refresh_token,
        }

        query = {
            'start_label': 'Dataset',
            'end_labels': ['File', 'Folder'],
            'query': {
                'start_params': {
                    'global_entity_id': data.dataset_geid,
                },
                'end_params': {
                    'archived': False,
                },
            },
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(ConfigClass.NEO4J_SERVICE_V2 + 'relations/query', json=query)
            res = await client.get(ConfigClass.NEO4J_SERVICE + 'nodes/geid/' + data.dataset_geid)
            if resp.status_code != 200 or res.status_code != 200:
                error_msg = 'Error when getting node for neo4j'
                api_response.error_msg = error_msg
                api_response.code = EAPIResponseCode.internal_error
                return api_response.json_response()

            nodes = resp.json()['results']
            dataset = res.json()
            dataset_code = dataset[0]['code']

        files = []
        for node in nodes:
            files.append(
                {
                    'geid': node['global_entity_id'],
                }
            )

        download_client = DownloadClient(
            files,
            minio_token,
            data.operator,
            dataset_code,
            data.dataset_geid,
            data.session_id,
            download_type='full_dataset',
        )
        hash_code = download_client.generate_hash_code()
        status_result = download_client.set_status(EDataDownloadStatus.ZIPPING.name, payload={'hash_code': hash_code})
        download_client.logger.info(f'Starting background job for: {dataset_code} {download_client.files_to_zip}')
        background_tasks.add_task(download_client.zip_worker, hash_code)
        download_client.update_activity_log(
            data.dataset_geid,
            data.dataset_geid,
            'DATASET_DOWNLOAD_SUCCEED',
        )
        api_response.result = status_result
        api_response.code = EAPIResponseCode.success
        return api_response.json_response()

    @router.get('/dataset/download/{hash_code}', tags=[_API_TAG], summary='Download dataset version')
    async def download_dataset_version(
        self,
        hash_code: str,
        authorization: Optional[str] = Header(None),
        refresh_token: Optional[str] = Header(None),
    ) -> Union[StreamingResponse, JSONResponse]:
        """Download a specific version of a dataset given a hash_code Please note here, this hash code api is different
        with other async download this one will use the minio client to fetch the file and directly send to frontend.
        and in /dataset/download/pre it will ONLY take the hashcode.

        Other api like project files will use the /pre to download from minio and zip.
        """

        api_response = APIResponse()
        valid, result = verify_dataset_version_token(hash_code)
        if not valid:
            api_response.code = EAPIResponseCode.unauthorized
            api_response.error_msg = result[1]
            return api_response.json_response()

        minio_path = result['location'].split('//')[-1]
        _, bucket, file_path = tuple(minio_path.split('/', 2))
        filename = file_path.split('/')[-1]
        self.__logger.info(str(authorization))
        self.__logger.info(str(refresh_token))

        try:
            mc = Minio_Client()
            result = mc.client.stat_object(bucket, file_path)
            headers = {'Content-Length': str(result.size), 'Content-Disposition': f'attachment; filename={filename}'}
            response = mc.client.get_object(bucket, file_path)
        except Exception as e:
            error_msg = f'Error getting file from minio: {str(e)}'
            self.__logger.error(error_msg)
            api_response.error_msg = error_msg
            return api_response.json_response()
        return StreamingResponse(response.stream(), headers=headers)
