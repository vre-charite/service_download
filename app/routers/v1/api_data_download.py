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

import os

from fastapi import APIRouter
from fastapi import Header
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from fastapi_utils import cbv

from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.config import ConfigClass
from app.models.base_models import APIResponse
from app.models.base_models import EAPIResponseCode
from app.models.models_data_download import DownloadStatusListResponse
from app.models.models_data_download import EDataDownloadStatus
from app.models.models_data_download import GetDataDownloadStatusResponse
from app.resources.download_token_manager import verify_download_token
from app.resources.error_handler import ECustomizedError
from app.resources.error_handler import catch_internal
from app.resources.error_handler import customized_error_template
from app.resources.helpers import delete_by_session_id
from app.resources.helpers import get_status
from app.resources.helpers import set_status
from app.resources.helpers import update_file_operation_logs

router = APIRouter()

_API_TAG = 'v1/data-download'
_API_NAMESPACE = 'api_data_download'


@cbv.cbv(router)
class APIDataDownload:
    """API Data Download Class."""

    def __init__(self):
        self.__logger = SrvLoggerFactory('api_data_download').get_logger()

    @router.get(
        '/downloads/status',
        tags=[_API_TAG],
        response_model=DownloadStatusListResponse,
        summary='Fetch download status list by session_id',
    )
    @catch_internal(_API_NAMESPACE)
    async def data_download_status_list(
        self,
        project_code: str,
        operator: str,
        job_id: str = '*',
        session_id: str = Header(None),
    ) -> JSONResponse:
        """Fetch download status list."""

        response = APIResponse()
        jobs_fetched = get_status(session_id, job_id, project_code, 'data_download', operator)
        if len(jobs_fetched) > 0:
            response.code = EAPIResponseCode.success
        else:
            response.code = EAPIResponseCode.not_found
            response.error_msg = 'No record.'
        response.result = jobs_fetched
        response.total = len(jobs_fetched)

        return response.json_response()

    @router.get(
        '/download/status/{hash_code}',
        tags=[_API_TAG],
        response_model=GetDataDownloadStatusResponse,
        summary='Check download status',
    )
    @catch_internal(_API_NAMESPACE)
    async def data_download_status(self, hash_code):
        """Check download status."""

        response = APIResponse()
        # verify hash code
        res_verify_token = verify_download_token(hash_code)
        if not res_verify_token[0]:
            response.code = EAPIResponseCode.unauthorized
            response.result = None
            response.error_msg = res_verify_token[1]
            return response.json_response()
        else:
            res_verify_token = res_verify_token[1]
        session_id = res_verify_token['session_id']
        job_id = res_verify_token['job_id']
        project_code = res_verify_token['project_code']
        operator = res_verify_token['operator']
        job_fatched = get_status(session_id, job_id, project_code, 'data_download', operator)
        found = False
        self.__logger.info('job_fatched list: ' + str(job_fatched))
        self.__logger.info('res_verify_token: ' + str(res_verify_token))

        if len(job_fatched) > 0:
            # find target source
            job_fatched = [job for job in job_fatched if job['source'] == res_verify_token['full_path']]
            if len(job_fatched) > 0:
                found = True
                job_fatched = job_fatched[0]
        self.__logger.info('job_fatched: ' + str(job_fatched))

        if found:
            response.code = EAPIResponseCode.success
            response.result = job_fatched
            return response.json_response()
        else:
            self.__logger.error(f'Status not found {res_verify_token} in namespace {ConfigClass.namespace}')
            response.code = EAPIResponseCode.not_found
            response.result = job_fatched
            response.error_msg = customized_error_template(ECustomizedError.JOB_NOT_FOUND)
            return response.json_response()

    @router.get(
        '/download/{hash_code}',
        tags=[_API_TAG],
        summary='Download the data, asynchronously streams a file as the response.',
    )
    @catch_internal(_API_NAMESPACE)
    async def data_download(self, hash_code: str):
        """If succeed, asynchronously streams a FileResponse."""

        response = APIResponse()
        self.__logger.info(f'Check downloading request: {hash_code}')

        # Verify and decode token
        res_verify_token = verify_download_token(hash_code)
        if not res_verify_token[0]:
            response.code = EAPIResponseCode.unauthorized
            response.result = None
            response.error_msg = res_verify_token[1]
            return response.json_response()
        else:
            res_verify_token = res_verify_token[1]

        # get the temporary file path we saved in token
        # and use it to fetch the actual file
        full_path = res_verify_token['full_path']

        # Use root to generate the path
        if not os.path.exists(full_path):
            self.__logger.error(f'File not found {full_path} in namespace {ConfigClass.namespace}')
            response.code = EAPIResponseCode.not_found
            response.result = None
            response.error_msg = customized_error_template(ECustomizedError.FILE_NOT_FOUND) % full_path
            return response.json_response()

        # this operation is needed since the file will be
        # download to nfs from minio then transfer to user
        filename = os.path.basename(full_path)

        # Add Download Log
        update_file_operation_logs(
            res_verify_token['operator'],
            full_path,
            res_verify_token['project_code'],
        )

        download_job = get_status(
            res_verify_token['session_id'],
            res_verify_token['job_id'],
            res_verify_token['project_code'],
            'data_download',
            res_verify_token['operator'],
        )
        self.__logger.info(f'Length of download job: {len(download_job)}')

        status_update_res = {}

        for record in download_job:
            self.__logger.info(f'download job: {record}')
            self.__logger.info(f'download job type: {type(record)}')

            status_update_res = set_status(
                res_verify_token['session_id'],
                res_verify_token['job_id'],
                record['source'],
                'data_download',
                EDataDownloadStatus.SUCCEED.name,
                res_verify_token['project_code'],
                res_verify_token['operator'],
                res_verify_token['geid'],
                record['payload'],
            )

        self.__logger.debug(status_update_res)

        return FileResponse(path=full_path, filename=filename)

    @router.delete('/download/status', tags=[_API_TAG], summary='Delete the download session status.')
    @catch_internal(_API_NAMESPACE)
    async def clear_status(self, session_id: str = Header(None)):
        """Delete status by session id."""

        __res = APIResponse()
        if not session_id:
            __res.code = EAPIResponseCode.bad_request
            __res.result = {}
            __res.error_msg = 'Invalid Session ID: ' + str(session_id)
            return __res.json_response()
        delete_by_session_id(session_id, action='data_download')
        __res.code = EAPIResponseCode.success
        __res.result = {'message': 'Success'}
        return __res.json_response()
