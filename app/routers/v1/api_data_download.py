import os
import time
from fastapi import APIRouter, BackgroundTasks, Header
from fastapi_utils import cbv
from fastapi.responses import FileResponse
from ...models.models_data_download import EDataDownloadStatus, PreDataDowanloadPOST, \
    PreDataDowanloadResponse, GetDataDownloadStatusRespon, DownloadStatusListRespon
from ...models.base_models import APIResponse, EAPIResponseCode
from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...resources.download_token_manager import verify_download_token, generate_token
from ...resources.error_handler import catch_internal, ECustomizedError, customized_error_template
from ...resources.helpers import generate_zipped_file_path, zip_multi_files, \
    set_status, get_status, update_file_operation_logs, delete_by_session_id, get_files_recursive
from ...config import ConfigClass
import requests
import json

router = APIRouter()

_API_TAG = 'v1/data-download'
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
    async def data_pre_download(self, request_payload: PreDataDowanloadPOST, background_tasks: BackgroundTasks):
        '''
        "files":
        [{
            "full_path": "",
            "project_code": "",
            "geid": ""
        }]
        '''
        response = APIResponse()
        request_payload = request_payload.copy()
        files = request_payload.files
        job_id = "data-download-" + str(int(time.time()))
        job_status = EDataDownloadStatus.INIT

        # check if number of files valid
        if len(files) < 1:
            response.code = EAPIResponseCode.bad_request
            response.result = None
            response.error_msg = customized_error_template(
                ECustomizedError.INVALID_FILE_AMOUNT)
            return response.json_response()

        # detect not found files
        not_found = []
        files_to_zip = []
        is_containe_folder = False
        for file in files:
            query = {"global_entity_id": file["geid"]}
            resp = requests.post(ConfigClass.NEO4J_SERVICE +
                                 "nodes/File/query", json=query)
            if not resp.json():
                # Handle Folder
                self.__logger.info(
                    f'Getting folder from geid: ' + str(file['geid']))
                all_files = []
                all_files = get_files_recursive(file["geid"], all_files=[])
                if len(all_files) > 0:
                    is_containe_folder = True
                self.__logger.info(f'Got files from folder: {all_files}')
                for node in all_files:
                    if not os.path.exists(node["full_path"]):
                        not_found.append(node["full_path"])
                    else:
                        files_to_zip.append({
                            "full_path": node["full_path"],
                            "geid": node["global_entity_id"],
                            "project_code": node["project_code"],
                            "operator": node["operator"],
                            "parent_folder": file["geid"],
                        })
            else:
                # Handle File
                file_node = resp.json()[0]
                if not os.path.exists(file_node["full_path"]):
                    not_found.append(file_node["full_path"])
                else:
                    files_to_zip.append({
                        "full_path": file_node["full_path"],
                        "geid": file_node["global_entity_id"],
                        "project_code": file_node["project_code"],
                        "operator": file_node["operator"],
                        "parent_folder": None
                    })
        if len(not_found) > 0:
            response.code = EAPIResponseCode.not_found
            response.result = {
                "not_found": not_found
            }
            response.error_msg = customized_error_template(
                ECustomizedError.FILE_NOT_FOUND) % str(not_found)
            return response.json_response()

        if not files_to_zip:
            response.code = EAPIResponseCode.bad_request
            response.error_msg = "Folder is empty"
            return response.json_response()

        query = {"global_entity_id": file["geid"]}
        resp = requests.post(ConfigClass.NEO4J_SERVICE +
                             "nodes/File/query", json=query)
        if resp.json():
            file_node = resp.json()[0]
            geid = file_node["global_entity_id"]
        else:
            query = {"global_entity_id": file["geid"]}
            resp = requests.post(ConfigClass.NEO4J_SERVICE +
                                 "nodes/Folder/query", json=query)
            geid = resp.json()[0]["global_entity_id"]
        full_path = files_to_zip[0]["full_path"]

        # if multiple files, zip as one file
        if len(files_to_zip) > 1 or is_containe_folder:
            # asyncly zip files
            zipped_file_path = generate_zipped_file_path(
                request_payload.project_code)
            full_path = zipped_file_path
            # update status as zipping
            job_status = EDataDownloadStatus.ZIPPING
        else:
            # update status as ready for download
            job_status = EDataDownloadStatus.READY_FOR_DOWNLOADING
        # generate jwt hash code token
        hash_code = generate_token({
            "geid":  geid,
            "full_path":  full_path,
            "issuer": "SERVICE DATA DOWNLOAD",
            "operator": request_payload.operator,
            "session_id": request_payload.session_id,
            "job_id": job_id,
            "project_code": request_payload.project_code,
            "iat": int(time.time()),
            "exp": int(time.time()) + (ConfigClass.DOWNLOAD_TOKEN_EXPIRE_AT * 60)
        })

        # set status for zip files
        zip_files_geid = []
        for file in files_to_zip:
            zip_files_geid.append(file["full_path"])
            parent_folder = file["parent_folder"]
            if len(files_to_zip) > 1 and not parent_folder:
                parent_folder = 'zip folder'
            job_recorded = set_status(
                request_payload.session_id,
                job_id,
                file["full_path"],
                "data_download",
                job_status.name,
                request_payload.project_code,
                request_payload.operator,
                file["geid"],
                payload={
                    "hash_code": hash_code,
                    "parent_folder": parent_folder
                }
            )

        if len(files_to_zip) > 1 or is_containe_folder:
            self.__logger.info(
                f'Starting background job for: {geid} {files_to_zip}')
            background_tasks.add_task(
                zip_worker, job_id, zipped_file_path, files_to_zip.copy(), request_payload, hash_code, geid, zip_files_geid)

        # set status in session store
        job_recorded = set_status(
            request_payload.session_id,
            job_id,
            full_path,
            "data_download",
            job_status.name,
            request_payload.project_code,
            request_payload.operator,
            geid,
            payload={
                "hash_code": hash_code,
                "files": zip_files_geid
            }
        )

        response.result = job_recorded
        response.code = EAPIResponseCode.success
        return response.json_response()

    @router.get("/downloads/status", tags=[_API_TAG],
                response_model=DownloadStatusListRespon,
                summary="Fetch download status list by session_id")
    @catch_internal(_API_NAMESPACE)
    async def data_download_status_list(self, project_code: str, operator: str,
                                        job_id: str = '*',
                                        session_id: str = Header(None)):
        '''
        Fetch download status list
        '''
        response = APIResponse()
        # verify hash code
        job_fatched = get_status(
            session_id, job_id, project_code, "data_download", operator)
        if len(job_fatched) >= 0:
            response.code = EAPIResponseCode.success
            response.result = job_fatched
            response.total = len(job_fatched)
            return response.json_response()
        else:
            response.code = EAPIResponseCode.not_found
            response.result = job_fatched
            response.total = len(job_fatched)
            response.error_msg = 'No record.'
            return response.json_response()

    @router.get("/download/status/{hash_code}", tags=[_API_TAG],
                response_model=GetDataDownloadStatusRespon,
                summary="Check download status")
    @catch_internal(_API_NAMESPACE)
    async def data_download_status(self, hash_code):
        '''
        Check download status
        '''
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
        job_fatched = get_status(
            session_id, job_id, project_code, "data_download", operator)
        found = False
        self.__logger.info(
            'job_fatched list: ' + str(job_fatched))
        self.__logger.info(
            'res_verify_token: ' + str(res_verify_token))
        if len(job_fatched) > 0:
            # find target source
            job_fatched = [job for job in job_fatched if job['source']
                           == res_verify_token['full_path']]
            if len(job_fatched) > 0:
                found = True
                job_fatched = job_fatched[0]
        self.__logger.info(
            'job_fatched: ' + str(job_fatched))
        if found:
            response.code = EAPIResponseCode.success
            response.result = job_fatched
            return response.json_response()
        else:
            response.code = EAPIResponseCode.not_found
            response.result = job_fatched
            response.error_msg = customized_error_template(
                ECustomizedError.JOB_NOT_FOUND)
            return response.json_response()

    @router.get("/download/{hash_code}", tags=[_API_TAG],
                summary="Download the data, asynchronously streams a file as the response.")
    @catch_internal(_API_NAMESPACE)
    async def data_download(self, hash_code: str):
        '''
        If succeed, asynchronously streams a FileResponse
        '''
        response = APIResponse()
        self.__logger.info(
            'Check downloading request: %s' % hash_code)

        # Verify and decode token
        res_verify_token = verify_download_token(hash_code)
        if not res_verify_token[0]:
            response.code = EAPIResponseCode.unauthorized
            response.result = None
            response.error_msg = res_verify_token[1]
            return response.json_response()
        else:
            res_verify_token = res_verify_token[1]

        full_path = res_verify_token["full_path"]

        # Use root to generate the path
        if not os.path.exists(full_path):
            response.code = EAPIResponseCode.not_found
            response.result = None
            response.error_msg = customized_error_template(
                ECustomizedError.FILE_NOT_FOUND) % full_path
            return response.json_response()

        filename = os.path.basename(full_path)

        # Add Download Log
        update_file_operation_logs(
            "VRE",
            res_verify_token['operator'],
            full_path,
            os.path.getsize(full_path),
            res_verify_token['project_code'],
            "undefined"
        )

        download_job = get_status(res_verify_token["session_id"], res_verify_token["job_id"],
                                  res_verify_token["project_code"], "data_download", res_verify_token["operator"])
        self.__logger.info(f'Length of download job: {len(download_job)}')
        for record in download_job:
            self.__logger.info(f'download job: {record}')
            self.__logger.info(f'download job type: {type(record)}')

            status_update_res = set_status(
                res_verify_token['session_id'],
                res_verify_token['job_id'],
                record['source'],
                "data_download",
                EDataDownloadStatus.SUCCEED.name,
                res_verify_token['project_code'],
                res_verify_token['operator'],
                res_verify_token['geid'],
                record['payload']
            )

        self.__logger.debug(status_update_res)

        return FileResponse(path=full_path, filename=filename)

    @router.delete("/download/status", tags=[_API_TAG],
                   summary="Delete the download session status.")
    @catch_internal(_API_NAMESPACE)
    async def clear_status(self, session_id: str = Header(None)):
        '''
        delete status by session id
        '''
        __res = APIResponse()
        if not session_id:
            __res.code = EAPIResponseCode.bad_request
            __res.result = {}
            __res.error_msg = "Invalid Session ID: " + str(session_id)
            return __res.json_response()
        delete_by_session_id(session_id, action="data_download")
        __res.code = EAPIResponseCode.success
        __res.result = {
            "message": "Success"
        }
        return __res.json_response()


def zip_worker(job_id, zipped_file_path, files, request_payload: PreDataDowanloadPOST, hash_code, geid, zip_files_geid):
    '''
    async zip worker
    '''
    try:
        res = zip_multi_files(zipped_file_path, files.copy(),
                              request_payload.project_code)
        set_status(
            request_payload.session_id,
            job_id,
            zipped_file_path,
            "data_download",
            EDataDownloadStatus.READY_FOR_DOWNLOADING.name,
            request_payload.project_code,
            request_payload.operator,
            geid,
            payload={
                "hash_code": hash_code,
                "files": zip_files_geid
            }
        )
    except Exception as e:
        set_status(
            request_payload.session_id,
            job_id,
            zipped_file_path,
            "data_download",
            EDataDownloadStatus.CANCELLED.name,
            request_payload.project_code,
            request_payload.operator,
            geid,
            payload={
                "hash_code": hash_code,
                "error_msg": str(e)
            }
        )
