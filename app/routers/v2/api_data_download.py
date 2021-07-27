import os
import time
from fastapi import APIRouter, BackgroundTasks, Header
from fastapi_utils import cbv
import requests
import datetime
import uuid
import shutil
import minio
import json

from ...resources.error_handler import catch_internal
from ...models.base_models import APIResponse, EAPIResponseCode
from ...resources.error_handler import catch_internal, ECustomizedError, customized_error_template
from ...resources.helpers import generate_zipped_file_path, zip_multi_files, \
    set_status, get_status, update_file_operation_logs, delete_by_session_id, get_files_recursive, get_geid
from ...resources.download_token_manager import verify_download_token, generate_token
from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...config import ConfigClass
from ...commons.service_connection.minio_client import Minio_Client
from ...models.models_data_download import PreSignedDownload, PreSignedBatchDownload, \
        EDataDownloadStatus, PreDataDownloadPOST, \
        PreDataDowanloadResponse, GetDataDownloadStatusRespon, DownloadStatusListRespon, DatasetPrePOST

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


    def update_activity_log(self, dataset_geid, source_entry, username, event_type):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": event_type,
            "payload": {
                "dataset_geid": dataset_geid,
                "act_geid": get_geid(),
                "operator": username,
                "action": "DOWNLOAD",
                "resource": "Dataset",
                "detail": {
                    "source": source_entry
                }
            },
            "queue": "dataset_actlog",
            "routing_key": "",
            "exchange": {
            "name": "DATASET_ACTS",
            "type": "fanout"
            }
        }
        res = requests.post(url, json=post_json)
        if res.status_code != 200:
            error_msg = 'update_activity_log {}: {}'.format(res.status_code, res.text)
            self.__logger.error(error_msg)
            raise Exception(error_msg)
        return res


    # @router.post("/download/{bucket}/object", tags=[_API_TAG],
    #              summary="Pre download process, zip as a package if more than 1 file")
    # @catch_internal(_API_NAMESPACE)
    # async def data_pre_download_presigned(self, bucket:str, request_payload: PreSignedDownload):

    #     try:
    #         mc = Minio_Client()
    #         # temperary for greenroom
    #         bucket = "gr-" + bucket
    #         presigned_download_url = mc.client.presigned_get_object(bucket, \
    #             request_payload.object_path, expires=datetime.timedelta(seconds=60))
    #     except Exception as e:
    #         self.__logger.error(e)

    #     if ConfigClass.env != 'dev':
    #         presigned_download_url = presigned_download_url.split('//', 2)[-1]
    #         presigned_download_url = 'https://' + presigned_download_url

    #     return presigned_download_url


    # @router.post("/download/pre", tags=[_API_TAG],
    #              summary="Allow user to download a zip with multiple files")
    # @catch_internal(_API_NAMESPACE)
    # async def data_pre_download_presigned(self, bucket:str, request_payload: PreSignedDownload):

    #     try:
    #         mc = Minio_Client()
    #         # temperary for greenroom
    #         bucket = "gr-" + bucket
    #         presigned_download_url = mc.client.presigned_get_object(bucket, \
    #             request_payload.object_path, expires=datetime.timedelta(seconds=60))
    #     except Exception as e:
    #         self.__logger.error(e)

    #     if ConfigClass.env != 'dev':
    #         presigned_download_url = presigned_download_url.split('//', 2)[-1]
    #         presigned_download_url = 'https://' + presigned_download_url

    #     return presigned_download_url


    async def handle_pre_download(self, request_payload, background_tasks, allow_empty=False):
        response = APIResponse()
        files = request_payload.files
        job_id = "data-download-" + str(int(time.time()))
        job_status = EDataDownloadStatus.INIT

        # check if number of files valid
        if len(files) < 1 and not allow_empty:
            response.code = EAPIResponseCode.bad_request
            response.result = None
            response.error_msg = customized_error_template(
                ECustomizedError.INVALID_FILE_AMOUNT)
            return response.json_response()

        # print(request_payload)

        # detect not found files
        not_found = []
        files_to_zip = []
        project_code = ""
        is_containe_folder = False
        if request_payload.dataset_description:
            is_containe_folder = True
        for file in files:
            query = {"global_entity_id": file["geid"]}
            resp = requests.post(ConfigClass.NEO4J_SERVICE +
                                 "nodes/File/query", json=query)
            # Handle Folder  
            if not resp.json():
                self.__logger.info(
                    f'Getting folder from geid: ' + str(file['geid']))
                all_files = []
                self.__logger.info(f'Got files from folder: {all_files}')
                all_files = get_files_recursive(file["geid"], all_files=[])
                # here we need to check if user download a folder
                # even thought the folder has only one file
                if len(all_files) > 0:
                    is_containe_folder = True
                self.__logger.info(
                    f'Got files from folder after filter: {all_files}')

                for node in all_files:
                    self.__logger.info(
                        'file node archived: ' + str(node.get("archived", False)))
                    if node.get("archived", False):
                        self.__logger.info(
                            'file node archived skipped' + str(node))
                        continue

                    files_to_zip.append({
                        "location": node["location"],
                        "geid": node["global_entity_id"],
                        "project_code": node.get("project_code", ""),
                        "operator": node["operator"],
                        "parent_folder": file["geid"],
                        "dataset_code": node.get("dataset_code", ""),
                    })
            # Handle File: 
            else:
                # since we use minio
                # then minio will help us to handle if file exist
                file_node = resp.json()[0]
                files_to_zip.append({
                    "location": file_node["location"],
                    "geid": file_node["global_entity_id"],
                    "project_code": file_node.get("project_code", ""),
                    "operator": file_node["operator"],
                    "parent_folder": None,
                    "dataset_code": file_node.get("dataset_code", ""),
                })

        if not files_to_zip and not allow_empty:
            response.code = EAPIResponseCode.bad_request
            response.error_msg = "Folder is empty"
            return response.json_response()


        location = ""
        geid = ""
        if files_to_zip:
            # since we now will need to download from minio
            # so the single file will have always shows zipping
            location = files_to_zip[0]["location"]
            geid = file["geid"]
        job_status = EDataDownloadStatus.ZIPPING

        status_id = request_payload.project_code
        if not status_id:
            status_id = request_payload.dataset_geid

        if not request_payload.project_code:
            if files_to_zip:
                folder_name = files_to_zip[0].get("dataset_code") 
            else:
                folder_name = request_payload.dataset_geid 
        else:
            folder_name = request_payload.project_code
        # split the logic of single download and batch download
        tmp_folder = ConfigClass.MINIO_TMP_PATH + folder_name + '_' + str(time.time())
        final_file_name = ""
        if len(files_to_zip) > 1 or is_containe_folder:
            final_file_name = tmp_folder + ".zip"
        else:
            minio_path = location.split("//")[-1]
            _, bucket, obj_path = tuple(minio_path.split("/", 2))
            final_file_name = tmp_folder + '/' + obj_path

        # generate jwt hash code token use the first geid in the token
        # make the directory beforehand formulate as /tmp/<hash>_<timestamp>/
        hash_code = generate_token({
            "geid":  geid,
            "full_path": final_file_name,
            "issuer": "SERVICE DATA DOWNLOAD",
            "operator": request_payload.operator,
            "session_id": request_payload.session_id,
            "job_id": job_id,
            "project_code": status_id,
            "iat": int(time.time()),
            "exp": int(time.time()) + (ConfigClass.DOWNLOAD_TOKEN_EXPIRE_AT * 60)
        })


        # if we hav multiple file then we start a worker
        # else if the single file just return the presigned url
        payload={
            "hash_code": hash_code,
        }
        
        self.__logger.info(
            f'Starting background job for: {geid} {files_to_zip}')
        background_tasks.add_task(zip_worker, job_id, location, \
            files_to_zip.copy(), request_payload, hash_code, \
            geid, tmp_folder, is_containe_folder)

        if request_payload.dataset_geid and not request_payload.dataset_description:
            # Dataset file download
            # get list of file names
            filenames = ["/".join(i["location"].split("/")[7:]) for i in files_to_zip]
            self.update_activity_log(
                request_payload.dataset_geid,
                filenames,
                request_payload.operator,
                "DATASET_FILEDOWNLOAD_SUCCEED",
            )

        # set status in session store
        job_recorded = set_status(
            request_payload.session_id,
            job_id,
            final_file_name,
            "data_download",
            job_status.name,
            status_id,
            request_payload.operator,
            geid,
            payload=payload
        )
        response.result = job_recorded
        response.code = EAPIResponseCode.success
        return response.json_response()


    @router.post("/download/pre/", tags=[_API_TAG], response_model=PreDataDowanloadResponse,
                 summary="Pre download process, zip as a package if more than 1 file")
    @catch_internal(_API_NAMESPACE)
    async def data_pre_download(self, request_payload: PreDataDownloadPOST, background_tasks: BackgroundTasks):
        '''
        "files":
        [{
            "display_path": "",
            "project_code": "",
            "geid": ""
        }]
        '''
        response = APIResponse()
        request_payload = request_payload.copy()
        response = await self.handle_pre_download(request_payload, background_tasks)
        return response


    @router.post("/dataset/download/pre", tags=[_API_TAG], summary="Download all files in a dataset")
    @catch_internal(_API_NAMESPACE)
    async def dataset_pre_download(self, data: DatasetPrePOST, background_tasks: BackgroundTasks):
        self.__logger.info('Called dataset download')
        api_response = APIResponse()
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

        download_payload = {
            "files": [],
            "session_id": data.session_id,
            "operator": data.operator,
            "dataset_geid": data.dataset_geid,
            "dataset_description": True,
        }
        for node in nodes:
            download_payload["files"].append({
                "geid": node["global_entity_id"],
            })
        self.__logger.info(f'Calling handle_per_download with payload: {download_payload}')
        request_payload = PreDataDownloadPOST(**download_payload)
        response = await self.handle_pre_download(request_payload, background_tasks, allow_empty=True)
        self.update_activity_log(
            data.dataset_geid,
            data.dataset_geid,
            data.operator,
            "DATASET_DOWNLOAD_SUCCEED",
        )
        return response


    # temperary add a presigned upload url here
    @router.get("/upload/{bucket}/object/{object_path}", tags=[_API_TAG],
                 summary="temperary api here, generate presigned upload url")
    @catch_internal(_API_NAMESPACE)
    async def data_pre_upload(self, bucket:str, object_path:str):

        self.__logger.info('data_pre_upload')

        mc = Minio_Client()
        presigned_upload_url = mc.client.presigned_put_object(bucket, object_path, expires=datetime.timedelta(seconds=60))

        if ConfigClass.env != 'dev':
            presigned_upload_url = presigned_upload_url.split('//', 2)[-1]
            presigned_upload_url = 'https://' + presigned_upload_url

        return presigned_upload_url

    

def zip_worker(job_id, location, files, request_payload: PreDataDownloadPOST, \
    hash_code, geid, tmp_folder, is_containe_folder):
    '''
    async zip worker
    '''
    try:      
        _logger = SrvLoggerFactory('api_data_download').get_logger()
        mc = Minio_Client()

        # download all file to tmp folder
        for obj in files:
            # minio location is minio://http://<end_point>/bucket/user/object_path
            minio_path = obj['location'].split("//")[-1]
            _, bucket, obj_path = tuple(minio_path.split("/", 2))
            try:
                mc.client.fget_object(bucket, obj_path, tmp_folder+"/"+obj_path)
            except minio.error.S3Error as e:
                if e.code == "NoSuchKey":
                    _logger.info("File not found, skipping: " + str(e))
                    print("file not found, skipping")
                    continue
                else:
                    raise e

        if request_payload.dataset_description:
            if not os.path.isdir(tmp_folder):
                os.mkdir(tmp_folder)
                os.mkdir(tmp_folder + "/data")
            payload = {
                "global_entity_id": request_payload.dataset_geid
            }
            response = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/Dataset/query", json=payload)
            dataset_node = response.json()[0]
            description_json = {
                "authors": dataset_node["authors"],
                "collection_method": dataset_node["collection_method"],
                "creator": dataset_node["creator"],
                "description": dataset_node["description"],
                "license": dataset_node["license"],
                "modality": dataset_node["modality"],
                "name": dataset_node["name"],
                "tags": dataset_node["tags"],
                "type": dataset_node["type"],
            }
            with open(tmp_folder + "/" + dataset_node["code"] + "_description.json", 'w') as w:
                w.write(json.dumps(description_json, indent=4))

        # we better to display the file name if there is a single file
        # or if the payload is a folder
        display_filename = tmp_folder
        if len(files) > 1 or is_containe_folder:
            shutil.make_archive(tmp_folder, "zip", tmp_folder)
            display_filename = tmp_folder + '.zip'
        else:
            display_filename = tmp_folder + '/' + obj_path

        status_id = request_payload.project_code
        if not status_id:
            status_id = request_payload.dataset_geid

        set_status(
            request_payload.session_id,
            job_id,
            display_filename,
            "data_download",
            EDataDownloadStatus.READY_FOR_DOWNLOADING.name,
            status_id,
            request_payload.operator,
            geid,
            payload={
                "hash_code": hash_code,
                # "files": presigned_download_url
                "files": files
            }
        )
    except Exception as e:
        print(e)
        status_id = request_payload.project_code
        if not status_id:
            status_id = request_payload.dataset_geid
        set_status(
            request_payload.session_id,
            job_id,
            location,
            "data_download",
            EDataDownloadStatus.CANCELLED.name,
            status_id,
            request_payload.operator,
            geid,
            payload={
                "hash_code": hash_code,
                "error_msg": str(e)
            }
        )
