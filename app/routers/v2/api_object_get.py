import os
import time
from fastapi import APIRouter, Header
from fastapi.responses import StreamingResponse, FileResponse
from fastapi_utils import cbv
import requests
import shutil
import minio

from ...resources.error_handler import catch_internal
from ...models.base_models import APIResponse, EAPIResponseCode
from ...resources.error_handler import catch_internal
from ...resources.helpers import  get_files_recursive
from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...config import ConfigClass
from ...commons.service_connection.minio_client import Minio_Client_


router = APIRouter()

_API_TAG = 'v2/data-download'
_API_NAMESPACE = "api_data_download"


@cbv.cbv(router)
class APIObjectGet:
    '''
    API Object Get Class
    '''

    def __init__(self):
        self.__logger = SrvLoggerFactory('api_data_download').get_logger()


    @router.get("/object/{obj_geid}", tags=[_API_TAG],
                 summary="zip as a package if more than 1 file")
    @catch_internal(_API_NAMESPACE)
    async def get_object(self, obj_geid,
            Authorization: str = Header(None),
            refresh_token: str = Header(None)):
        '''
        Get Object API
        '''
        # pass the access and refresh token to minio operation
        auth_token = {
            "at": Authorization,
            "rt": refresh_token
        }
        zip_list = []
        query = {"global_entity_id": obj_geid}
        entity_type = "File"
        resp = requests.post(ConfigClass.NEO4J_SERVICE +
                                "nodes/File/query", json=query)
        # if not resp, consider it as a Folder
        json_respon = resp.json()
        if not json_respon:
            entity_type = "Folder"
        # handle file stream
        if entity_type == "Folder":
            zip_list = pack_zip_list(self.__logger, obj_geid)
            return folder_stream(self.__logger, zip_list, obj_geid, auth_token)
        elif entity_type == "File":
            file_node = json_respon[0]
            return file_stream(self.__logger, file_node, auth_token)

        invalid_entity_resp = APIResponse()
        invalid_entity_resp.code = EAPIResponseCode.bad_request
        invalid_entity_resp.error_msg = "Invalid entity type"
        return invalid_entity_resp.json_response()

def file_stream(__logger, file_node, auth_token):
    try:
        location = file_node["location"]
        minio_path = location.split("//")[-1]
        _, bucket, file_path = tuple(minio_path.split("/", 2))
        filename = file_path.split("/")[-1]
        mc = Minio_Client_(auth_token["at"], auth_token["rt"])
        result = mc.client.stat_object(bucket, file_path)
        headers = {
            "Content-Length": str(result.size),
            "Content-Disposition": f"attachment; filename={filename}"
        }
        response = mc.client.get_object(bucket, file_path)
    except Exception as e:
        api_response = APIResponse()
        error_msg = f"Error getting file from minio: {str(e)}"
        __logger.error(error_msg)
        api_response.error_msg = error_msg
        return api_response.json_response()
    return StreamingResponse(response.stream(), headers=headers)

def folder_stream(__logger, zip_list, folder_name, auth_token):
    tmp_folder = ConfigClass.MINIO_TMP_PATH + folder_name + '_' + str(time.time())
    zip_name = folder_name + ".zip"
    zipped_path = zip_worker(__logger, zip_list, tmp_folder, auth_token)
    return FileResponse(path=zipped_path, filename=zip_name)

def pack_zip_list(__logger, obj_geid):
    cache = []
    __logger.info(
            f'Getting folder from geid: ' + str(obj_geid))
    all_files = []
    __logger.info(f'Got files from folder: {all_files}')
    all_files = get_files_recursive(obj_geid, all_files=[])
    # here we need to check if user download a folder
    # even thought the folder has only one file
    if len(all_files) > 0:
        is_containe_folder = True
    __logger.info(
        f'Got files from folder after filter: {all_files}')
    for node in all_files:
        __logger.info(
            'file node archived: ' + str(node.get("archived", False)))
        if node.get("archived", False):
            __logger.info(
                'file node archived skipped' + str(node))
            continue
        cache.append({
            "location": node["location"],
            "geid": node["global_entity_id"],
            "project_code": node.get("project_code", ""),
            "parent_folder": obj_geid
        })
    return cache

def zip_worker(_logger, zip_list, tmp_folder, auth_token):
    '''
    async zip worker
    '''
    try:
        mc = Minio_Client_(auth_token["at"], auth_token["rt"])
        # download all file to tmp folder
        for obj in zip_list:
            # minio location is minio://http://<end_point>/bucket/user/object_path
            minio_path = obj['location'].split("//")[-1]
            _, bucket, obj_path = tuple(minio_path.split("/", 2))
            try:
                mc.client.fget_object(bucket, obj_path, tmp_folder + "/"+obj_path)
            except minio.error.S3Error as e:
                if e.code == "NoSuchKey":
                    _logger.info("File not found, skipping: " + str(e))
                    continue
                else:
                    raise e
        shutil.make_archive(tmp_folder, "zip", tmp_folder)
        disk_full_path = tmp_folder + '.zip'
        return disk_full_path
    except Exception as e:
        raise
