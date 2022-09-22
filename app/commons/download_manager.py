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

import json
import os
import shutil
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

import httpx
import minio

from app.commons.locks import recursive_lock
from app.commons.locks import unlock_resource
from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.commons.service_connection.minio_client import Minio_Client_
from app.config import ConfigClass
from app.models.base_models import EAPIResponseCode
from app.models.models_data_download import EDataDownloadStatus
from app.resources.download_token_manager import generate_token
from app.resources.error_handler import APIException

# from app.resources.helpers import get_geid
from app.resources.helpers import get_files_recursive
from app.resources.helpers import set_status

# from common import GEIDClient


class DownloadClient:
    def __init__(
        self,
        files: List[Dict[str, Any]],
        auth_token: Dict[str, Any],
        operator: str,
        project_code: str,
        geid: str,
        session_id: str,
        download_type: str = 'project',
        file_geids_to_include: Optional[Set[str]] = None,
    ):
        self.job_id = 'data-download-' + str(int(time.time()))
        self.job_status = EDataDownloadStatus.INIT
        self.files = files
        self.file_nodes = []
        self.files_to_zip = []
        self.operator = operator
        self.project_code = project_code
        self.tmp_folder = ConfigClass.MINIO_TMP_PATH + project_code + '_' + str(time.time())
        self.result_file_name = ''
        self.auth_token = auth_token
        self.session_id = session_id
        self.download_type = download_type
        self.file_geids_to_include = file_geids_to_include
        self.geid = geid
        self.contains_folder = True if self.download_type == 'full_dataset' else False
        self.logger = SrvLoggerFactory('api_data_download').get_logger()

        for file in files:
            self.add_files_to_list(file['geid'])

        if len(self.files_to_zip) < 1 and self.download_type != 'full_dataset':
            error_msg = '[Invalid file amount] must greater than 0'
            self.logger.error(error_msg)
            raise APIException(status_code=EAPIResponseCode.bad_request.value, error_msg=error_msg)

    def set_status(self, status, payload):
        geid = self.files_to_zip[0]['geid'] if len(self.files_to_zip) > 0 else self.geid
        return set_status(
            self.session_id,
            self.job_id,
            self.result_file_name,
            'data_download',
            status,
            self.project_code,
            self.operator,
            geid,
            payload=payload,
        )

    def add_files_to_list(self, geid: str):
        url = ConfigClass.NEO4J_SERVICE + f'nodes/geid/{geid}'
        try:
            with httpx.Client() as client:
                res = client.get(url)
            response = res.json()[0]
            if 'Folder' in response['labels']:
                # Folder in file list
                self.logger.info(f'Getting folder from geid: {geid}')
                file_list = get_files_recursive(geid)
                if len(file_list) > 0:
                    self.contains_folder = True
            else:
                file_list = res.json()
            if self.file_geids_to_include is not None:
                file_list = [file for file in file_list if file['global_entity_id'] in self.file_geids_to_include]
            for file in file_list:
                if file.get('archived', False):
                    self.logger.info(f'file node archived skipped: {file}')
                    continue
                self.files_to_zip.append(
                    {
                        'location': file['location'],
                        'geid': file['global_entity_id'],
                        'project_code': file.get('project_code', ''),
                        'operator': self.operator,
                        'parent_folder': file['global_entity_id'],
                        'dataset_code': file.get('dataset_code', ''),
                    }
                )
        except Exception as e:
            self.logger.error(f'Fail to add files to list: {str(e)}')
            raise

    def parse_minio_location(self, location):
        minio_path = location.split('//')[-1]
        _, bucket, obj_path = tuple(minio_path.split('/', 2))
        return bucket, obj_path

    def generate_hash_code(self):
        if len(self.files_to_zip) > 1 or self.contains_folder:
            self.result_file_name = self.tmp_folder + '.zip'
        else:
            location = self.files_to_zip[0]['location']
            self.result_file_name = self.tmp_folder + '/' + self.parse_minio_location(location)[1]

        geid = self.files_to_zip[0]['geid'] if len(self.files_to_zip) > 0 else self.geid
        return generate_token(
            {
                'geid': geid,
                'full_path': self.result_file_name,
                'issuer': 'SERVICE DATA DOWNLOAD',
                'operator': self.operator,
                'session_id': self.session_id,
                'job_id': self.job_id,
                'project_code': self.project_code,
                'iat': int(time.time()),
                'exp': int(time.time()) + (ConfigClass.DOWNLOAD_TOKEN_EXPIRE_AT * 60),
            }
        )

    def add_schemas(self, dataset_geid):
        """Saves schema json files to folder that will zipped."""
        try:
            if not os.path.isdir(self.tmp_folder):
                os.mkdir(self.tmp_folder)
                os.mkdir(self.tmp_folder + '/data')

            payload = {
                'dataset_geid': dataset_geid,
                'standard': 'default',
                'is_draft': False,
            }
            with httpx.Client() as client:
                response = client.post(ConfigClass.DATASET_SERVICE + 'schema/list', json=payload)
            for schema in response.json()['result']:
                with open(self.tmp_folder + '/default_' + schema['name'], 'w') as w:
                    w.write(json.dumps(schema['content'], indent=4, ensure_ascii=False))

            payload = {
                'dataset_geid': dataset_geid,
                'standard': 'open_minds',
                'is_draft': False,
            }
            with httpx.Client() as client:
                response = client.post(ConfigClass.DATASET_SERVICE + 'schema/list', json=payload)
            for schema in response.json()['result']:
                with open(self.tmp_folder + '/openMINDS_' + schema['name'], 'w') as w:
                    w.write(json.dumps(schema['content'], indent=4, ensure_ascii=False))
        except Exception as e:
            self.logger.error(f'Fail to create schemas: {str(e)}')
            raise

    def download_files_to_tmp_folder(self, minio_client, file):
        # minio location is minio://http://<end_point>/bucket/user/object_path
        bucket, file_path = self.parse_minio_location(file['location'])
        try:
            minio_client.client.fget_object(bucket, file_path, self.tmp_folder + '/' + file_path)
        except minio.error.S3Error as e:
            # release_locks(locked)
            if e.code == 'NoSuchKey':
                self.logger.info('File not found, skipping: ' + str(e))
            else:
                raise e

    def zip_worker(self, hash_code):

        locked_node = []
        try:
            # add the file lock
            locked_node, err = recursive_lock(self.project_code, self.files)
            if err:
                raise err
            mc = Minio_Client_(self.auth_token['at'], self.auth_token['rt'])
            # download all file to tmp folder
            for obj in self.files_to_zip:
                self.download_files_to_tmp_folder(mc, obj)

            if self.download_type == 'full_dataset':
                self.add_schemas(self.geid)

            if len(self.files_to_zip) > 1 or self.contains_folder:
                shutil.make_archive(self.tmp_folder, 'zip', self.tmp_folder)

            if self.download_type == 'dataset_files':
                # Dataset file download
                # get list of file names
                filenames = ['/'.join(i['location'].split('/')[7:]) for i in self.files_to_zip]
                self.update_activity_log(
                    self.geid,
                    filenames,
                    'DATASET_FILEDOWNLOAD_SUCCEED',
                )
            self.set_status(EDataDownloadStatus.READY_FOR_DOWNLOADING.name, payload={'hash_code': hash_code})
        except Exception as e:
            payload = {'error_msg': str(e)}
            self.set_status(EDataDownloadStatus.CANCELLED.name, payload=payload)
        finally:
            self.logger.info('Start to unlock the nodes')
            for resource_key, operation in locked_node:
                unlock_resource(resource_key, operation)

    def update_activity_log(self, dataset_geid, source_entry, event_type):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': event_type,
            'payload': {
                'dataset_geid': dataset_geid,
                # "act_geid": GEIDClient().get_GEID(),
                'operator': self.operator,
                'action': 'DOWNLOAD',
                'resource': 'Dataset',
                'detail': {'source': source_entry},
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        with httpx.Client() as client:
            res = client.post(url, json=post_json)
        if res.status_code != 200:
            error_msg = 'update_activity_log {}: {}'.format(res.status_code, res.text)
            self.logger.error(error_msg)
            raise Exception(error_msg)
        return res
