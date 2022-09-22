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

from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from pydantic import Field
from pydantic.types import constr

from .base_models import APIResponse


class PreDataDownloadPOST(BaseModel):
    """Pre download payload model."""

    files: List[Dict[str, Any]]
    operator: str
    session_id: str
    project_code: str = ''
    dataset_geid: str = ''
    dataset_description: bool = False
    approval_request_id: Optional[UUID]


class DatasetPrePOST(BaseModel):
    """Pre download dataset payload model."""

    dataset_geid: constr(min_length=2)
    operator: str
    session_id: str


class PreSignedDownload(BaseModel):
    """Pre signed download url payload for minio."""

    object_path: str


class PreSignedBatchDownload(BaseModel):
    """Pre signed download url payload for minio but accept a list."""

    object_path: list


class PreDataDownloadResponse(APIResponse):
    """Pre download response class."""

    result: dict = Field(
        {},
        example={
            'session_id': 'unique_session_id',
            'job_id': 'data-download-1613507376',
            'source': './test_project/workdir/test_project_zipped_1613507376.zip',
            'action': 'data_download',
            'status': 'ZIPPING',
            'project_code': 'test_project',
            'operator': 'zhengyang',
            'progress': 0,
            'payload': {
                'hash_code': (
                    'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmdWxsX3BhdGgiOiIuL3Rlc3'
                    'RfcHJvamVjdC93b3JrZGlyL3Rlc3RfcHJvamVjdF96aXBwZWRfMTYxMzUwNzM3N'
                    'i56aXAiLCJpc3N1ZXIiOiJTRVJWSUNFIERBVEEgRE9XTkxPQUQgIiwib3BlcmF0'
                    'b3IiOiJ6aGVuZ3lhbmciLCJzZXNzaW9uX2lkIjoidW5pcXVlX3Nlc3Npb25faWQ'
                    'iLCJqb2JfaWQiOiJkYXRhLWRvd25sb2FkLTE2MTM1MDczNzYiLCJwcm9qZWN0X2'
                    'NvZGUiOiJ0ZXN0X3Byb2plY3QiLCJpYXQiOjE2MTM1MDczNzYsImV4cCI6MTYxM'
                    'zUwNzY3Nn0.ipzWy6y79QxRGhQQ_VWIk-Lz8Iv8zU7JHGF3ZBoNt-g'
                )
            },
            'update_timestamp': '1613507376',
        },
    )


class GetDataDownloadStatusResponse(APIResponse):
    """Get data download status."""

    result: dict = Field(
        {},
        example={
            'session_id': 'unique_session_id',
            'job_id': 'data-download-1613507376',
            'source': './test_project/workdir/test_project_zipped_1613507376.zip',
            'action': 'data_download',
            'status': 'READY_FOR_DOWNLOADING',
            'project_code': 'test_project',
            'operator': 'zhengyang',
            'progress': 0,
            'payload': {
                'hash_code': (
                    'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmdWxsX3BhdGgiOiIuL3Rlc3'
                    'RfcHJvamVjdC93b3JrZGlyL3Rlc3RfcHJvamVjdF96aXBwZWRfMTYxMzUwNzM3N'
                    'i56aXAiLCJpc3N1ZXIiOiJTRVJWSUNFIERBVEEgRE9XTkxPQUQgIiwib3BlcmF0'
                    'b3IiOiJ6aGVuZ3lhbmciLCJzZXNzaW9uX2lkIjoidW5pcXVlX3Nlc3Npb25faWQ'
                    'iLCJqb2JfaWQiOiJkYXRhLWRvd25sb2FkLTE2MTM1MDczNzYiLCJwcm9qZWN0X2'
                    'NvZGUiOiJ0ZXN0X3Byb2plY3QiLCJpYXQiOjE2MTM1MDczNzYsImV4cCI6MTYxM'
                    'zUwNzY3Nn0.ipzWy6y79QxRGhQQ_VWIk-Lz8Iv8zU7JHGF3ZBoNt-g'
                )
            },
            'update_timestamp': '1613507385',
        },
    )


class DownloadStatusListResponse(APIResponse):
    """List data download status."""

    result: dict = Field(
        {},
        example=[
            {
                'session_id': 'unique_session_id',
                'job_id': 'data-download-1613507376',
                'source': './test_project/workdir/test_project_zipped_1613507376.zip',
                'action': 'data_download',
                'status': 'READY_FOR_DOWNLOADING',
                'project_code': 'test_project',
                'operator': 'zhengyang',
                'progress': 0,
                'payload': {
                    'hash_code': (
                        'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmdWxsX3BhdGgiOiIuL3Rlc3'
                        'RfcHJvamVjdC93b3JrZGlyL3Rlc3RfcHJvamVjdF96aXBwZWRfMTYxMzUwNzM3N'
                        'i56aXAiLCJpc3N1ZXIiOiJTRVJWSUNFIERBVEEgRE9XTkxPQUQgIiwib3BlcmF0'
                        'b3IiOiJ6aGVuZ3lhbmciLCJzZXNzaW9uX2lkIjoidW5pcXVlX3Nlc3Npb25faWQ'
                        'iLCJqb2JfaWQiOiJkYXRhLWRvd25sb2FkLTE2MTM1MDczNzYiLCJwcm9qZWN0X2'
                        'NvZGUiOiJ0ZXN0X3Byb2plY3QiLCJpYXQiOjE2MTM1MDczNzYsImV4cCI6MTYxM'
                        'zUwNzY3Nn0.ipzWy6y79QxRGhQQ_VWIk-Lz8Iv8zU7JHGF3ZBoNt-g'
                    )
                },
                'update_timestamp': '1613507385',
            }
        ],
    )


class EDataDownloadStatus(Enum):
    INIT = 0
    CANCELLED = 1
    ZIPPING = 3
    READY_FOR_DOWNLOADING = 5
    SUCCEED = 7
