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

from unittest import mock

import minio
import pytest

from app.commons.download_manager import DownloadClient
from app.resources.error_handler import APIException


def test_download_client_without_files_should_raise_exception(httpx_mock, mock_minio):
    with pytest.raises(APIException):
        DownloadClient(
            files=[],
            auth_token={'at': 'token', 'rt': 'refresh_token'},
            operator='me',
            project_code='any_code',
            geid='geid_1',
            session_id='1234',
        )


def test_zip_worker_set_status_READY_FOR_DOWNLOADING_when_success(httpx_mock, mock_minio):
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/geid_1',
        json=[
            {
                'labels': ['File'],
                'global_entity_id': 'geid_2',
                'location': 'http://anything.com/bucket/obj/path',
                'display_path': 'display_path',
                'uploader': 'test',
                'archived': True,
            },
            {
                'labels': ['File'],
                'global_entity_id': 'geid_2',
                'location': 'http://anything.com/bucket/obj/path',
                'display_path': 'display_path',
                'uploader': 'test',
            },
        ],
    )
    download_client = DownloadClient(
        files=[{'geid': 'geid_1'}],
        auth_token={'at': 'token', 'rt': 'refresh_token'},
        operator='me',
        project_code='any_code',
        geid='geid_1',
        session_id='1234',
    )
    with mock.patch.object(DownloadClient, 'set_status') as fake_set:
        download_client.zip_worker('fake_hash')
    fake_set.assert_called_once_with('READY_FOR_DOWNLOADING', payload={'hash_code': 'fake_hash'})


def test_zip_worker_set_status_CANCELLED_when_success(httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/geid_1',
        json=[
            {
                'labels': ['File'],
                'global_entity_id': 'geid_2',
                'location': 'http://anything.com/bucket/obj/path',
                'display_path': 'display_path',
                'uploader': 'test',
            }
        ],
    )
    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/', status_code=200, json={})
    httpx_mock.add_response(method='DELETE', url='http://data_ops_util/v2/resource/lock/', status_code=200, json={})
    download_client = DownloadClient(
        files=[{'geid': 'geid_1'}],
        auth_token='token',
        operator='me',
        project_code='any_code',
        geid='geid_1',
        session_id='1234',
    )
    with mock.patch.object(DownloadClient, 'set_status') as fake_set:
        download_client.zip_worker('fake_hash')
    fake_set.assert_called_once_with('CANCELLED', payload={'error_msg': 'string indices must be integers'})


@mock.patch('app.commons.service_connection.minio_client.Minio')
@pytest.mark.parametrize(
    'exception_code,result',
    [
        (
            'any',
            {
                'status': 'CANCELLED',
                'payload': {
                    'error_msg': (
                        'S3 operation failed; code: any, message: any msg'
                        ', resource: any, request_id: any, host_id: any'
                    )
                },
            },
        ),
        ('NoSuchKey', {'status': 'READY_FOR_DOWNLOADING', 'payload': {'hash_code': 'fake_hash'}}),
    ],
)
def test_zip_worker_raise_exception_when_minio_return_error(mock_minio, httpx_mock, exception_code, result):
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/geid_1',
        json=[
            {
                'labels': ['File'],
                'global_entity_id': 'geid_2',
                'location': 'http://anything.com/bucket/obj/path',
                'display_path': 'display_path',
                'uploader': 'test',
            }
        ],
    )
    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/', status_code=200, json={})
    httpx_mock.add_response(method='DELETE', url='http://data_ops_util/v2/resource/lock/', status_code=200, json={})
    minio_exception = minio.error.S3Error(
        code=exception_code, message='any msg', resource='any', request_id='any', host_id='any', response='error'
    )
    mock_minio().fget_object.side_effect = [minio_exception]

    download_client = DownloadClient(
        files=[{'geid': 'geid_1'}],
        auth_token={'at': 'token', 'rt': 'refresh_token'},
        operator='me',
        project_code='any_code',
        geid='geid_1',
        session_id='1234',
    )
    with mock.patch.object(DownloadClient, 'set_status') as fake_set:
        download_client.zip_worker('fake_hash')
    fake_set.assert_called_once_with(result['status'], payload=result['payload'])


def test_zip_worker_full_dataset_set_status_READY_FOR_DOWNLOADING_when_success(httpx_mock, mock_minio):
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/geid_1',
        json=[
            {
                'labels': ['File'],
                'global_entity_id': 'geid_2',
                'location': 'http://anything.com/bucket/obj/path',
                'display_path': 'display_path',
                'uploader': 'test',
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://dataset_service/v1/schema/list',
        status_code=200,
        json={'result': [{'name': 'name', 'content': 'content'}]},
    )
    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/', status_code=200, json={})
    httpx_mock.add_response(method='DELETE', url='http://data_ops_util/v2/resource/lock/', status_code=200, json={})
    download_client = DownloadClient(
        files=[{'geid': 'geid_1'}],
        auth_token={'at': 'token', 'rt': 'refresh_token'},
        operator='me',
        project_code='any_code',
        geid='geid_1',
        session_id='1234',
        download_type='full_dataset',
    )
    with mock.patch.object(DownloadClient, 'set_status') as fake_set:
        download_client.zip_worker('fake_hash')
    fake_set.assert_called_once_with('READY_FOR_DOWNLOADING', payload={'hash_code': 'fake_hash'})
