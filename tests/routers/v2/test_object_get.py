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


async def test_v2_get_object_File_should_return_200_when_success(client, httpx_mock, mock_minio):
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/nodes/File/query',
        json=[
            {
                'code': 'any_code',
                'labels': 'File',
                'location': 'http://anything.com/bucket/obj/path',
                'global_entity_id': 'fake_geid',
                'project_code': '',
                'operator': 'me',
                'parent_folder': '',
                'dataset_code': 'fake_dataset_code',
            }
        ],
    )
    resp = await client.get('/v2/object/any_id', headers={'Authorization': 'token', 'Refresh-Token': 'refresh_token'})
    assert resp.status_code == 200
    assert resp.text == 'File like object'


async def test_v2_get_object_File_should_return_200_and_error_msg_when_minio_fails(client, httpx_mock):
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/nodes/File/query',
        json=[
            {
                'code': 'any_code',
                'labels': 'File',
                'location': 'http://anything.com/bucket/obj/path',
                'global_entity_id': 'fake_geid',
                'project_code': '',
                'operator': 'me',
                'parent_folder': '',
                'dataset_code': 'fake_dataset_code',
            }
        ],
    )
    resp = await client.get('/v2/object/any_id', headers={'Authorization': 'token', 'Refresh-Token': 'refresh_token'})
    assert resp.status_code == 200
    assert 'Error getting file from minio:' in resp.json()['error_msg']


@mock.patch('time.time')
@pytest.mark.parametrize('archived', [(True), (False)])
async def test_v2_get_object_Folder_should_return_200_when_success(mock_time, client, httpx_mock, mock_minio, archived):
    httpx_mock.add_response(method='POST', url='http://neo4j_service/v1/neo4j/nodes/File/query', json=[])
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v2/neo4j/relations/query',
        json={
            'results': [
                {
                    'code': 'any_code',
                    'labels': 'File',
                    'location': 'http://anything.com/bucket/obj/path',
                    'global_entity_id': 'fake_geid',
                    'project_code': '',
                    'operator': 'me',
                    'parent_folder': '',
                    'dataset_code': 'fake_dataset_code',
                    'archived': archived,
                }
            ]
        },
    )
    mock_time.return_value = 1
    resp = await client.get('/v2/object/any_id', headers={'Authorization': 'token', 'refresh_token': 'refresh_token'})
    assert resp.status_code == 200
    assert resp.text == 'PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'


@mock.patch('time.time')
@mock.patch('app.commons.service_connection.minio_client.Minio')
@pytest.mark.parametrize(
    'status_code,exception_code',
    [
        (500, 'any'),
        (200, 'NoSuchKey'),
    ],
)
async def test_v2_get_object_Folder_should_return_correct_status_code_when_minio_exception_raised(
    mock_minio, mock_time, client, httpx_mock, status_code, exception_code
):
    httpx_mock.add_response(method='POST', url='http://neo4j_service/v1/neo4j/nodes/File/query', json=[])
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v2/neo4j/relations/query',
        json={
            'results': [
                {
                    'code': 'any_code',
                    'labels': 'File',
                    'location': 'http://anything.com/bucket/obj/path',
                    'global_entity_id': 'fake_geid',
                    'project_code': '',
                    'operator': 'me',
                    'parent_folder': '',
                    'dataset_code': 'fake_dataset_code',
                }
            ]
        },
    )
    mock_time.return_value = 1
    minio_exception = minio.error.S3Error(
        code=exception_code, message='any msg', resource='any', request_id='any', host_id='any', response='error'
    )
    mock_minio().fget_object.side_effect = [minio_exception]
    resp = await client.get('/v2/object/any_id', headers={'Authorization': 'token', 'refresh_token': 'refresh_token'})
    assert resp.status_code == status_code
