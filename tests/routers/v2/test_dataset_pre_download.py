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

async def test_v2_dataset_download_pre_return_500_when_query_not_found(
    client,
    httpx_mock,
):
    dataset_geid = 'fake_dataset_geid'
    httpx_mock.add_response(
        method='POST', url='http://neo4j_service/v2/neo4j/relations/query', json={}, status_code=404
    )
    httpx_mock.add_response(
        method='GET', url=f'http://neo4j_service/v1/neo4j/nodes/geid/{dataset_geid}', json=[], status_code=404
    )
    resp = await client.post(
        '/v2/dataset/download/pre', json={'session_id': 1234, 'operator': 'me', 'dataset_geid': dataset_geid}
    )
    assert resp.status_code == 500
    assert resp.json() == {
        'code': 500,
        'error_msg': 'Error when getting node for neo4j',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': [],
    }


async def test_v2_dataset_download_pre_return_200_when_success(
    client,
    httpx_mock,
):
    dataset_geid = 'fake_dataset_geid'
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
    httpx_mock.add_response(
        method='GET',
        url=f'http://neo4j_service/v1/neo4j/nodes/geid/{dataset_geid}',
        json=[
            {
                'code': 'any_code',
                'labels': ['Folder'],
                'location': 'http://anything.com/bucket/obj/path',
                'global_entity_id': 'fake_geid',
                'project_code': '',
                'operator': 'me',
                'parent_folder': '',
                'dataset_code': 'fake_dataset_code',
            }
        ],
    )
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/fake_geid',
        json=[
            {
                'code': 'any_code',
                'labels': ['File'],
                'location': 'http://anything.com/bucket/obj/path',
                'global_entity_id': 'fake_geid',
                'project_code': '',
                'operator': 'me',
                'parent_folder': '',
                'dataset_code': 'fake_dataset_code',
            }
        ],
    )

    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json={},
    )
    resp = await client.post(
        '/v2/dataset/download/pre', json={'session_id': 1234, 'operator': 'me', 'dataset_geid': dataset_geid}
    )

    assert resp.status_code == 200
    result = resp.json()['result']
    assert resp.status_code == 200
    assert result['session_id'] == '1234'
    assert result['job_id']
    assert result['geid'] == 'fake_geid'
    assert result['project_code'] in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == 'any_code'
    assert result['operator'] == 'me'
    assert result['progress'] == 0
    assert result['payload']['hash_code']
