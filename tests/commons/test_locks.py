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

import pytest

from app.commons.locks import lock_resource
from app.commons.locks import recursive_lock
from app.commons.locks import unlock_resource


@pytest.mark.parametrize('lock_function,request_method', [(lock_resource, 'POST'), (unlock_resource, 'DELETE')])
def test_lock_resource_should_call_resource_lock_correctly(httpx_mock, lock_function, request_method):
    httpx_mock.add_response(
        method=request_method, url='http://data_ops_util/v2/resource/lock/', status_code=200, json={}
    )
    resp = lock_function('fake_key', 'me')
    assert resp == {}


@pytest.mark.parametrize('lock_function,request_method', [(lock_resource, 'POST'), (unlock_resource, 'DELETE')])
def test_lock_resource_should_raise_exception_when_lock_request_not_200(httpx_mock, lock_function, request_method):
    httpx_mock.add_response(
        method=request_method, url='http://data_ops_util/v2/resource/lock/', status_code=404, json={}
    )
    with pytest.raises(Exception):
        lock_function('fake_key', 'me')


@pytest.mark.parametrize(
    'labels,expected_locked_node',
    [
        (['File'], ('any_code/display_path', 'read')),
        (['Greenroom', 'File'], ('gr-any_code/display_path', 'read')),
        (['Core', 'File'], ('core-any_code/display_path', 'read')),
    ],
)
def test_recursive_lock_should_build_correct_node_path_to_specific_label(httpx_mock, labels, expected_locked_node):
    code = 'any_code'
    ff_geids = [{'geid': 'geid_1'}]
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/geid_1',
        json=[{'labels': labels, 'display_path': 'display_path', 'uploader': 'test'}],
    )
    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/', status_code=200, json={})
    locked_node, error = recursive_lock(code, ff_geids)
    assert not error
    assert locked_node[0] == expected_locked_node


def test_recursive_when_node_is_archived(httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/geid_1',
        json=[{'labels': ['File'], 'display_path': 'display_path', 'uploader': 'test', 'archived': True}],
    )
    locked_node, error = recursive_lock('any_code', [{'geid': 'geid_1'}])
    assert not error
    assert locked_node == []


def test_recursive_lock_when_node_is_Folder(httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/geid_1',
        json=[
            {
                'labels': ['Folder'],
                'global_entity_id': 'geid_2',
                'display_path': 'display_path',
                'uploader': 'test',
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[
            {
                'end_node': {
                    'labels': 'File',
                    'global_entity_id': 'fake_geid',
                }
            }
        ],
    )
    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/', status_code=200, json={})
    locked_node, error = recursive_lock('any_code', [{'geid': 'geid_1'}])
    assert not error
    assert locked_node[0] == ('any_code/display_path', 'read')
