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
import time
from typing import List

import httpx

from app.commons.data_providers.redis import SrvRedisSingleton
from app.config import ConfigClass

from .error_handler import internal_jsonrespon_handler


def get_geid():
    url = ConfigClass.UTILITY_SERVICE + 'utility/id'
    with httpx.Client() as client:
        response = client.get(url)
    if response.status_code == 200:
        return response.json()['result']
    else:
        raise Exception('get_geid {}: {}'.format(response.status_code, url))


def get_files_recursive(folder_geid):
    all_files = []

    query = {
        'start_label': 'Folder',
        'end_labels': ['File', 'Folder'],
        'query': {
            'start_params': {
                'global_entity_id': folder_geid,
            },
            'end_params': {
                'archived': False,
            },
        },
    }
    with httpx.Client() as client:
        resp = client.post(ConfigClass.NEO4J_SERVICE_V2 + 'relations/query', json=query)
    for node in resp.json()['results']:
        if 'File' in node['labels']:
            all_files.append(node)
        else:
            all_files += get_files_recursive(node['global_entity_id'])
    return all_files


def get_children_nodes(start_geid: str) -> list:
    """The function is different than above one this one will return next layer folder or files under the start_geid."""

    payload = {
        'label': 'own',
        'start_label': 'Folder',
        'start_params': {'global_entity_id': start_geid},
    }

    node_query_url = ConfigClass.NEO4J_SERVICE + 'relations/query'
    with httpx.Client() as client:
        response = client.post(node_query_url, json=payload)
    ffs = [x.get('end_node') for x in response.json()]

    return ffs


def get_resource_bygeid(geid, exclude_archived=False) -> dict:
    """function will call the neo4j api to get the node by geid.

    raise exception if the geid is not exist
    """
    url = ConfigClass.NEO4J_SERVICE + 'nodes/geid/%s' % geid
    with httpx.Client() as client:
        res = client.get(url)
    nodes = res.json()

    if len(nodes) == 0:
        raise Exception('Not found resource: ' + geid)

    return nodes[0]


def set_status(
    session_id, job_id, source, action, target_status, project_code, operator, geid, payload=None, progress=0
):
    """Set session job status."""

    srv_redis = SrvRedisSingleton()
    my_key = 'dataaction:{}:{}:{}:{}:{}:{}'.format(session_id, job_id, action, project_code, operator, source)
    payload = payload if payload else {}
    # payload["zone"] = ConfigClass.disk_namespace
    # payload["frontend_zone"] = get_frontend_zone(ConfigClass.disk_namespace)
    record = {
        'session_id': session_id,
        'job_id': job_id,
        'geid': geid,
        'source': source,
        'action': action,
        'status': target_status,
        'project_code': project_code,
        'operator': operator,
        'progress': progress,
        'payload': payload,
        'update_timestamp': str(round(time.time())),
    }
    my_value = json.dumps(record)
    srv_redis.set_by_key(my_key, my_value)
    return record


def get_status(session_id, job_id, project_code, action, operator=None) -> List[str]:
    """Get session job status from datastore."""

    srv_redis = SrvRedisSingleton()
    my_key = 'dataaction:{}:{}:{}:{}'.format(session_id, job_id, action, project_code)
    if operator:
        my_key = 'dataaction:{}:{}:{}:{}:{}'.format(session_id, job_id, action, project_code, operator)
    res_binary = srv_redis.mget_by_prefix(my_key)
    return [json.loads(record.decode('utf-8')) for record in res_binary] if res_binary else []


def delete_by_session_id(session_id: str, job_id: str = '*', action: str = '*'):
    """Delete status by session id."""

    srv_redis = SrvRedisSingleton()
    prefix = 'dataaction:' + session_id + ':' + job_id + ':' + action
    srv_redis.mdelete_by_prefix(prefix)
    return True


def update_file_operation_logs(operator, download_path, project_code,
                               operation_type="data_download", extra=None):
    """
    Endpoint.

    """
    # new audit log api
    url_audit_log = ConfigClass.PROVENANCE_SERVICE + 'audit-logs'
    payload_audit_log = {
        'action': operation_type,
        'operator': operator,
        'target': download_path,
        'outcome': download_path,
        'resource': 'file',
        'display_name': os.path.basename(download_path),
        'project_code': project_code,
        'extra': extra if extra else {},
    }
    with httpx.Client() as client:
        res_audit_logs = client.post(url_audit_log, json=payload_audit_log)
    return internal_jsonrespon_handler(url_audit_log, res_audit_logs)
