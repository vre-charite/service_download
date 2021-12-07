import json
import os
import time
from typing import List

import requests

from .error_handler import internal_jsonrespon_handler
from ..commons.data_providers.redis import SrvRedisSingleton
from ..config import ConfigClass


def get_geid():
    url = ConfigClass.COMMON_SERVICE + "utility/id"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['result']
    else:
        raise Exception('get_geid {}: {}'.format(response.status_code, url))


def get_files_recursive(folder_geid):
    all_files = []

    query = {
        "start_label": "Folder",
        "end_labels": ["File", "Folder"],
        "query": {
            "start_params": {
                "global_entity_id": folder_geid,
            },
            "end_params": {
                "archived": False,
            }
        }
    }
    resp = requests.post(
        ConfigClass.NEO4J_SERVICE_V2 + "relations/query", json=query
    )
    for node in resp.json()["results"]:
        if "File" in node["labels"]:
            all_files.append(node)
        else:
            all_files += get_files_recursive(node["global_entity_id"])
    return all_files


def get_children_nodes(start_geid:str) -> list:
    '''
        The function is different than above one
        this one will return next layer folder or 
        files under the start_geid.
    '''

    payload = {
        "label": "own",
        "start_label": "Folder",
        "start_params": {"global_entity_id":start_geid},
    }

    node_query_url = ConfigClass.NEO4J_SERVICE + "relations/query"
    response = requests.post(node_query_url, json=payload)
    ffs = [x.get("end_node") for x in response.json()]

    return ffs


def get_resource_bygeid(geid, exclude_archived=False) -> dict:
    '''
        function will call the neo4j api to get the node
        by geid. raise exception if the geid is not exist
    '''
    url = ConfigClass.NEO4J_SERVICE + "nodes/geid/%s"%geid
    res = requests.get(url)
    nodes = res.json()

    if len(nodes) == 0:
        raise Exception('Not found resource: ' + geid)

    return nodes[0]




def namespace_to_path(my_disk_namespace: str):
    """Disk namespace to path."""

    return {
        "greenroom": ConfigClass.NFS_ROOT_PATH,
        "vrecore": ConfigClass.VRE_ROOT_PATH
    }.get(my_disk_namespace, None)


def get_frontend_zone(my_disk_namespace: str):
    """Disk namespace to path."""

    return {
        "greenroom": "Green Room",
        "vre": "Vre Core",
        "vrecore": "VRE Core"
    }.get(my_disk_namespace, None)


def set_status(session_id, job_id, source, action, target_status,
               project_code, operator, geid, payload=None, progress=0):
    """Set session job status."""

    srv_redis = SrvRedisSingleton()
    my_key = "dataaction:{}:{}:{}:{}:{}:{}".format(
        session_id, job_id, action, project_code, operator, source)
    payload = payload if payload else {}
    payload["zone"] = ConfigClass.disk_namespace
    payload["frontend_zone"] = get_frontend_zone(ConfigClass.disk_namespace)
    record = {
        "session_id": session_id,
        "job_id": job_id,
        "geid": geid,
        "source": source,
        "action": action,
        "status": target_status,
        "project_code": project_code,
        "operator": operator,
        "progress": progress,
        "payload": payload,
        'update_timestamp': str(round(time.time()))
    }
    my_value = json.dumps(record)
    srv_redis.set_by_key(my_key, my_value)
    return record


def get_status(session_id, job_id, project_code, action, operator=None) -> List[str]:
    """Get session job status from datastore."""

    srv_redis = SrvRedisSingleton()
    my_key = "dataaction:{}:{}:{}:{}".format(
        session_id, job_id, action, project_code)
    if operator:
        my_key = "dataaction:{}:{}:{}:{}:{}".format(
            session_id, job_id, action, project_code, operator)
    res_binary = srv_redis.mget_by_prefix(my_key)
    return [json.loads(record.decode('utf-8')) for record in res_binary] if res_binary else []


def delete_by_session_id(session_id: str, job_id: str = "*", action: str = "*"):
    """Delete status by session id."""

    srv_redis = SrvRedisSingleton()
    prefix = "dataaction:" + session_id + ":" + job_id + ":" + action
    srv_redis.mdelete_by_prefix(prefix)
    return True


def update_file_operation_logs(owner, operator, download_path, file_size, project_code,
                               generate_id, operation_type="data_download", extra=None):
    """Endpoint.

    /v1/file/actions/logs
    """

    url = ConfigClass.DATA_OPS_GR + 'file/actions/logs'
    payload = {
        "operation_type": operation_type,
        "owner": owner,
        "operator": operator,
        "input_file_path": download_path,
        "output_file_path": download_path,
        "file_size": file_size,
        "project_code": project_code,
        "generate_id": generate_id
    }
    res_update_file_operation_logs = requests.post(
        url,
        json=payload
    )
    # new audit log api
    url_audit_log = ConfigClass.PROVENANCE_SERVICE + 'audit-logs'
    payload_audit_log = {
        "action": operation_type,
        "operator": operator,
        "target": download_path,
        "outcome": download_path,
        "resource": "file",
        "display_name": os.path.basename(download_path),
        "project_code": project_code,
        "extra": extra if extra else {}
    }
    res_audit_logs = requests.post(
        url_audit_log,
        json=payload_audit_log
    )
    return internal_jsonrespon_handler(url_audit_log, res_audit_logs)


# ########################################### File lock apis ######################################
# def lock_resource(resource_key:str, operation:str) -> dict:
#     # operation can be either read or write
#     print("====== Lock resource:", resource_key)
#     url = ConfigClass.DATA_OPS_UT_V2 + 'resource/lock'
#     post_json = {
#         "resource_key": resource_key,
#         "operation": operation
#     }

#     response = requests.post(url, json=post_json)
#     if response.status_code != 200:
#         raise Exception("resource %s already in used"%resource_key)

#     return response.json()


# def unlock_resource(resource_key:str, operation:str) -> dict:
#     # operation can be either read or write
#     print("====== Unlock resource:", resource_key)
#     url = ConfigClass.DATA_OPS_UT_V2 + 'resource/lock'
#     post_json = {
#         "resource_key": resource_key,
#         "operation": operation
#     }
    
#     response = requests.delete(url, json=post_json)
#     if response.status_code != 200:
#         raise Exception("Error when unlock resource %s"%resource_key)

#     return response.json()


# def recursive_lock(code:str, ff_geids:list, new_name:str=None) \
#     -> (list, Exception):
#     '''
#     the function will recursively lock the node tree
#     '''

#     # this is for crash recovery, if something trigger the exception
#     # we will unlock the locked node only. NOT the whole tree. The example
#     # case will be copy the same node, if we unlock the whole tree in exception
#     # then it will affect the processing one.
#     locked_node, err = [], None

#     def recur_walker(currenct_nodes, new_name=None):
#         '''
#         recursively trace down the node tree and run the lock function
#         '''

#         for ff_object in currenct_nodes:
#             # we will skip the deleted nodes
#             if ff_object.get("archived", False):
#                 continue
            
#             # conner case here, we DONT lock the name folder
#             # for the copy we will lock the both source and target
#             if ff_object.get("name") != ff_object.get("uploader"):
#                 bucket_prefix = "gr-" if "Greenroom" in ff_object.get("labels") else "core-"
#                 source_key = "{}/{}".format(bucket_prefix+code, ff_object.get("display_path"))
#                 lock_resource(source_key, "read")
#                 locked_node.append((source_key, "read"))

#             # open the next recursive loop if it is folder
#             if 'Folder' in ff_object.get("labels"):
#                 children_nodes = get_children_nodes(ff_object.get("global_entity_id", None))
#                 recur_walker(children_nodes)

#         return

#     # start here
#     try:
#         # slightly different here, since the download only gives
#         # the folder/file geid. then I have to get node by geid so
#         # that we can get the path/
#         nodes = [get_resource_bygeid(geid.get("geid")) for geid in ff_geids]
        
#         # recur_walker(nodes, new_name)
#     except Exception as e:
#         err = e

#     return locked_node, err