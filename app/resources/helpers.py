import os
import zipfile
import time
import json
import requests
from .error_handler import internal_jsonrespon_handler
from ..config import ConfigClass
from ..commons.data_providers.redis import SrvRedisSingleton


def get_files_recursive(folder_geid, all_files=[]):
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
    resp = requests.post(ConfigClass.NEO4J_SERVICE_V2 +
                         "relations/query", json=query)
    for node in resp.json()["results"]:
        if "File" in node["labels"]:
            all_files.append(node)
        else:
            get_files_recursive(node["global_entity_id"], all_files=all_files)
    return all_files


def generate_zipped_file_path(project_code):
    '''
    generate zipped file path
    '''
    target_path = os.path.join(
        ConfigClass.ROOT_PATH, project_code, 'workdir')  # data/vre-storage/project_code/workdir
    zip_filename = project_code + '_zipped' + \
        '_' + str(int(time.time())) + '.zip'
    zipped_file_path = os.path.join(target_path, zip_filename)
    return zipped_file_path


def zip_multi_files(zipped_file_path, target_files, project_code):
    '''
    zip multiple files
    '''
    target_path = os.path.dirname(zipped_file_path)
    if not os.path.isdir(target_path):
        try:
            os.makedirs(target_path)
        except FileExistsError as file_e:
            # ignore existed folder
            pass
    try:
        with zipfile.ZipFile(zipped_file_path, 'w', zipfile.ZIP_STORED) as zf:
            for f in target_files:
                full_path = f["full_path"]
                if not os.path.exists(full_path):
                    return False, 'File not found: %s' % full_path
                with open(full_path, 'rb') as fp:
                    path = full_path.replace(
                        ConfigClass.ROOT_PATH + "/" + project_code, "")
                    if path.startswith("/raw"):
                        path = path[5:]
                    elif path.startswith("/processed"):
                        path = path[11:]
                    zf.writestr(path, fp.read())
    except Exception as e:
        return False, str(e)

    return True, zipped_file_path


def namespace_to_path(my_disk_namespace: str):
    '''
    disk namespace to path
    '''
    return {
        "greenroom": ConfigClass.NFS_ROOT_PATH,
        "vrecore": ConfigClass.VRE_ROOT_PATH
    }.get(my_disk_namespace, None)


def get_frontend_zone(my_disk_namespace: str):
    '''
    disk namespace to path
    '''
    return {
        "greenroom": "Green Room",
        "vre": "Vre Core",
        "vrecore": "VRE Core"
    }.get(my_disk_namespace, None)


def set_status(session_id, job_id, source, action, target_status,
               project_code, operator, geid, payload=None, progress=0):
    '''
    set session job status
    '''
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


def get_status(session_id, job_id, project_code, action, operator=None):
    '''
    get session job status from datastore
    '''
    srv_redis = SrvRedisSingleton()
    my_key = "dataaction:{}:{}:{}:{}".format(
        session_id, job_id, action, project_code)
    if operator:
        my_key = "dataaction:{}:{}:{}:{}:{}".format(
            session_id, job_id, action, project_code, operator)
    res_binary = srv_redis.mget_by_prefix(my_key)
    return [json.loads(record.decode('utf-8')) for record in res_binary] if res_binary else []


def delete_by_session_id(session_id: str, job_id: str = "*", action: str = "*"):
    '''
    delete status by session id
    '''
    srv_redis = SrvRedisSingleton()
    prefix = "dataaction:" + session_id + ":" + job_id + ":" + action
    srv_redis.mdelete_by_prefix(prefix)
    return True


def update_file_operation_logs(owner, operator, download_path, file_size, project_code,
                               generate_id, operation_type="data_download", extra=None):
    '''
    Endpoint
    /v1/file/actions/logs
    '''
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
