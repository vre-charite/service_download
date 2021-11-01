import requests
from app.config import ConfigClass

def release_locks(locked: list):
    unlocked = [unlock_resource(k) for k in locked]
    return unlocked

def lock_resource(resource_key):
    '''
    lock resource
    '''
    url = ConfigClass.DATA_OPS_UTIL + 'resource/lock'
    post_json = {
        "resource_key": resource_key
    }
    response = requests.post(url, json=post_json)
    return response

def check_lock(resource_key):
    '''
    get resource lock
    '''
    url = ConfigClass.DATA_OPS_UTIL + 'resource/lock'
    params = {
        "resource_key": resource_key
    }
    response = requests.get(url, params=params)
    return response

def unlock_resource(resource_key):
    '''
    unlock resource
    '''
    url = ConfigClass.DATA_OPS_UTIL + 'resource/lock'
    post_json = {
        "resource_key": resource_key
    }
    response = requests.delete(url, json=post_json)
    return response
