from enum import Enum
from pydantic import BaseModel, Field
from .base_models import APIResponse

### PreDataDowanload ###


class PreDataDownloadPOST(BaseModel):
    '''
    Pre download payload model
    '''
    files: list
    operator: str
    session_id: str
    project_code: str = ""
    dataset_geid: str = ""
    dataset_description: bool = False

class DatasetPrePOST(BaseModel):
    '''
    Pre download dataset payload model
    '''
    dataset_geid: str
    operator: str
    session_id: str

class PreSignedDownload(BaseModel):
    '''
    presigned download url payload for minio
    '''
    object_path: str

class PreSignedBatchDownload(BaseModel):
    '''
    presigned download url payload for minio but accept a list of 
    '''
    object_path: list

class PreDataDowanloadResponse(APIResponse):
    '''
    Pre download response class
    '''
    result: dict = Field({}, example={
        "session_id": "unique_session_id",
        "job_id": "data-download-1613507376",
        "source": "./test_project/workdir/test_project_zipped_1613507376.zip",
        "action": "data_download",
        "status": "ZIPPING",
        "project_code": "test_project",
        "operator": "zhengyang",
        "progress": 0,
        "payload": {
            "hash_code": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmdWxsX3BhdGgiOiIuL3Rlc3RfcHJvamVjdC93b3JrZGlyL3Rlc3RfcHJvamVjdF96aXBwZWRfMTYxMzUwNzM3Ni56aXAiLCJpc3N1ZXIiOiJTRVJWSUNFIERBVEEgRE9XTkxPQUQgIiwib3BlcmF0b3IiOiJ6aGVuZ3lhbmciLCJzZXNzaW9uX2lkIjoidW5pcXVlX3Nlc3Npb25faWQiLCJqb2JfaWQiOiJkYXRhLWRvd25sb2FkLTE2MTM1MDczNzYiLCJwcm9qZWN0X2NvZGUiOiJ0ZXN0X3Byb2plY3QiLCJpYXQiOjE2MTM1MDczNzYsImV4cCI6MTYxMzUwNzY3Nn0.ipzWy6y79QxRGhQQ_VWIk-Lz8Iv8zU7JHGF3ZBoNt-g"
        },
        "update_timestamp": "1613507376"
    }
    )


class GetDataDownloadStatusRespon(APIResponse):
    '''
    Get data download status
    '''
    result: dict = Field({}, example={
        "session_id": "unique_session_id",
        "job_id": "data-download-1613507376",
        "source": "./test_project/workdir/test_project_zipped_1613507376.zip",
        "action": "data_download",
        "status": "READY_FOR_DOWNLOADING",
        "project_code": "test_project",
        "operator": "zhengyang",
        "progress": 0,
        "payload": {
            "hash_code": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmdWxsX3BhdGgiOiIuL3Rlc3RfcHJvamVjdC93b3JrZGlyL3Rlc3RfcHJvamVjdF96aXBwZWRfMTYxMzUwNzM3Ni56aXAiLCJpc3N1ZXIiOiJTRVJWSUNFIERBVEEgRE9XTkxPQUQgIiwib3BlcmF0b3IiOiJ6aGVuZ3lhbmciLCJzZXNzaW9uX2lkIjoidW5pcXVlX3Nlc3Npb25faWQiLCJqb2JfaWQiOiJkYXRhLWRvd25sb2FkLTE2MTM1MDczNzYiLCJwcm9qZWN0X2NvZGUiOiJ0ZXN0X3Byb2plY3QiLCJpYXQiOjE2MTM1MDczNzYsImV4cCI6MTYxMzUwNzY3Nn0.ipzWy6y79QxRGhQQ_VWIk-Lz8Iv8zU7JHGF3ZBoNt-g"
        },
        "update_timestamp": "1613507385"
    })


class DownloadStatusListRespon(APIResponse):
    '''
    List data download status
    '''
    result: dict = Field({}, example=[
        {
            "session_id": "unique_session_id",
            "job_id": "data-download-1613507376",
            "source": "./test_project/workdir/test_project_zipped_1613507376.zip",
            "action": "data_download",
            "status": "READY_FOR_DOWNLOADING",
            "project_code": "test_project",
            "operator": "zhengyang",
            "progress": 0,
            "payload": {
                "hash_code": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmdWxsX3BhdGgiOiIuL3Rlc3RfcHJvamVjdC93b3JrZGlyL3Rlc3RfcHJvamVjdF96aXBwZWRfMTYxMzUwNzM3Ni56aXAiLCJpc3N1ZXIiOiJTRVJWSUNFIERBVEEgRE9XTkxPQUQgIiwib3BlcmF0b3IiOiJ6aGVuZ3lhbmciLCJzZXNzaW9uX2lkIjoidW5pcXVlX3Nlc3Npb25faWQiLCJqb2JfaWQiOiJkYXRhLWRvd25sb2FkLTE2MTM1MDczNzYiLCJwcm9qZWN0X2NvZGUiOiJ0ZXN0X3Byb2plY3QiLCJpYXQiOjE2MTM1MDczNzYsImV4cCI6MTYxMzUwNzY3Nn0.ipzWy6y79QxRGhQQ_VWIk-Lz8Iv8zU7JHGF3ZBoNt-g"
            },
            "update_timestamp": "1613507385"
        }
    ])


class EDataDownloadStatus(Enum):
    INIT = 0
    CANCELLED = 1
    ZIPPING = 3
    READY_FOR_DOWNLOADING = 5
    SUCCEED = 7
