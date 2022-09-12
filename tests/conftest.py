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
import shutil
from io import BytesIO

import pytest
from async_asgi_testclient import TestClient
from httpx import Response
from redis import StrictRedis
from starlette.config import environ
from urllib3 import HTTPResponse

environ['NEO4J_SERVICE'] = 'http://NEO4J_SERVICE'
environ['QUEUE_SERVICE'] = 'http://QUEUE_SERVICE'
environ['PROVENANCE_SERVICE'] = 'http://PROVENANCE_SERVICE'
environ['DATASET_SERVICE'] = 'http://DATASET_SERVICE'
environ['UTILITY_SERVICE'] = 'http://UTILITY_SERVICE'
environ['DATA_OPS_UTIL'] = 'http://DATA_OPS_UTIL'

environ['CORE_ZONE_LABEL'] = 'Core'
environ['GREEN_ZONE_LABEL'] = 'Greenroom'
environ['MINIO_OPENID_CLIENT'] = 'MINIO_OPENID_CLIENT'
environ['MINIO_ENDPOINT'] = 'MINIO_ENDPOINT'
environ['MINIO_HTTPS'] = 'MINIO_HTTPS'
environ['KEYCLOAK_URL'] = 'KEYCLOAK_URL'
environ['MINIO_TEST_PASS'] = 'MINIO_TEST_PASS'
environ['MINIO_ACCESS_KEY'] = 'MINIO_ACCESS_KEY'
environ['MINIO_SECRET_KEY'] = 'MINIO_SECRET_KEY'
environ['KEYCLOAK_MINIO_SECRET'] = 'KEYCLOAK_MINIO_SECRET'
environ['REDIS_PORT'] = '6379'
environ['REDIS_DB'] = '0'
environ['REDIS_PASSWORD'] = ''
environ['RDS_HOST'] = '127.0.0.1'
environ['RDS_PORT'] = '5432'
environ['RDS_USER'] = 'RDS_USER'
environ['RDS_PWD'] = 'RDS_PWD'
environ['RDS_DBNAME'] = 'RDS_DBNAME'
environ['RDS_SCHEMA_DEFAULT'] = 'RDS_SCHEMA_DEFAULT'
environ['ROOT_PATH'] = './tests/'


@pytest.fixture(autouse=True)
def clean_up_redis():
    cache = StrictRedis(host=environ.get('REDIS_HOST'))
    cache.flushall()


@pytest.fixture(scope='session', autouse=True)
def create_folders():
    folder_path = './tests/tmp/'
    os.makedirs(folder_path + 'any_id_1')
    yield
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)


@pytest.fixture(autouse=True)
def mock_settings(monkeypatch):
    from app.config import ConfigClass

    monkeypatch.setattr(ConfigClass, 'MINIO_TMP_PATH', './tests/tmp/')


@pytest.fixture
def jwt_token():
    return (
        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3OD'
        'kwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJmdWxsX3Bh'
        'dGgiOiJ0ZXN0cy9yb3V0ZXJzL3YxL2VtcHR5LnR4dCIsInNlc3Npb25faWQiOj'
        'EyMywiam9iX2lkIjoiZmFrZV9nbG9iYWxfZW50aXR5X2lkIiwicHJvamVjdF9j'
        'b2RlIjoiYW55Iiwib3BlcmF0b3IiOiJtZSIsImdlaWQiOiJmYWtlX2dsb2JhbF'
        '9lbnRpdHlfaWQiLCJsb2NhdGlvbiI6Imh0dHA6Ly9hbnl0aGluZy5jb20vYnVj'
        'a2V0L29iai9wYXRoIn0.mJJ7tTxyQdQcUxq3KmK_-Q6W7wvIt4qmAIT2OyfTQF8'
    )


@pytest.fixture
def anyio_backend():
    return 'asyncio'


@pytest.fixture
def app(anyio_backend):
    from app.main import create_app

    app = create_app()
    yield app


@pytest.fixture
async def client(app):
    return TestClient(app)


@pytest.fixture
def mock_minio(monkeypatch):
    from app.commons.service_connection.minio_client import Minio

    class FakeObject:
        size = b'a'

    http_response = HTTPResponse()
    response = Response(status_code=200)
    response.raw = http_response
    response.raw._fp = BytesIO(b'File like object')

    monkeypatch.setattr(Minio, 'stat_object', lambda x, y, z: FakeObject())
    monkeypatch.setattr(Minio, 'get_object', lambda x, y, z: http_response)
    monkeypatch.setattr(Minio, 'list_buckets', lambda x: [])
    monkeypatch.setattr(Minio, 'fget_object', lambda *x: [])


@pytest.fixture
async def fake_job(monkeypatch):
    from app.commons.data_providers.redis import SrvRedisSingleton

    fake_job = {
        'session_id': '1234',
        'job_id': 'fake_global_entity_id',
        'source': 'tests/routers/v1/empty.txt',
        'action': 'data_upload',
        'status': 'PRE_UPLOADED',
        'project_code': 'any',
        'operator': 'me',
        'progress': 0,
        'payload': {
            'task_id': 'fake_global_entity_id',
            'resumable_identifier': 'fake_global_entity_id',
            'parent_folder_geid': None,
        },
        'update_timestamp': '1643041439',
    }
    monkeypatch.setattr(SrvRedisSingleton, 'mget_by_prefix', lambda x, y: [bytes(json.dumps(fake_job), 'utf-8')])
