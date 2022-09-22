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

from typing import Any
from typing import Dict

from common import VaultClient
from pydantic import BaseSettings
from pydantic import Extra
from starlette.config import Config

config = Config('.env')

SRV_NAMESPACE = config('APP_NAME', cast=str, default='service_download')
CONFIG_CENTER_ENABLED = config('CONFIG_CENTER_ENABLED', cast=str, default='false')
CONFIG_CENTER_BASE_URL = config('CONFIG_CENTER_BASE_URL', cast=str, default='NOT_SET')


def load_vault_settings(settings: BaseSettings) -> Dict[str, Any]:
    if CONFIG_CENTER_ENABLED == 'false':
        return {}
    else:
        vc = VaultClient(config('VAULT_URL'), config('VAULT_CRT'), config('VAULT_TOKEN'))
        return vc.get_from_vault(SRV_NAMESPACE)


class Settings(BaseSettings):
    """Store service configuration settings."""

    APP_NAME: str = 'service_download'
    VERSION: str = '0.1.0'
    port: int = 5077
    host: str = '127.0.0.1'
    env: str = 'test'
    namespace: str = ''

    # disk mounts
    ROOT_PATH: str

    CORE_ZONE_LABEL: str
    GREEN_ZONE_LABEL: str

    # services
    NEO4J_SERVICE: str
    PROVENANCE_SERVICE: str
    QUEUE_SERVICE: str
    UTILITY_SERVICE: str
    DATA_OPS_UTIL: str
    DATASET_SERVICE: str

    # minio
    MINIO_OPENID_CLIENT: str
    MINIO_ENDPOINT: str
    MINIO_HTTPS: str
    KEYCLOAK_URL: str
    MINIO_TEST_PASS: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    KEYCLOAK_MINIO_SECRET: str

    # download secret
    DOWNLOAD_KEY: str = 'indoc101'
    DOWNLOAD_TOKEN_EXPIRE_AT: int = 86400
    # Redis Service
    REDIS_HOST: str
    REDIS_PORT: str
    REDIS_DB: str
    REDIS_PASSWORD: str

    # Postgres
    RDS_HOST: str
    RDS_PORT: str
    RDS_USER: str
    RDS_PWD: str
    RDS_DBNAME: str
    RDS_SCHEMA_DEFAULT: str

    OPEN_TELEMETRY_ENABLED: bool = False
    OPEN_TELEMETRY_HOST: str = '127.0.0.1'
    OPEN_TELEMETRY_PORT: int = 6831

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        extra = Extra.allow

        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return env_settings, load_vault_settings, init_settings, file_secret_settings

    def __init__(self) -> None:
        super().__init__()

        # services
        self.NEO4J_SERVICE_V2 = self.NEO4J_SERVICE + '/v2/neo4j/'
        self.NEO4J_SERVICE += '/v1/neo4j/'
        self.PROVENANCE_SERVICE += '/v1/'
        self.QUEUE_SERVICE += '/v1/'
        # self.COMMON_SERVICE += '/v1/'
        self.DATA_OPS_UT_V2 = self.DATA_OPS_UTIL + '/v2/'
        self.DATA_OPS_UTIL += '/v1/'
        self.DATASET_SERVICE += '/v1/'

        # minio
        self.MINIO_TMP_PATH = self.ROOT_PATH + '/tmp/'
        self.MINIO_HTTPS = self.MINIO_HTTPS == 'TRUE'  # the vault is storing the string

        # Redis Service
        self.REDIS_PORT = int(self.REDIS_PORT)
        self.REDIS_DB = int(self.REDIS_DB)


ConfigClass = Settings()
