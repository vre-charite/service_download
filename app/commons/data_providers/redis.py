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

from enum import Enum

from redis import StrictRedis

from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.config import ConfigClass

_logger = SrvLoggerFactory('SrvRedisSingleton').get_logger()


class SrvRedisSingleton:

    __instance = {}

    def __init__(self):
        self.host = ConfigClass.REDIS_HOST
        self.port = ConfigClass.REDIS_PORT
        self.db = ConfigClass.REDIS_DB
        self.pwd = ConfigClass.REDIS_PASSWORD
        self.connect()

    def connect(self):
        if self.__instance:
            pass
        else:
            self.__instance = StrictRedis(host=self.host, port=self.port, db=self.db, password=self.pwd)

    def get_by_key(self, key: str):
        return self.__instance.get(key)

    def set_by_key(self, key: str, content: str):
        self.__instance.set(key, content)
        _logger.debug('redis set by key: ' + key + ':  ' + content)

    def mget_by_prefix(self, prefix: str):
        _logger.debug(prefix)
        query = '{}:*'.format(prefix)
        keys = self.__instance.keys(query)
        return self.__instance.mget(keys)

    def check_by_key(self, key: str):
        return self.__instance.exists(key)

    def delete_by_key(self, key: str):
        return self.__instance.delete(key)

    def mdelete_by_prefix(self, prefix: str):
        _logger.debug(prefix)
        query = '{}:*'.format(prefix)
        keys = self.__instance.keys(query)
        for key in keys:
            self.__instance.delete(key)

    def get_by_pattern(self, key: str, pattern: str):
        query_string = '{}:*{}*'.format(key, pattern)
        keys = self.__instance.keys(query_string)
        return self.__instance.mget(keys)

    def publish(self, channel, data):
        res = self.__instance.publish(channel, data)
        return res

    def subscriber(self, channel):
        p = self.__instance.pubsub()
        p.subscribe(channel)
        return p


class ERedisChannels(Enum):
    pipeline_process_start = 0
