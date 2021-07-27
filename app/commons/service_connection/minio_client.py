import requests
import xmltodict
from minio import Minio
from minio.commonconfig import Tags
import os
import time
import datetime
from ...config import ConfigClass

from minio.credentials.providers import ClientGrantsProvider


def get_temp_credentials():
    # first login with keycloak
    username = "admin"
    password = ConfigClass.MINIO_TEST_PASS
    payload = {
        "grant_type":"password",
        "username":username,
        "password":password, 
        "client_id":ConfigClass.MINIO_OPENID_CLIENT,
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }  

    print(ConfigClass.KEYCLOAK_URL)

    result = requests.post(ConfigClass.KEYCLOAK_URL+"/vre/auth/realms/vre/protocol/openid-connect/token", data=payload, headers=headers)
    keycloak_access_token = result.json().get("access_token")
    expire_time = result.json().get("expires_in")
    print(result.json())


    # next login with minio
    minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + ConfigClass.MINIO_ENDPOINT
    minio_login_url = minio_http+"?Action=AssumeRoleWithClientGrants&DurationSeconds=900&Token=%s&Version=2011-06-15"%(keycloak_access_token)
    result = requests.post(minio_login_url)
    print(result.__dict__)
    # note here the response from minio is xml. Parse it into json
    result = xmltodict.parse(result.content.decode("utf-8"))


    credential_detail = result["AssumeRoleWithClientGrantsResponse"]["AssumeRoleWithClientGrantsResult"]["Credentials"]

    return credential_detail


class Minio_Client():

    def __init__(self):
        # retrieve credential provide with tokens
        # c = self.get_provider()

        # self.client = Minio(
        #     ConfigClass.MINIO_ENDPOINT, 
        #     credentials=c,
        #     secure=ConfigClass.MINIO_HTTPS)

        # Temperary use the credential
        self.client = Minio(
            ConfigClass.MINIO_ENDPOINT, 
            access_key=ConfigClass.MINIO_ACCESS_KEY,
            secret_key=ConfigClass.MINIO_SECRET_KEY,
            secure=ConfigClass.MINIO_HTTPS)


    def _get_jwt(self):
        # first login with keycloak
        username = "admin"
        password = ConfigClass.MINIO_TEST_PASS
        payload = {
            "grant_type":"password",
            "username":username,
            "password":password, 
            "client_id":ConfigClass.MINIO_OPENID_CLIENT,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        result = requests.post(ConfigClass.KEYCLOAK_URL+"/vre/auth/realms/vre/protocol/openid-connect/token", data=payload, headers=headers)
        keycloak_access_token = result.json().get("access_token")
        return result.json()

    def get_provider(self):
        minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + ConfigClass.MINIO_ENDPOINT
        provider = ClientGrantsProvider(
            self._get_jwt,
            minio_http,
        )

        return provider.retrieve()