import os


class ConfigClass(object):
    env = os.environ.get('env')
    # env = os.environ.get('env')
    disk_namespace = os.environ.get('namespace')

    version = "0.1.0"
    NEO4J_SERVICE = "http://neo4j.utility:5062/v1/neo4j/"
    NEO4J_SERVICE_V2 = "http://neo4j.utility:5062/v2/neo4j/"
    DATA_OPS_GR = "http://dataops-gr.greenroom:5063/v1/"
    PROVENANCE_SERVICE = "http://provenance.utility:5077/v1/"

    MINIO_OPENID_CLIENT = "react-app"
    if env == "staging":
        # MINIO_ENDPOINT = "10.3.7.240:80"
        MINIO_ENDPOINT = "vre-staging-minio.indocresearch.org"
        MINIO_HTTPS = False
        KEYCLOAK_URL = "http://10.3.7.240:80"
        MINIO_TEST_PASS = "Trillian42!"
    else:
        MINIO_ENDPOINT = "10.3.7.220"
        MINIO_HTTPS = False
        KEYCLOAK_URL = "http://keycloak.utility:8080"
        # KEYCLOAK_URL = "http://10.3.7.220" # for local test ONLY
        MINIO_TEST_PASS = "admin"

    # disk mounts
    NFS_ROOT_PATH = "./"
    VRE_ROOT_PATH = "/vre-data"
    ROOT_PATH = {
        "vre": "/vre-data"
    }.get(os.environ.get('namespace'), "/data/vre-storage")

    # download secret
    DOWNLOAD_KEY = "indoc101"
    DOWNLOAD_TOKEN_EXPIRE_AT = 5

    # Redis Service
    # REDIS_HOST = "10.3.7.233"
    REDIS_HOST = "redis-master.utility"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = {
        'staging': '8EH6QmEYJN',
        'charite': 'o2x7vGQx6m'
    }.get(env, "5wCCMMC1Lk")
