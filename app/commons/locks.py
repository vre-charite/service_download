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

import httpx

from app.config import ConfigClass
from app.resources.helpers import get_children_nodes
from app.resources.helpers import get_resource_bygeid


def lock_resource(resource_key: str, operation: str) -> dict:
    # operation can be either read or write
    url = ConfigClass.DATA_OPS_UT_V2 + 'resource/lock/'
    post_json = {'resource_key': resource_key, 'operation': operation}
    with httpx.Client() as client:
        response = client.post(url, json=post_json)
    if response.status_code != 200:
        raise Exception('resource %s already in used' % resource_key)

    return response.json()


def unlock_resource(resource_key: str, operation: str) -> dict:
    # operation can be either read or write
    url = ConfigClass.DATA_OPS_UT_V2 + 'resource/lock/'
    post_json = {'resource_key': resource_key, 'operation': operation}
    with httpx.Client() as client:
        response = client.request(url=url, method='DELETE', json=post_json)
    if response.status_code != 200:
        raise Exception('Error when unlock resource %s' % resource_key)

    return response.json()


def recursive_lock(code: str, ff_geids: list, new_name: str = None) -> (list, Exception):
    """the function will recursively lock the node tree."""

    # this is for crash recovery, if something trigger the exception
    # we will unlock the locked node only. NOT the whole tree. The example
    # case will be copy the same node, if we unlock the whole tree in exception
    # then it will affect the processing one.
    locked_node, err = [], None

    def recur_walker(currenct_nodes, new_name=None):
        """recursively trace down the node tree and run the lock function."""
        for ff_object in currenct_nodes:
            # we will skip the deleted nodes
            if ff_object.get('archived', False):
                continue
            # conner case here, we DONT lock the name folder
            # for the copy we will lock the both source and target
            if ff_object.get('display_path') != ff_object.get('uploader'):
                bucket_prefix = ''
                if ConfigClass.GREEN_ZONE_LABEL in ff_object.get('labels'):
                    bucket_prefix = 'gr-'
                elif ConfigClass.CORE_ZONE_LABEL in ff_object.get('labels'):
                    bucket_prefix = 'core-'
                bucket = bucket_prefix + code
                minio_obj_path = ff_object.get('display_path')
                # note here the dataset and project are using same class
                # two file have different attribte so here I just use `location``
                # if not minio_obj_path:
                #     if "File" in ff_object.get('labels'):
                #         minio_path = ff_object.get('location').split("//")[-1]
                #         _, bucket, minio_obj_path = tuple(minio_path.split("/", 2))
                #     else:
                #         bucket = ff_object.get('dataset_code')
                #         minio_obj_path = "%s/%s"%(ff_object.get('folder_relative_path'),
                #             ff_object.get('name'))

                source_key = '{}/{}'.format(bucket, minio_obj_path)
                lock_resource(source_key, 'read')
                locked_node.append((source_key, 'read'))

            # open the next recursive loop if it is folder
            if 'Folder' in ff_object.get('labels'):
                children_nodes = get_children_nodes(ff_object.get('global_entity_id', None))
                recur_walker(children_nodes)

        return

    # start here
    try:
        # slightly different here, since the download only gives
        # the folder/file geid. then I have to get node by geid so
        # that we can get the path/
        nodes = [get_resource_bygeid(geid.get('geid')) for geid in ff_geids]
        recur_walker(nodes, new_name)
    except Exception as e:
        err = e

    return locked_node, err
