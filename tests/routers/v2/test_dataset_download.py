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

async def test_v2_dataset_download_should_return_401_when_wrong_token(
    client,
):
    resp = await client.get(
        '/v2/dataset/download/bad_token',
    )
    assert resp.status_code == 401
    assert resp.json() == {'code': 401, 'error_msg': 'n', 'page': 0, 'total': 1, 'num_of_pages': 1, 'result': []}


async def test_v2_dataset_download_should_return_200_when_success(client, jwt_token, mock_minio):
    resp = await client.get(
        f'/v2/dataset/download/{jwt_token}',
    )
    assert resp.status_code == 200
    assert resp.text == 'File like object'


async def test_v2_dataset_download_should_return_200_when_minio_raise_error(client, jwt_token, httpx_mock):
    resp = await client.get(
        f'/v2/dataset/download/{jwt_token}',
    )
    assert resp.status_code == 200
    assert 'error_msg' in resp.json()
    assert 'Error getting file from minio' in resp.json()['error_msg']
