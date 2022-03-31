import os
import time

import requests


def test_service_info():
    url = f"http://localhost:{os.getenv('API_PORT')}/service-info"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None
    assert response_json["id"] == "fuse-provider-immunespace"


def test_submit():
    url = f"http://localhost:{os.getenv('API_PORT')}/submit"
    params = {'service_id': 'fuse-provider-immunespace',
              'submitter_id': 'jdr0887@gmail.com',
              'data_type': 'class_dataset_expression',
              'file_type': 'filetype_dataset_expression',
              'accession_id': 'asdf',
              'apikey': 'apikey|5d2f826c452af1849b3f106630fef50a'}
    r = requests.post(url=url, data=params, headers={'accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'}, timeout=45)
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None
    #print(f"response_json: {response_json}")
    assert response_json["object_id"] is not None
    #print(f"object_id: {response_json['object_id']}")


def test_search():
    url = f"http://localhost:{os.getenv('API_PORT')}/search/jdr0887@gmail.com"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None
    first = response_json[0]
    assert first["immunespace_download_id"] is not None and first["submitter_id"] == "jdr0887@gmail.com"
    return first["object_id"]


def test_objects_get():
    object_id = test_search()
    url = f"http://localhost:{os.getenv('API_PORT')}/objects/{object_id}"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None
    assert response_json["id"] == f"{object_id}"
    assert response_json["contents"] is not None
    assert ["geneBySampleMatrix.csv", "phenoDataMatrix.csv"].__contains__(response_json["contents"][0]["name"])


def test_download_file():
    object_id = test_search()
    url = f"http://localhost:{os.getenv('API_PORT')}/files/{object_id}"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    content_type = r.headers.get('content-type')
    assert status_code == 200 and content_type.__contains__("text/csv")


def test_delete():
    object_id = test_search()
    url = f"http://localhost:{os.getenv('API_PORT')}/delete/{object_id}"
    r = requests.delete(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None
    assert response_json["status"] == "deleted"
