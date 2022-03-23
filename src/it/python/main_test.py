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
    params = {'submitter_id': 'jdr0887@gmail.com', 'accession_id': 'asdf', 'apikey': 'apikey|5d2f826c452af1849b3f106630fef50a'}
    r = requests.post(url=url, params=params, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None
    assert response_json["object_id"] is not None


def test_search():
    url = f"http://localhost:{os.getenv('API_PORT')}/search/jdr0887@gmail.com"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None
    first = response_json[0]
    assert first["immunespace_download_id"] is not None and first["email"] == "jdr0887@gmail.com"
    return first["immunespace_download_id"]


def test_objects_get():
    content_files = ["geneBySampleMatrix", "phenoDataMatrix"]
    immunespace_download_id = test_search()
    url = f"http://localhost:{os.getenv('API_PORT')}/objects/{immunespace_download_id}"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None
    assert response_json["id"] == f"{immunespace_download_id}"
    assert response_json["contents"] is not None
    assert content_files.__contains__(response_json["contents"][0]["id"])


def test_download_file():
    immunespace_download_id = test_search()
    url = f"http://localhost:{os.getenv('API_PORT')}/files/{immunespace_download_id}/geneBySampleMatrix"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    content_type = r.headers.get('content-type')
    assert status_code == 200 and content_type.__contains__("text/csv")


def test_delete():
    immunespace_download_id = test_search()
    url = f"http://localhost:{os.getenv('API_PORT')}/delete/{immunespace_download_id}"
    r = requests.delete(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None
    assert response_json["status"] == "deleted"
