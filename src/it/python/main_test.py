import os

import requests
import time


def test_service_info():
    url = f"http://localhost:{os.getenv('API_PORT')}/service-info"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None and response_json["id"] == "fuse-provider-immunespace"


def test_submit():
    url = f"http://localhost:{os.getenv('API_PORT')}/submit"
    params = {'email': 'jdr0887@gmail.com', 'group': 'asdf', 'apikey': 'apikey|5d2f826c452af1849b3f106630fef50a'}
    r = requests.post(url=url, params=params, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json["immunespace_download_id"] is not None
    time.sleep(15) # to allow the service to run


def test_search():
    url = f"http://localhost:{os.getenv('API_PORT')}/search/jdr0887@gmail.com"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None and response_json[0]["immunespace_download_id"] is not None and response_json[0]["email"] == "jdr0887@gmail.com"
    return response_json[0]["immunespace_download_id"]


def test_status():
    immunespace_download_id = test_search()
    url = f"http://localhost:{os.getenv('API_PORT')}/status/{immunespace_download_id}"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    response_json = r.json()
    assert status_code == 200 and response_json is not None and response_json["status"] != "failed"


def test_download_files():
    immunespace_download_id = test_search()
    url = f"http://localhost:{os.getenv('API_PORT')}/files/{immunespace_download_id}"
    r = requests.get(url=url, headers={'accept': 'application/json'})
    status_code = r.status_code
    content_type = r.headers.get('content-type')
    assert status_code == 200 and content_type == "application/zip"


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
    assert status_code == 200 and response_json["status"] == "deleted"
