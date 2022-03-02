import datetime
import json
import logging
import os
import pathlib
import shutil
import traceback
import uuid
import zipfile
from multiprocessing import Process

import docker
import pymongo
from fastapi import FastAPI, Depends, Path, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from redis import Redis
from rq import Queue, Worker
from rq.job import Job
from starlette.responses import StreamingResponse

from fuse.models.Objects import Passports, ImmunespaceGA4GHDRSResponse, Contents

app = FastAPI()

logger = logging.getLogger(name="api")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(console_handler)

origins = [
    f"http://{os.getenv('HOSTNAME')}:{os.getenv('HOSTPORT')}",
    f"http://{os.getenv('HOSTNAME')}",
    f"http://localhost:{os.getenv('HOSTPORT')}",
    "http://localhost",
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_connection = Redis(host='immunespace-redis', port=6379, db=0)
q = Queue(connection=redis_connection, is_async=True, default_timeout=3600)

mongo_user_name = os.getenv('MONGO_NON_ROOT_USERNAME')
mongo_password = os.getenv('MONGO_NON_ROOT_PASSWORD')
mongo_database_name = os.getenv('MONGO_INITDB_DATABASE')
mongo_database_connection_url = f"mongodb://{mongo_user_name}:{mongo_password}@immunespace-mongodb:27017/{mongo_database_name}"
# logger.info(mongo_database_connection_url)
mongo_client = pymongo.MongoClient(mongo_database_connection_url)
mongo_db = mongo_client[mongo_database_name]
mongo_db_immunespace_downloads_column = mongo_db["immunespace_downloads"]

docker_client = docker.from_env()


def init_worker():
    worker = Worker(q, connection=redis_connection)
    worker.work()


@app.get("/service-info", summary="Retrieve information about this service")
async def service_info():
    '''
    Returns information about the DRS service

    Extends the v1.0.0 GA4GH Service Info specification as the standardized format for GA4GH web services to self-describe.

    According to the service-info type registry maintained by the Technical Alignment Sub Committee (TASC), a DRS service MUST have:
    - a type.group value of org.ga4gh
    - a type.artifact value of drs

    e.g.
    ```
    {
      "id": "com.example.drs",
      "description": "Serves data according to DRS specification",
      ...
      "type": {
        "group": "org.ga4gh",
        "artifact": "drs"
      }
    ...
    }
    ```
    See the Service Registry Appendix for more information on how to register a DRS service with a service registry.
    '''
    service_info_path = pathlib.Path(__file__).parent.parent / "resources" / "service_info.json"
    with open(service_info_path) as f:
        return json.load(f)


# READ-ONLY endpoints follow the GA4GH DRS API, modeled below
# https://editor.swagger.io/?url=https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.2.0/openapi.yaml
@app.get("/objects/{object_id}", summary="Get info about a DrsObject.")
async def objects(object_id: str = Path(default="", description="DrsObject identifier"),
                  expand: bool = Query(default=False,
                                       description="If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains aother bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored.")):
    projection = {"_id": 0, "immunespace_download_id": 1, "email": 1, "group_id": 1, "apikey": 1, "status": 1, "date_created": 1, "start_date": 1, "end_date": 1}
    query = {"immunespace_download_id": object_id}
    found_immunespace_download = mongo_db_immunespace_downloads_column.find_one(query, projection)
    logger.info(f"{found_immunespace_download}")

    if found_immunespace_download is not None:
        content_files = ["geneBySampleMatrix", "phenoDataMatrix"]
        contents = list(map(lambda x: Contents(id=x, name=x, drs_uri=f"http://localhost:{os.getenv('API_PORT')}/data/{object_id}/{x}"), content_files))

        ret = ImmunespaceGA4GHDRSResponse(id=found_immunespace_download['immunespace_download_id'],
                                          name=found_immunespace_download['immunespace_download_id'],
                                          self_uri=f"http://localhost:{os.getenv('API_PORT')}/data/{found_immunespace_download['immunespace_download_id']}",
                                          created_time=f"{found_immunespace_download['date_created']}", mime_type="application/zip",
                                          contents=contents)
        return ret.__dict__
    else:
        return HTTPException(status_code=404, detail="Not found")


# xxx add value for passport example that doesn't cause server error
# xxx figure out how to add the following description to 'passports':
# the encoded JWT GA4GH Passport that contains embedded Visas. The overall JWT is signed as are the individual Passport Visas
@app.post("/objects/{object_id}", summary="Get info about a DrsObject through POST'ing a Passport.")
async def post_objects(object_id: str = Path(default="", description="DrsObject identifier"),
                       expand: bool = Query(default=False,
                                            description="If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains aother bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored."),
                       passports: Passports = Depends(Passports.as_form)):
    '''
    Returns object metadata, and a list of access methods that can be
    used to fetch object bytes. Method is a POST to accomodate a JWT
    GA4GH Passport sent in the formData in order to authorize access.
    '''

    ret = {
        "url": f"http://localhost:{os.getenv('API_PORT')}/data/{object_id}",
        "headers": "Authorization: None"
    }
    return ret


@app.get("/objects/{object_id}/access/{access_id}", summary="Get a URL for fetching bytes")
async def get_objects(object_id: str = Path(default="", description="DrsObject identifier"),
                      access_id: str = Path(default="", description="An access_id from the access_methods list of a DrsObject")):
    '''
    Returns a URL that can be used to fetch the bytes of a
    DrsObject. This method only needs to be called when using an
    AccessMethod that contains an access_id (e.g., for servers that
    use signed URLs for fetching object bytes).
    '''

    return {
        "url": "http://localhost/object.zip",
        "headers": "Authorization: None"
    }


# xxx figure out how to add the following description to 'passports':
# the encoded JWT GA4GH Passport that contains embedded Visas. The overall JWT is signed as are the individual Passport Visas.
@app.post("/objects/{object_id}/access/{access_id}", summary="Get a URL for fetching bytes through POST'ing a Passport")
async def post_objects(object_id: str = Path(default="", description="DrsObject identifier"),
                       access_id: str = Path(default="", description="An access_id from the access_methods list of a DrsObject"),
                       passports: Passports = Depends(Passports.as_form)):
    '''
    Returns a URL that can be used to fetch the bytes of a
    DrsObject. This method only needs to be called when using an
    AccessMethod that contains an access_id (e.g., for servers that
    use signed URLs for fetching object bytes). Method is a POST to
    accomodate a JWT GA4GH Passport sent in the formData in order to
    authorize access.

    '''
    return {
        "url": "http://localhost/object.zip",
        "headers": "Authorization: None"
    }


@app.get("/search/{email}")
async def search(email: str):
    query = {"email": email}
    projection = {"_id": 0, "immunespace_download_id": 1, "email": 1, "group_id": 1, "apikey": 1, "status": 1, "date_created": 1, "start_date": 1, "end_date": 1}
    # projection = {"_id": 0, "immunespace_download_id": 1}
    ret = list(map(lambda a: a, mongo_db_immunespace_downloads_column.find(query, projection)))
    if len(ret) > 0:
        return ret
    else:
        return HTTPException(status_code=404, detail="Not found")


@app.get("/status/{immunespace_download_id}")
def status(immunespace_download_id: str):
    try:
        job = Job.fetch(immunespace_download_id, connection=redis_connection)
        ret = {"status": job.get_status()}
        task_mapping_entry = {"immunespace_download_id": immunespace_download_id}
        new_values = {"$set": ret}
        mongo_db_immunespace_downloads_column.update_one(task_mapping_entry, new_values)
        return ret
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.post("/submit")
async def submit(email: str, group: str, apikey: str):
    # write data to memory
    immunespace_download_query = {"email": email, "group_id": group, "apikey": apikey}
    projection = {"_id": 0, "immunespace_download_id": 1, "email": 1, "group_id": 1, "apikey": 1, "status": 1, "date_created": 1, "start_date": 1, "end_date": 1}
    entry = mongo_db_immunespace_downloads_column.find(immunespace_download_query, projection)

    local_path = os.path.join("/app/data")

    if entry.count() > 0:
        immunespace_download_id = entry.next()["immunespace_download_id"]

        local_path = os.path.join(local_path, f"{immunespace_download_id}-immunespace-data")
        if os.path.exists(local_path) and len(os.listdir(local_path)) == 0:
            q.enqueue(run_immunespace_download, immunespace_download_id=immunespace_download_id, group=group, apikey=apikey, job_id=immunespace_download_id, job_timeout=3600,
                      result_ttl=-1)
            p_worker = Process(target=init_worker)
            p_worker.start()

        return {"immunespace_download_id": immunespace_download_id}
    else:
        immunespace_download_id = str(uuid.uuid4())[:8]

        task_mapping_entry = {"immunespace_download_id": immunespace_download_id, "email": email, "group_id": group, "apikey": apikey, "status": None,
                              "date_created": datetime.datetime.utcnow(), "start_date": None, "end_date": None}
        mongo_db_immunespace_downloads_column.insert_one(task_mapping_entry)

        local_path = os.path.join(local_path, f"{immunespace_download_id}-immunespace-data")
        os.mkdir(local_path)

        # instantiate task
        q.enqueue(run_immunespace_download, immunespace_download_id=immunespace_download_id, group=group, apikey=apikey, job_id=immunespace_download_id, job_timeout=3600,
                  result_ttl=-1)
        p_worker = Process(target=init_worker)
        p_worker.start()
        return {"immunespace_download_id": immunespace_download_id}


def run_immunespace_download(immunespace_download_id: str, group: str, apikey: str):
    local_path = os.getenv('HOST_ABSOLUTE_PATH')

    job = Job.fetch(immunespace_download_id, connection=redis_connection)
    task_mapping_entry = {"immunespace_download_id": immunespace_download_id}
    new_values = {"$set": {"start_date": datetime.datetime.utcnow(), "status": job.get_status()}}
    mongo_db_immunespace_downloads_column.update_one(task_mapping_entry, new_values)

    image = "txscience/tx-immunespace-groups:0.3"
    volumes = {os.path.join(local_path, f"data/{immunespace_download_id}-immunespace-data"): {'bind': '/data', 'mode': 'rw'}}
    command = f"-g \"{group}\" -a \"{apikey}\" -o /data"
    immunespace_groups_container_logs = docker_client.containers.run(image, volumes=volumes, name=f"{immunespace_download_id}-immunespace-groups", working_dir="/data",
                                                                     privileged=True, remove=True, command=command)
    logger.info(msg=immunespace_groups_container_logs)
    logger.info(msg=f"finished txscience/tx-immunespace-groups:0.3")

    image = "txscience/fuse-mapper-immunespace:0.1"
    volumes = {os.path.join(local_path, f"data/{immunespace_download_id}-immunespace-data"): {'bind': '/data', 'mode': 'rw'}}
    command = f"-g /data/geneBySampleMatrix.csv -p /data/phenoDataMatrix.csv"
    mapper_container_logs = docker_client.containers.run(image, volumes=volumes, name=f"{immunespace_download_id}-immunespace-mapper", working_dir="/data", privileged=True,
                                                         remove=True, command=command)
    logger.info(msg=mapper_container_logs)
    logger.info(msg=f"finished fuse-mapper-immunespace:0.1")

    new_values = {"$set": {"end_date": datetime.datetime.utcnow(), "status": job.get_status()}}
    mongo_db_immunespace_downloads_column.update_one(task_mapping_entry, new_values)


@app.get("/files/{immunespace_download_id}")
def files(immunespace_download_id: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "/app/data")
    dir_path = os.path.join(local_path, f"{immunespace_download_id}-immunespace-data")
    zip_file = os.path.join(dir_path, "data.zip")
    zipf = zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED)
    zipf.write(os.path.join(dir_path, "geneBySampleMatrix.csv"), "geneBySampleMatrix.csv")
    zipf.write(os.path.join(dir_path, "phenoDataMatrix.csv"), "phenoDataMatrix.csv")
    zipf.close()
    if not os.path.isdir(dir_path) or not os.path.exists(zip_file):
        raise HTTPException(status_code=404, detail="Not found")

    def iterfile():
        try:
            with open(zip_file, mode="rb") as file_data:
                yield from file_data
        except:
            raise Exception()

    response = StreamingResponse(iterfile(), media_type="application/zip")
    response.headers["Content-Disposition"] = "attachment; filename=data.zip"
    return response


@app.get("/files/{immunespace_download_id}/{file_name}")
def files(immunespace_download_id: str, file_name: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "/app/data")
    dir_path = os.path.join(local_path, f"{immunespace_download_id}-immunespace-data")
    file_path = os.path.join(dir_path, f"{file_name}.csv")
    if not os.path.isdir(dir_path) or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Not found")

    def iterfile():
        try:
            with open(file_path, mode="rb") as file_data:
                yield from file_data
        except:
            raise Exception()

    response = StreamingResponse(iterfile(), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename={file_name}.csv"
    return response


@app.delete("/delete/{immunespace_download_id}", summary="DANGER ZONE: Delete a downloaded object; this action is rarely justified.")
async def immunespace_download_delete(immunespace_download_id: str):
    '''
    Delete cached data from the remote provider, identified by the provided download_id.
    <br>**WARNING**: This will orphan associated analyses; only delete downloads if:
    - the data are redacted.
    - the system state needs to be reset, e.g., after testing.
    - the sytem state needs to be corrected, e.g., after a bugfix.

    <br>**Note**: If the object was changed on the data provider's server, the old copy should be versioned in order to keep an appropriate record of the input data for past dependent analyses.
    <br>**Note**: Object will be deleted from disk regardless of whether or not it was found in the database. This can be useful for manual correction of erroneous system states.
    <br>**Returns**:
    - status = 'deleted' if object is found in the database and 1 object successfully deleted.
    - status = 'exception' if an exception is encountered while removing the object from the database or filesystem, regardless of whether or not the object was successfully deleted, see other returned fields for more information.
    - status = 'failed' if 0 or greater than 1 object is not found in database.
    '''
    delete_status = "done"

    # Delete may be requested while the download job is enqueued, so check that first:
    ret_job = ""
    ret_job_err = ""
    try:
        job = Job.fetch(immunespace_download_id, connection=redis_connection)
        if job is None:
            ret_job = "No job found in queue.\n"
        else:
            job.delete(delete_dependents=True, remove_from_queue=True)
    except Exception as e:
        # job is not expected to be on queue so don't change deleted_status from "done"
        ret_job_err += f"! Exception {type(e)} occurred while deleting job from redis queue, message=[{e}] \n! traceback=\n{traceback.format_exc()}\n"
        delete_status = "exception"

    # Assuming the job already executed, remove any database records
    ret_mongo = ""
    ret_mongo_err = ""
    try:
        task_query = {"immunespace_download_id": immunespace_download_id}
        ret = mongo_db_immunespace_downloads_column.delete_one(task_query)
        # <class 'pymongo.results.DeleteResult'>
        delete_status = "deleted"
        if not ret.acknowledged:
            delete_status = "failed"
            ret_mongo += "ret.acknowledged not True.\n"
        if ret.deleted_count != 1:
            # should never happen if index was created for this field
            delete_status = "failed"
            ret_mongo += f"Wrong number of records deleted ({str(ret.deleted_count)}).\n"
        ## xxx
        # could check if there are any remaining; but this should instead be enforced by creating an index for this columnxs
        # could check ret.raw_result['n'] and ['ok'], but 'ok' seems to always be 1.0, and 'n' is the same as deleted_count
        ##
        ret_mongo += f"Deleted count=({str(ret.deleted_count)}), Acknowledged=({str(ret.acknowledged)}).\n"
    except Exception as e:
        ret_mongo_err += f"! Exception {type(e)} occurred while deleting job from database, message=[{e}] \n! traceback=\n{traceback.format_exc()}\n"
        delete_status = "exception"

    # Data are cached on a mounted filesystem, unlink that too if it's there
    ret_os = ""
    ret_os_err = ""
    try:
        local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "/app/data")
        local_path = os.path.join(local_path, f"{immunespace_download_id}-immunespace-data")
        shutil.rmtree(local_path, ignore_errors=False)
    except Exception as e:
        ret_os_err += f"! Exception {type(e)} occurred while deleting job from filesystem, message=[{e}] \n! traceback=\n{traceback.format_exc()}\n"
        delete_status = "exception"

    ret_message = ret_job + ret_mongo + ret_os
    ret_err_message = ret_job_err + ret_mongo_err + ret_os_err
    return {
        "status": delete_status,
        "info": ret_message,
        "stderr": ret_err_message,
    }
