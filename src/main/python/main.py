import datetime
import json
import logging
import os
import pathlib
import shutil
import traceback
import uuid
from logging.config import dictConfig

import docker
import pymongo
from fastapi import FastAPI, Depends, Path, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fuse_utilities.main import ProviderParameters, FileType, DataType
from starlette.responses import StreamingResponse

# https://developer.mozilla.org/en-US/docs/Web/API/WritableStream
from fuse.models.Objects import Contents, ImmunespaceProviderResponse, ProviderResponse

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '%(asctime)s | %(levelname)s | %(module)s:%(funcName)s | %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'default'
        },
    },
    'loggers': {
        "fuse-provider-immunespace": {"handlers": ["console"], "level": os.getenv("LOG_LEVEL")},
    }
}

dictConfig(LOGGING)
logger = logging.getLogger("fuse-provider-immunespace")

g_api_version = "0.0.1"

app = FastAPI(openapi_url=f"/api/{g_api_version}/openapi.json",
              title="Immunespace Provider",
              version=g_api_version,
              terms_of_service="https://github.com/RENCI/fuse-agent/doc/terms.pdf",
              contact={
                  "url": "http://txscience.renci.org/contact/",
              },
              license_info={
                  "name": "MIT License",
                  "url": "https://github.com/RENCI/fuse-agent/blob/main/LICENSE"
              }
              )

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

mongo_user_name = os.getenv('MONGO_NON_ROOT_USERNAME')
mongo_password = os.getenv('MONGO_NON_ROOT_PASSWORD')
mongo_database_name = os.getenv('MONGO_INITDB_DATABASE')
mongo_database_connection_url = f"mongodb://{mongo_user_name}:{mongo_password}@immunespace-mongodb:27017/{mongo_database_name}"
# logger.info(mongo_database_connection_url)
mongo_client = pymongo.MongoClient(mongo_database_connection_url)
mongo_db = mongo_client[mongo_database_name]
mongo_db_immunespace_downloads_column = mongo_db["immunespace_downloads"]

docker_client = docker.from_env()


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
    projection = {"_id": 0, "immunespace_download_id": 1, "object_id": 1, "submitter_id": 1, "accession_id": 1, "apikey": 1, "status": 1, "data_type": 1,
                  "file_type": 1, "file_name": 1, "stderr": 1, "date_downloaded": 1}
    query = {"object_id": object_id}
    found_immunespace_download = mongo_db_immunespace_downloads_column.find_one(query, projection)
    if found_immunespace_download is not None:
        logger.info(f"{found_immunespace_download}")
        file_size = os.path.getsize(f"/app/data/{found_immunespace_download['immunespace_download_id']}/{found_immunespace_download['file_name']}")
        contents = Contents(id=found_immunespace_download["object_id"], name=found_immunespace_download["file_name"],
                            drs_uri=f"http://fuse-provider-immunespace:{os.getenv('API_PORT')}/files/{object_id}")
        # contents.append(Contents(id="archive", name="archive", drs_uri=f"http://localhost:{os.getenv('API_PORT')}/archive/{object_id}"))
        ret = ImmunespaceProviderResponse(id=found_immunespace_download["object_id"],
                                          object_id=found_immunespace_download["object_id"],
                                          submitter_id=found_immunespace_download["submitter_id"],
                                          name=found_immunespace_download['immunespace_download_id'],
                                          self_uri=f"http://fuse-provider-immunespace:{os.getenv('API_PORT')}/objects/{found_immunespace_download['object_id']}",
                                          size=file_size,
                                          data_type=found_immunespace_download["data_type"],
                                          file_type=found_immunespace_download["file_type"],
                                          created_time=f"{found_immunespace_download['date_downloaded']}",
                                          mime_type="application/csv",
                                          status="finished",
                                          contents=[contents], stderr=found_immunespace_download['stderr'])

        return vars(ret)
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

    # ret = {
    #     "url": f"http://localhost:{os.getenv('API_PORT')}/data/{object_id}",
    #     "headers": "Authorization: None"
    # }
    # return ret
    return HTTPException(status_code=501, detail="Not implemented")


@app.get("/objects/{object_id}/access/{access_id}", summary="Get a URL for fetching bytes")
async def get_objects(object_id: str = Path(default="", description="DrsObject identifier"),
                      access_id: str = Path(default="", description="An access_id from the access_methods list of a DrsObject")):
    '''
    Returns a URL that can be used to fetch the bytes of a
    DrsObject. This method only needs to be called when using an
    AccessMethod that contains an access_id (e.g., for servers that
    use signed URLs for fetching object bytes).
    '''

    # return {
    #     "url": "http://localhost/object.zip",
    #     "headers": "Authorization: None"
    # }
    return HTTPException(status_code=501, detail="Not implemented")


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
    # return {
    #     "url": "http://localhost/object.zip",
    #     "headers": "Authorization: None"
    # }
    return HTTPException(status_code=501, detail="Not implemented")


@app.get("/search/{submitter_id}")
async def search(submitter_id: str):
    query = {"submitter_id": submitter_id}
    projection = {"_id": 0, "immunespace_download_id": 1, "object_id": 1, "submitter_id": 1, "accession_id": 1, "apikey": 1, "file_name": 1, "date_downloaded": 1}
    ret = list(map(lambda a: a, mongo_db_immunespace_downloads_column.find(query, projection)))
    if len(ret) > 0:
        return ret
    else:
        return HTTPException(status_code=404, detail="Not found")


@app.post("/submit")
async def submit(parameters: ProviderParameters = Depends(ProviderParameters.as_form)):
    logger.info(f"parameters: {parameters}")
    try:

        immunespace_download_query = {"submitter_id": parameters.submitter_id, "accession_id": parameters.accession_id,
                                      "apikey": parameters.apikey, "file_type": parameters.file_type}
        projection = {"_id": 0, "immunespace_download_id": 1, "object_id": 1, "file_type": 1}
        found_immunespace_download = mongo_db_immunespace_downloads_column.find_one(immunespace_download_query, projection)
        if found_immunespace_download is not None:
            logger.info(f"found_immunespace_download: {found_immunespace_download}")
            immunespace_download_id = found_immunespace_download["immunespace_download_id"]
            local_path = os.path.abspath(f"/app/data/{immunespace_download_id}")
            logger.debug(f"local_path: {local_path}")
            if os.path.exists(local_path) and len(os.listdir(local_path)) == 0:
                logger.debug(f"path exists, but is empty")
                run_immunespace_download(immunespace_download_id=immunespace_download_id, accession_id=parameters.accession_id, apikey=parameters.apikey)
        else:
            immunespace_download_id = str(uuid.uuid4())[:8]
            local_path = os.path.join(f"/app/data/{immunespace_download_id}")
            logger.info(f"local_path: {local_path}")
            os.makedirs(local_path, exist_ok=True)
            stderr = run_immunespace_download(immunespace_download_id=immunespace_download_id, accession_id=parameters.accession_id, apikey=parameters.apikey)
            for (file_type, file_name) in [(FileType.datasetGeneExpression, "geneBySampleMatrix.csv"), (FileType.datasetProperties, "phenoDataMatrix.csv")]:
                file_path = os.path.join(local_path, file_name)
                with open(file_path) as f:
                    number_of_columns = len(f.readline().rstrip().split(sep=",")) - 1
                f.close()

                with open(file_path) as f:
                    number_of_rows = len(f.readlines())
                f.close()

                size = os.path.getsize(file_path)
                dimensions = f"{number_of_rows}x{number_of_columns}"

                immunespace_download_entry = {"immunespace_download_id": immunespace_download_id, "submitter_id": parameters.submitter_id,
                                              "data_type": DataType.geneExpression, "object_id": str(uuid.uuid4()), "accession_id": parameters.accession_id,
                                              "apikey": parameters.apikey, "file_type": file_type, "file_name": file_name,
                                              "date_downloaded": datetime.datetime.utcnow(), "size": size, "dimensions": dimensions, "stderr": stderr}
                mongo_db_immunespace_downloads_column.insert_one(immunespace_download_entry)

        immunespace_download_query = {"submitter_id": parameters.submitter_id, "accession_id": parameters.accession_id,
                                      "apikey": parameters.apikey, "file_type": parameters.file_type}

        projection = {"_id": 0, "immunespace_download_id": 1, "object_id": 1, "submitter_id": 1, "accession_id": 1, "apikey": 1, "status": 1, "data_type": 1,
                      "file_type": 1, "file_name": 1, "size": 1, "dimensions": 1, "stderr": 1, "date_downloaded": 1}
        found_immunespace_download = mongo_db_immunespace_downloads_column.find_one(immunespace_download_query, projection)

        # contents = Contents(id=found_immunespace_download["object_id"], name=found_immunespace_download["file_name"],
        #                     drs_uri=f"http://localhost:{os.getenv('API_PORT')}/files/{found_immunespace_download['object_id']}")

        ret = ProviderResponse(id=found_immunespace_download["object_id"],
                               object_id=found_immunespace_download["object_id"],
                               submitter_id=found_immunespace_download["submitter_id"],
                               size=found_immunespace_download["size"],
                               dimensions=found_immunespace_download["dimensions"],
                               name=found_immunespace_download['file_name'],
                               self_uri=f"http://localhost:{os.getenv('API_PORT')}/objects/{found_immunespace_download['object_id']}",
                               data_type=found_immunespace_download["data_type"],
                               file_type=found_immunespace_download["file_type"],
                               created_time=f"{found_immunespace_download['date_downloaded']}",
                               mime_type="application/csv", status="finished",
                               contents=[], stderr=found_immunespace_download['stderr'])

        return vars(ret)

    except Exception as e:
        logger.exception(e)
        return HTTPException(status_code=404, detail="Not found")


def run_immunespace_download(immunespace_download_id: str, accession_id: str, apikey: str):
    stderr = ""
    volumes = {
        "immunespace-download-data": {'bind': '/data', 'mode': 'rw'}
    }
    image = "txscience/tx-immunespace-groups:0.3"
    command = f"-g \"{accession_id}\" -a \"{apikey}\" -o /data/{immunespace_download_id}"
    immunespace_groups_container_logs = docker_client.containers.run(image, volumes=volumes, name=f"{immunespace_download_id}-immunespace-groups",
                                                                     working_dir=f"/data/{immunespace_download_id}",
                                                                     privileged=True, remove=True, command=command, detach=False)
    immunespace_groups_container_logs_decoded = immunespace_groups_container_logs.decode("utf8")
    stderr += immunespace_groups_container_logs_decoded
    logger.info(msg=f"finished txscience/tx-immunespace-groups:0.3")
    if immunespace_groups_container_logs_decoded.__contains__("returned non-zero exit status"):
        raise Exception("There was a problem running the txscience/tx-immunespace-groups container")

    image = "txscience/fuse-mapper-immunespace:0.1"
    command = f"-g /data/{immunespace_download_id}/geneBySampleMatrix.csv -p /data/{immunespace_download_id}/phenoDataMatrix.csv"
    mapper_container_logs = docker_client.containers.run(image, volumes=volumes, name=f"{immunespace_download_id}-immunespace-mapper",
                                                         working_dir=f"/data/{immunespace_download_id}",
                                                         privileged=True, remove=True, command=command, detach=False)
    logger.info(msg=f"finished fuse-mapper-immunespace:0.1")
    mapper_container_logs_decoded = mapper_container_logs.decode("utf8")
    stderr += mapper_container_logs_decoded
    logger.debug(msg=f"stderr: {stderr}")
    return stderr


@app.get("/files/{object_id}")
async def files(object_id: str):
    query = {"object_id": object_id}
    projection = {"_id": 0, "immunespace_download_id": 1, "object_id": 1, "submitter_id": 1, "accession_id": 1, "apikey": 1, "file_name": 1, "date_downloaded": 1}
    entry = mongo_db_immunespace_downloads_column.find_one(query, projection)
    immunespace_download_id = entry["immunespace_download_id"]
    file_name = entry["file_name"]

    file_path = os.path.abspath(f"/app/data/{immunespace_download_id}/{file_name}")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Not found")

    def iterfile():
        try:
            with open(file_path, mode="rb") as file_data:
                yield from file_data
        except:
            raise Exception()

    response = StreamingResponse(iterfile(), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename={file_name}"
    return response


@app.delete("/delete/{object_id}", summary="DANGER ZONE: Delete a downloaded object; this action is rarely justified.")
async def delete(object_id: str):
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

    projection = {"_id": 0, "immunespace_download_id": 1, "object_id": 1, "submitter_id": 1, "accession_id": 1, "apikey": 1, "status": 1, "data_type": 1,
                  "file_type": 1, "file_name": 1, "date_downloaded": 1}
    query = {"object_id": object_id}
    found_immunespace_download = mongo_db_immunespace_downloads_column.find_one(query, projection)

    message = ""
    error = ""

    if found_immunespace_download is not None:

        # Assuming the job already executed, remove any database records
        try:

            task_query = {"immunespace_download_id": found_immunespace_download["immunespace_download_id"]}
            ret = mongo_db_immunespace_downloads_column.delete_many(task_query)
            # <class 'pymongo.results.DeleteResult'>
            delete_status = "deleted"
            if not ret.acknowledged:
                delete_status = "failed"
                message += "ret.acknowledged not True.\n"
            message += f"Deleted count=({str(ret.deleted_count)}), Acknowledged=({str(ret.acknowledged)}).\n"
        except Exception as e:
            error += f"! Exception {type(e)} occurred while deleting job from database, message=[{e}] \n! traceback=\n{traceback.format_exc()}\n"
            delete_status = "exception"

        # Data are cached on a mounted filesystem, unlink that too if it's there
        try:
            local_path = os.path.abspath(f"/app/data/{found_immunespace_download['immunespace_download_id']}")
            shutil.rmtree(local_path, ignore_errors=False)
        except Exception as e:
            error += f"! Exception {type(e)} occurred while deleting job from filesystem, message=[{e}] \n! traceback=\n{traceback.format_exc()}\n"
            delete_status = "exception"

    ret = {
        "status": delete_status,
        "info": message,
        "stderr": error,
    }
    logger.debug(f"{ret}")
    return ret
