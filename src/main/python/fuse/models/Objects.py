import inspect
from typing import Type, List, Optional, Any

from fastapi import Form, Query
from pydantic import BaseModel, Field
from pydantic.networks import EmailStr
from enum import Enum


def as_form(cls: Type[BaseModel]):
    new_params = [
        inspect.Parameter(
            field.alias,
            inspect.Parameter.POSITIONAL_ONLY,
            default=(Form(field.default) if not field.required else Form(...)),
        )
        for field in cls.__fields__.values()
    ]

    async def _as_form(**data):
        return cls(**data)

    sig = inspect.signature(_as_form)
    sig = sig.replace(parameters=new_params)
    _as_form.__signature__ = sig
    setattr(cls, "as_form", _as_form)
    return cls


@as_form
class PluginParameter(BaseModel):
    id: str
    title: str
    description: str
    value: str
    type: str
    format: str


@as_form
class SampleVariable(BaseModel):
    id: str = "FUSE:ExampleId"
    title: str = "Demo sampleVariable"
    description: str = "This sample variable is for demonstration only. Replace this with variables your appliance supports for the digital objects in your system."
    type: str = "string"
    default: str = "example default value"


class Checksums(BaseModel):
    def __init__(self, checksum: str, type: str, **data: Any):
        super().__init__(**data)
        self.checksum = checksum
        self.type = type


# access_url: AccessURL = {
#     "url": "string",
#     "headers": "Authorization: Basic Z2E0Z2g6ZHJz"
# }
class AccessURL:
    def __init__(self, url: str, headers: str):
        self.url = url
        self.headers = headers


class AccessMethods:
    def __init__(self, type: str, access_id: str, region: str):
        self.type = type
        self.access_id = access_id
        self.region = region


class Contents:
    def __init__(self, id: str, name: str, drs_uri: str, contents: Optional[list[str]] = None):
        self.id = id
        self.name = name
        self.drs_uri = drs_uri
        self.contents = contents


class ImmunespaceProviderResponse:
    def __init__(self, id: str, object_id: str, submitter_id: str, name: str, self_uri: str, created_time: str, mime_type: str, file_type: str, status: str,
                 description: Optional[str] = None,
                 size: Optional[int] = 0, updated_time: Optional[str] = None, version: Optional[str] = None, aliases: Optional[list[str]] = None,
                 checksums: Optional[list[Checksums]] = None,
                 access_methods: Optional[list[AccessMethods]] = None, contents: Optional[list[Contents]] = None, data_type: Optional[str] = None, stderr: Optional[str] = None):
        self.id = id
        self.object_id = object_id
        self.submitter_id = submitter_id
        self.name = name
        self.mime_type = mime_type
        self.data_type = data_type
        self.file_type = file_type
        self.status = status
        self.description = description
        self.self_uri = self_uri
        self.size = size
        self.created_time = created_time
        self.updated_time = updated_time
        self.version = version
        self.aliases = aliases
        self.checksums = checksums
        self.access_methods = access_methods
        self.contents = contents
        self.stderr = stderr


@as_form
class Passports(BaseModel):
    expand: bool = False
    passports: List[str] = ["eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJnYTRnaF9wYXNzcG9ydF92MSI6W119.JJ5rN0ktP0qwyZmIPpxmF_p7JsxAZH6L6brUxtad3CM"]


class DataType(str, Enum):
    geneExpression = 'class_dataset_expression'
    resultsPCATable = 'class_results_PCATable'
    resultsCellFieDetailScoringTable = 'class_results_CellFieDetailScoringTable'
    resultsCellFieScoreBinaryTable = 'class_results_CellFieScoreBinaryTable'
    resultsCellFieScoreTable = 'class_results_CellFieScoreTable'
    resultsCellFieTaskInfoTable = 'class_results_CellFieTaskInfoTable'
    # xxx to add more datatypes: expand this


class FileType(str, Enum):
    datasetGeneExpression = 'filetype_dataset_expression'
    datasetProperties = 'filetype_dataset_properties'
    datasetArchive = 'filetype_dataset_archive'
    resultsPCATable = 'filetype_results_PCATable'
    resultsCellFieDetailScoringTable = 'filetype_results_CellFieDetailScoringTable'
    resultsCellFieScoreBinaryTable = 'filetype_results_CellFieScoreBinaryTable'
    resultsCellFieScoreTable = 'filetype_results_CellFieScoreTable'
    resultsCellFieTaskInfoTable = 'filetype_results_CellFieTaskInfoTable'
    # xxx to add more datatypes: expand this


@as_form
class ProviderParameters(BaseModel):
    service_id: str = Field(..., title="Provider service id", description="id of service used to upload this object")
    submitter_id: EmailStr = Field(..., title="email", description="unique submitter id (email)")
    data_type: DataType = Field(..., title="Data type of this object", description="the type of data associated with this object (e.g, results or input dataset)")
    description: Optional[str] = Field(None, title="Description", description="detailed description of this data (optional)")
    version: Optional[str] = Field(None, title="Version of this object",
                                   description="objects shouldn't ever be deleted unless data are redacted or there is a database consistency problem.")
    accession_id: Optional[str] = Field(None, title="External accession ID", description="if sourced from a 3rd party, this is the accession ID on that db")
    apikey: Optional[str] = Field(None, title="External apikey", description="if sourced from a 3rd party, this is the apikey used for retrieval")
    aliases: Optional[str] = Field(None, title="Optional list of aliases for this object")
    checksums: Optional[List[Checksums]] = Field(None, title="Optional checksums for the object",
                                                 description="enables verification checking by clients; this is a json list of objects, each object contains 'checksum' and 'type' fields, where 'type' might be 'sha-256' for example.")
    requested_object_id: Optional[str] = Field(default=None,
                                               description="optional argument to be used by submitter to request an object_id; this could be, for example, used to retrieve objects from a 3rd party for which this endpoint is a proxy. The requested object_id is not guaranteed, enduser should check return value for final object_id used."),


