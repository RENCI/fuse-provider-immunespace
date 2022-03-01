import inspect
from typing import Type, List, Any

from fastapi import Form
from pydantic import BaseModel


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


class PluginParameter(BaseModel):
    id: str
    title: str
    description: str
    value: str
    type: str
    format: str


class SampleVariable(BaseModel):
    id: str = "FUSE:ExampleId"
    title: str = "Demo sampleVariable"
    description: str = "This sample variable is for demonstration only. Replace this with variables your appliance supports for the digital objects in your system."
    type: str = "string"
    default: str = "example default value"


class Checksums(BaseModel):
    checksum: str
    type: str


class AccessURL(BaseModel):
    url: str = "string"
    headers: str = "Authorization: Basic Z2E0Z2g6ZHJz"


class AccessMethods(BaseModel):
    type: str = "s3"
    access_url: AccessURL = {
        "url": "string",
        "headers": "Authorization: Basic Z2E0Z2g6ZHJz"
    }
    access_id: str = "string"
    region: str = "us-east-1"


class Contents(BaseModel):

    def __init__(self, id: str = None, name: str = None, drs_uri: str = None, contents: list = None, **data: Any):
        super().__init__(**data)
        self.id = id
        self.name = name
        self.drs_uri = drs_uri
        self.contents = contents


@as_form
class ImmunespaceGA4GHDRSResponse(BaseModel):
    def __init__(self, id: str = None, name: str = None, description: str = None, self_uri: str = None, size: int = 0, created_time: str = None, updated_time: str = None,
                 version: str = None, mime_type: str = None, aliases: list = None, checksums: list = None, access_methods: list = None, contents: list = None, **data: Any):
        super().__init__(**data)
        self.id = id
        self.name = name
        self.description = description
        self.self_uri = self_uri
        self.size = size
        self.created_time = created_time
        self.updated_time = updated_time
        self.version = version
        self.mime_type = mime_type
        self.aliases = aliases
        self.checksums = checksums
        self.access_methods = access_methods
        self.contents = contents


@as_form
class Passports(BaseModel):
    expand: bool = False
    passports: List[str] = ["eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJnYTRnaF9wYXNzcG9ydF92MSI6W119.JJ5rN0ktP0qwyZmIPpxmF_p7JsxAZH6L6brUxtad3CM"]
