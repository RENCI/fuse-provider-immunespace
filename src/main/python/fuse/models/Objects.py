import inspect
from typing import Type, List, Any, Optional

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


class Checksums:
    def __init__(self, checksum: str, type: str):
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


class ImmunespaceGA4GHDRSResponse:
    def __init__(self, id: str, name: str, self_uri: str, created_time: str, mime_type: str, description: Optional[str] = None, size: Optional[int] = 0,
                 updated_time: Optional[str] = None, version: Optional[str] = None, aliases: Optional[list[str]] = None, checksums: Optional[list[Checksums]] = None,
                 access_methods: Optional[list[AccessMethods]] = None, contents: Optional[list[Contents]] = None):
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
