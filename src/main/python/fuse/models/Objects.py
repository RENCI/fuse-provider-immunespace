from typing import Optional

from fuse_cdm.main import Checksums, AccessMethods, Contents
from pydantic import BaseModel


class ProviderResponse(BaseModel):
    id: str
    object_id: str
    submitter_id: str
    name: str
    self_uri: str
    created_time: str
    mime_type: str
    file_type: str
    status: str
    description: Optional[str] = None,
    size: Optional[int] = 0,
    dimension: Optional[str] = None,
    updated_time: Optional[str] = None,
    version: Optional[str] = None,
    aliases: Optional[list[str]] = None,
    checksums: Optional[list[Checksums]] = None,
    access_methods: Optional[list[AccessMethods]] = None,
    contents: Optional[list[Contents]] = None,
    data_type: Optional[str] = None,
    stderr: Optional[str] = None
