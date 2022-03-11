import json
import datetime
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'main', 'python'))

from fuse.models.Objects import ImmunespaceGA4GHDRSResponse


def test_datetime_serialization():
    ret = ImmunespaceGA4GHDRSResponse(id="1234",
                                      name="1234",
                                      self_uri=f"http://localhost:{os.getenv('API_PORT')}/objects/1234",
                                      created_time=f"4312", mime_type="application/zip")
    print(ret.__dict__)
    ret_json = json.dumps(ret.__dict__)
    print(ret_json)
    assert ret_json is not None

