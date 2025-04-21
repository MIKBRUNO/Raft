from enum import Enum
from datetime import datetime
from pydantic import BaseModel, Field


ENCODING = "UTF-8"


class RPCType(Enum):
    CALL = 'CALL'
    RESPONSE = 'RESPONSE'


class RPCMessage[DataT](BaseModel):
    rpc_type: RPCType
    id: str = Field(default_factory=lambda: str(datetime.now()))
    data: DataT

    def dump_bytes(self) -> bytes:
        return bytes(self.model_dump_json(), encoding=ENCODING)
