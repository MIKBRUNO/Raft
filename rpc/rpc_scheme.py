from typing import Hashable, Optional
from abc import ABC, abstractmethod
from enum import Enum
import json
from datetime import datetime


ENCODING = "UTF-8"


class RPCTypes(Enum):
    CALL = 0
    RESPONSE = 1


class RPCScheme(ABC):
    def dump(self) -> bytes:
        return bytes(json.dumps({"id": self.id, "type": self.rpc_type.name, "data": self.data}), encoding=ENCODING)


    @staticmethod
    def load(data: bytes) -> Optional['RPCScheme']:
        try:
            json_str = str(data, encoding=ENCODING, errors='strict')
            obj = json.loads(json_str)
            t = RPCTypes[obj["type"]]
            dt = obj["data"]
            if not isinstance(dt, dict):
                return None
            i = obj["id"]
            if not isinstance(i, str):
                return None
        except:
            return None
        rpc: RPCScheme = None
        if t == RPCTypes.CALL:
            rpc = RPCCall(dt)
        elif t == RPCTypes.RESPONSE:
            rpc = RPCResponse(i, dt)
        else:
            return None
        rpc._id = i
        return rpc
    

    _id: str = None


    @property
    def id(self) -> Hashable:
        if not self._id:
            self._id = str(datetime.now())
        return self._id


    @property
    @abstractmethod
    def data(self) -> dict: ...


    @property
    @abstractmethod
    def rpc_type(self) -> RPCTypes: ...


    def __str__(self) -> str:
        return self.id


class RPCCall(RPCScheme):
    def __init__(self, args: dict):
        super().__init__()
        self._args = args


    @property
    def data(self) -> dict:
        return self._args


    @property
    def rpc_type(self) -> RPCTypes:
        return RPCTypes.CALL
    

class RPCResponse(RPCScheme):
    def __init__(self, id: Hashable, response: dict):
        super().__init__()
        self._id = id
        self._result = response


    @property
    def data(self) -> dict:
        return self._result


    @property
    def rpc_type(self) -> RPCTypes:
        return RPCTypes.RESPONSE
