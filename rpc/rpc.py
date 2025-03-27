from typing import Any, Callable, Awaitable
from networking import NetworkMember, Network, TcpNetwork
from abc import ABC, abstractmethod
import json
import asyncio
from enum import Enum


class RPCTypes(Enum):
    CALL = 0
    RESPONSE = 1


class RPC:
    def __init__(self):
        self._event = asyncio.Event()
        self._result = None

    def terminate(self):
        self._event.set()

    def response(self, answer: dict):
        self._result = answer
        self._event.set()

    async def wait(self, timeout: int | None = None) -> bool:
        try:
            async with asyncio.timeout(timeout):
                await self._event.wait()
        except TimeoutError:
            return False
        return True

    @property
    def answer(self) -> dict | None:
        return self._result


class RPCCallable(ABC):
    @abstractmethod
    async def call(self, args: dict) -> dict | None:
        ...


class MemberCallable(RPCCallable):
        def __init__(self, rpc: 'RPCManager', member: NetworkMember):
            self._this = member
            self._rpc = rpc
        
        async def call(self, args: dict) -> Any:
            return await self._rpc.call(self._this, args)


class RPCManager:
    def __init__(self, member: NetworkMember, other_members: list[NetworkMember], handler: Callable[[NetworkMember, dict], Awaitable[dict]],
                 network_factory: Callable[[NetworkMember, list[NetworkMember]], Network] = TcpNetwork, retry_policy: Callable[[], float] = lambda: 1):
        self._handler = handler
        self._others = other_members
        self._idx = 0
        self._rpcs: dict[NetworkMember, dict[int, RPC]] = {m : {} for m in other_members}
        self._network = network_factory(member, other_members)
        self._network.set_read_callback(self._read_callback)
        self._retry_policy = retry_policy
    
    
    async def call(self, member: NetworkMember, args: dict | None) -> dict | None:
        self._idx += 1
        i = self._idx
        rpc = RPC()
        self._rpcs[member][i] = rpc
        await self._send(member, i, RPCTypes.CALL, args)
        while not await rpc.wait(self._retry_policy()):
            await self._send(member, i, RPCTypes.CALL, args)
        self._rpcs[member].pop(i)
        return rpc.answer
    

    def cancel_pending_rpcs(self, member: NetworkMember | None = None):
        if not member:
            for v in self._rpcs.values():
                for i, m in v.items():
                    m.terminate()
            return
        if member in self._rpcs.keys():
            for i, m in self._rpcs[member].items():
                m.terminate()


    def get_callable(self, member: NetworkMember) -> RPCCallable:
        return MemberCallable(self, member)
    

    async def close(self):
        self.cancel_pending_rpcs()
        await self._network.close()


    async def _send(self, member: NetworkMember, i: int, type: RPCTypes, data: dict):
        data_name = 'args' if type == RPCTypes.CALL else 'result'
        await self._network.send(member, bytes(json.dumps({'i': i, 'type': type.name, data_name: data}), encoding="UTF-8"))


    async def _read_callback(self, member: NetworkMember, msg: bytes):
        try:
            answer = json.loads(msg)
            typ = answer['type']
            i = answer['i']
            if RPCTypes[typ] == RPCTypes.CALL:
                result = await self._handler(member, answer['args'])
                await self._send(member, i, RPCTypes.RESPONSE, result)
            elif RPCTypes[typ] == RPCTypes.RESPONSE:
                if i not in self._rpcs[member].keys():
                    return
                self._rpcs[member][i].response(answer['result'])
            else:
                return
        except json.JSONDecodeError | KeyError as e:
            ...
