from typing import Callable, Awaitable, Hashable
from raft.networking import NetworkMember, Network
from .rpc_callable import RPCCallable
from .rpc_scheme import RPCCall, RPCResponse, RPCScheme, RPCTypes
import asyncio
import logging


__logger__ = logging.getLogger(__name__)
__logger__.setLevel(logging.ERROR)


class RPCTerminatedException(BaseException):
    pass


class RPC:
    def __init__(self):
        self._event = asyncio.Event()
        self._result = None
        self._terminated = False

    def terminate(self):
        self._terminated = True
        self._event.set()

    def response(self, answer: dict):
        if not self._event.is_set():
            self._result = answer
            self._event.set()

    async def wait(self, timeout: int | None = None) -> bool:
        try:
            async with asyncio.timeout(timeout):
                await self._event.wait()
        except TimeoutError:
            return False
        if self._terminated:
            raise RPCTerminatedException()
        return True

    @property
    def answer(self) -> dict | None:
        return self._result


class MemberCallable(RPCCallable):
    def __init__(self, rpc: 'RPCManager', member: NetworkMember):
        self._this = member
        self._rpc = rpc
    
    async def call(self, args: dict) -> dict:
        return await self._rpc.call(self._this, args)
    
    def __str__(self):
        return str(self._this)


class RPCManager:
    def __init__(self, network: Network,
                 retry_policy: Callable[[], float] | None = None):
        self._network = network
        self._network.set_read_callback(self._read_callback)
        self._others = network.members
        self._retry_policy = lambda: 1
        if retry_policy:
            self._retry_policy = retry_policy
        self._rpcs: dict[Hashable, RPC] = {}
        self._q = asyncio.Queue()
    

    async def listen(self, handler: Callable[[NetworkMember, dict], Awaitable[dict | None] | dict | None]):
        m, d, q = await self._q.get()
        r = None
        try:
            if asyncio.iscoroutinefunction(handler):
                r = await handler(m, d)
            else:
                r = handler(m, d)
        finally:
            await q.put(r)

    
    async def call(self, member: NetworkMember, args: dict | None) -> dict | None:
        """raises: RPCTerminatedException"""
        msg = RPCCall(args)
        __logger__.debug(f"Send CALL {msg}")
        rpc = RPC()
        self._rpcs[msg.id] = rpc
        data = msg.dump()
        try:
            await self._network.send(member, data)
            while not await rpc.wait(self._retry_policy()):
                await self._network.send(member, data)
        finally:
            self._rpcs.pop(msg.id)
        return rpc.answer
    

    def terminate_pending_rpcs(self):
        for rpc in self._rpcs.values():
            rpc.terminate()


    def get_rcp_endpoint(self, member: NetworkMember) -> RPCCallable:
        return MemberCallable(self, member)


    async def _read_callback(self, member: NetworkMember, data: bytes):
        msg: RPCScheme = RPCScheme.load(data)
        if not msg:
            __logger__.warning(f"invalid data recieved: {data}")
            return
        if msg.rpc_type == RPCTypes.CALL:
            __logger__.debug(f"Recieved CALL {msg}")
            q = asyncio.Queue()
            await self._q.put((member, msg.data, q))
            result = await q.get()
            if result:
                __logger__.debug(f"Send RESPONSE {RPCResponse(msg.id, result)}")
                await self._network.send(member, RPCResponse(msg.id, result).dump())
        elif msg.rpc_type == RPCTypes.RESPONSE:
            __logger__.debug(f"Recieved RESPONSE {msg}")
            if msg.id not in self._rpcs.keys():
                return
            self._rpcs[msg.id].response(msg.data)
        else:
            return
