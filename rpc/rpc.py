from typing import Callable, Awaitable, Hashable
from networking import NetworkMember, Network
from .rpc_callable import RPCCallable
from .rpc_scheme import RPCCall, RPCResponse, RPCScheme, RPCTypes
import asyncio


class RPC:
    def __init__(self):
        self._event = asyncio.Event()
        self._result = None

    def terminate(self):
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


class RPCManager:
    def __init__(self, network: Network,
                 input_calls_handler: Callable[[NetworkMember, dict], Awaitable[dict | None] | dict | None],
                 retry_policy: Callable[[], float] | None = None):
        self._network = network
        self._network.set_read_callback(self._read_callback)
        self._others = network.members
        self._handler = input_calls_handler
        self._retry_policy = lambda: 1
        if retry_policy:
            self._retry_policy = retry_policy
        self._rpcs: dict[Hashable, RPC] = {}
    
    
    async def call(self, member: NetworkMember, args: dict | None) -> dict | None:
        msg = RPCCall(args)
        rpc = RPC()
        self._rpcs[msg.id] = rpc
        data = msg.dump()
        await self._network.send(member, data)
        while not await rpc.wait(self._retry_policy()):
            await self._network.send(member, data)
        self._rpcs.pop(msg.id)
        return rpc.answer
    

    def cancel_pending_rpcs(self):
        for rpc in self._rpcs:
            rpc.terminate()


    def get_rcp_endpoint(self, member: NetworkMember) -> RPCCallable:
        return MemberCallable(self, member)
    

    async def close(self):
        self.cancel_pending_rpcs()


    async def _read_callback(self, member: NetworkMember, data: bytes):
        msg: RPCScheme = RPCScheme.load(data)
        if not msg:
            return
        if msg.rpc_type == RPCTypes.CALL:
            if asyncio.iscoroutinefunction(self._handler):
                result = await self._handler(member, msg.data)
            else:
                result = self._handler(member, msg.data)
            if result:
                await self._network.send(member, RPCResponse(msg.id, result).dump())
        elif msg.rpc_type == RPCTypes.RESPONSE:
            if msg.id not in self._rpcs.keys():
                return
            self._rpcs[msg.id].response(msg.data)
        else:
            return
