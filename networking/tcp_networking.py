from .networking import NetworkMember, Network
from typing import Callable, Awaitable, Optional
import asyncio
from ipaddress import ip_address, IPv4Address, IPv6Address
from hashlib import sha256


class TcpAddress:
    def __init__(self, ip: str, port: int):
        self._ip: IPv6Address | IPv4Address = ip_address(ip)
        self.port = port

    @property
    def address(self) -> tuple[str, int]:
        return (self.ip, self.port)
    
    @property
    def ip(self) -> str:
        return self._ip.compressed
    
    @ip.setter
    def ip(self, value: str):
        self._ip = ip_address(value)

    @ip.deleter
    def ip(self):
        del self._ip


class TcpMember(NetworkMember):
    _address: TcpAddress | None = None
    _hash: bytes
    _id: str

    def __init__(self, *, hash: Optional[bytes] = None, address: Optional[tuple[str, int]] = None):
        super().__init__()
        if hash and not address:
            self._hash = hash
        elif address and not hash:
            self._address = TcpAddress(address[0], address[1])
            self._hash = sha256(bytes(":".join([self.address.ip, str(self.address.port)]), encoding="UTF-8")).digest()
        else:
            raise ValueError("only hash or only address should be specified")
        self._id = self._hash[:8].hex()
    
    @property
    def address(self) -> TcpAddress | None:
        return self._address

    @property
    def id(self) -> str:
        return self._id
    
    @property
    def hash(self) -> bytes:
        return self._hash

    def __eq__(self, other: 'TcpMember') -> bool:
        return self._hash == other._hash

    def __lt__(self, other: 'TcpMember') -> bool:
        return self._hash < other._hash

    def __le__(self, other: 'TcpMember') -> bool:
        return self._hash <= other._hash

    def __hash__(self):
        return hash(self._hash)

    def __str__(self):
        return self._id


class TcpNetwork(Network):
    def __init__(self, member: TcpMember, other_members: list[TcpMember],
                       retry_connection_policy: Callable[[], float] = lambda: 5):
        super().__init__(member, other_members)
        self._this = member
        self._other_members = other_members
        self._recv_cb: Callable[[NetworkMember, bytes], None | Awaitable[None]] = None
        self._disconnected_cb: Callable[[NetworkMember], None | Awaitable[None]] = None
        self._connected_cb: Callable[[NetworkMember], None | Awaitable[None]] = None
        self._connections: dict[TcpMember, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._task_group: asyncio.TaskGroup = None
        self._retry_policy: Callable[[], float] = retry_connection_policy


    async def run(self):
        server: asyncio.Server | None = None
        try:
            async with asyncio.TaskGroup() as tg:
                self._task_group = tg
                server = await asyncio.start_server(
                    self._handle_connected_by_server,
                    self._this.address.ip, self._this.address.port)
                for mem in self._other_members:
                    self._start_connecting(mem, self._retry_policy)
                # server.serve_forever()
                while True:
                    await asyncio.sleep(100)
        finally:
            # if server:
            #     server.close()
            #     await server.wait_closed()
            self._task_group = None
            self._connections.clear()


    async def _handle_connected_by_server(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            peer_hash = await reader.readexactly(32)
        except (ConnectionError, asyncio.IncompleteReadError):
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        else:
            member = TcpMember(hash=peer_hash)
            if member in self._other_members:
                self._connections[member] = (reader, writer)
                await self._handle_connected(member)
            else:
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()


    async def _handle_connected(self, member: TcpMember):
        self._task_group.create_task(self._handle_read(member))
        if asyncio.iscoroutinefunction(self._connected_cb):
            await self._connected_cb(member)
        elif self._connected_cb:
            self._connected_cb(member)


    def _start_connecting(self, member: TcpMember, retry_policy: Callable[[], float]):
        self._task_group.create_task(self._try_connnect_coroutine(member, retry_policy))


    async def _try_connnect_coroutine(self, member: TcpMember, retry_policy: Callable[[], float]):
        while (member > self._this and member not in self._connections.keys()):
            try:
                reader, writer = await asyncio.open_connection(member.address.ip, member.address.port)
                writer.write(self._this.hash)
                await writer.drain()
                self._connections[member] = (reader, writer)
                await self._handle_connected(member)
                break
            except ConnectionError:
                await asyncio.sleep(retry_policy())


    async def _handle_read(self, member: TcpMember):
        if member not in self._connections.keys():
            return
        reader = self._connections[member][0]
        while not reader.at_eof():
            try:
                msg_len_bytes = await reader.readexactly(4)
                msg_len = int.from_bytes(msg_len_bytes, byteorder="big", signed=False)
                message = await reader.readexactly(msg_len)
                if asyncio.iscoroutinefunction(self._recv_cb):
                    await self._recv_cb(member, message)
                elif self._recv_cb:
                    self._recv_cb(member, message)
            except (ConnectionError, asyncio.IncompleteReadError):
                break
        await self._handle_disconnected(member)


    async def send(self, member: NetworkMember, msg: bytes) -> None:
        if member not in self._connections.keys():
            return
        writer = self._connections[member][1]
        msg_len = len(msg)
        try:
            writer.write(msg_len.to_bytes(length=4, byteorder="big", signed=False))
            writer.write(msg)
            await writer.drain()
        except ConnectionError:
            await self._handle_disconnected(member)


    async def _handle_disconnected(self, member: TcpMember):
        if member not in self._connections.keys():
            return
        writer = self._connections[member][1]
        self._connections.pop(member)
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
        if asyncio.iscoroutinefunction(self._disconnected_cb):
            await self._disconnected_cb(member)
        elif self._disconnected_cb:
            self._disconnected_cb(member)
        self._start_connecting(member, self._retry_policy)


    @property
    def connected_members(self) -> list[NetworkMember]:
        return self._connections.keys()


    @property
    def members(self) -> list[NetworkMember]:
        return self._other_members


    @property
    def this(self) -> NetworkMember:
        return self._this


    def set_read_callback(self, cb: Callable[[NetworkMember, bytes], None | Awaitable[None]] | None) -> None:
        self._recv_cb = cb


    def set_disconnected_callback(self, cb: Callable[[NetworkMember], None | Awaitable[None]] | None) -> None:
        self._disconnected_cb = cb


    def set_connected_callback(self, cb: Callable[[NetworkMember], None | Awaitable[None]] | None) -> None:
        self._connected_cb = cb
