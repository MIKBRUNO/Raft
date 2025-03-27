from networking import NetworkMember, Network
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
        self._recv_cb: Callable[[NetworkMember, bytes], Awaitable] = None
        self._disconnected_cb: Callable[[NetworkMember], Awaitable] = None
        self._connected_cb: Callable[[NetworkMember], Awaitable] = None
        self._connections: dict[TcpMember, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._connection_tasks = set()
        self._reading_tasks = set()
        self._retry_policy: Callable[[], float] = retry_connection_policy
        for mem in self._other_members:
            self._start_connecting(mem, self._retry_policy)
        self._server_task = asyncio.create_task(self._start_server())


    async def _start_server(self):
        server = await asyncio.start_server(
            self._handle_client_connected,
            self._this.address.ip, self._this.address.port)
        await server.serve_forever()


    def _start_connecting(self, member: TcpMember, retry_policy: Callable[[], float]):
        task = asyncio.create_task(self._try_connnect_coroutine(member, retry_policy))
        self._connection_tasks.add(task)
        task.add_done_callback(self._connection_tasks.discard)


    async def _try_connnect_coroutine(self, member: TcpMember, retry_policy: Callable[[], float]):
        while (member > self._this and member not in self._connections.keys()):
            await asyncio.sleep(retry_policy())
            try:
                reader, writer = await asyncio.open_connection(member.address.ip, member.address.port)
                try:
                    writer.write(self._this.hash)
                    await writer.drain()
                except:
                    writer.close()
                    await writer.wait_closed()
                    continue
                self._connections[member] = (reader, writer)
                await self._handle_connected(member)
                break
            except asyncio.CancelledError:
                break
            except:
                continue


    async def _handle_client_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            peer_hash = await reader.readexactly(32)
        except:
            writer.close()
            await writer.wait_closed()
            return
        member = TcpMember(hash=peer_hash)
        if member in self._other_members:
            self._connections[member] = (reader, writer)
            await self._handle_connected(member)
        else:
            writer.close()
            await writer.wait_closed()


    async def _handle_read(self, member: TcpMember):
        if member not in self._connections.keys():
            return
        reader = self._connections[member][0]
        while not reader.at_eof():
            try:
                msg_len_bytes = await reader.readexactly(4)
                msg_len = int.from_bytes(msg_len_bytes, byteorder="big", signed=False)
                message = await reader.readexactly(msg_len)
                if self._recv_cb:
                    await self._recv_cb(member, message)
            except:
                break
        await self._handle_disconnected(member)


    async def _handle_disconnected(self, member: TcpMember):
        if member not in self._connections.keys():
            return
        reader = self._connections[member][0]
        writer = self._connections[member][1]
        self._connections.pop(member)
        try:
            writer.close()
            await writer.wait_closed()
        except ConnectionError:
            ...
        if self._disconnected_cb:
            await self._disconnected_cb(member)
        self._start_connecting(member, self._retry_policy)

    
    async def _handle_connected(self, member: TcpMember):
        task = asyncio.create_task(self._handle_read(member))
        self._reading_tasks.add(task)
        task.add_done_callback(self._reading_tasks.discard)
        if self._connected_cb:
            await self._connected_cb(member)


    def get_connected_members(self) -> list[NetworkMember]:
        return self._connections.keys()


    def set_read_callback(self, cb: Callable[[NetworkMember, bytes], Awaitable] | None) -> None:
        self._recv_cb = cb


    def set_disconnected_callback(self, cb: Callable[[NetworkMember], Awaitable] | None) -> None:
        self._disconnected_cb = cb


    def set_connected_callback(self, cb: Callable[[NetworkMember], Awaitable] | None) -> None:
        self._connected_cb = cb


    async def send(self, member: NetworkMember, msg: bytes) -> None:
        if member not in self._connections.keys():
            return
        writer = self._connections[member][1]
        msg_len = len(msg)
        try:
            writer.write(msg_len.to_bytes(length=4, byteorder="big", signed=False))
            writer.write(msg)
            await writer.drain()
        except:
            await self._handle_disconnected(member)

    
    async def close(self) -> None:
        self._server_task.cancel()
        for task in self._reading_tasks.union(self._connection_tasks):
            task.cancel()
        for mem in self._other_members:
            await self._handle_disconnected(mem)
