from networking import NetworkMember, Network
from typing import Callable, Awaitable
import asyncio


class TcpAddress:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        self.address = (ip, port)


class TcpMember(NetworkMember):
    def __init__(self, ip: str, port: int):
        super().__init__()
        self._address = TcpAddress(ip, port)

    
    @property
    def id(self) -> TcpAddress:
        return self._address


    def __eq__(self, other: 'TcpMember') -> bool:
        return self.id.address == other.id.address


    def __lt__(self, other: 'TcpMember') -> bool:
        return self.id.address < other.id.address


    def __le__(self, other: 'TcpMember') -> bool:
        return self.id.address <= other.id.address


class TcpNetwork(Network):
    async def __init__(self, member: TcpMember, other_members: list[TcpMember],
                       retry_connection_policy: Callable[[], float] = lambda: 5):
        await super().__init__(member, other_members)
        self._this = member
        self._other_members = other_members
        self._connections: dict[TcpMember, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._connection_tasks = []
        for mem in self._other_members:
            self._connection_tasks.append(
                asyncio.create_task(self._try_connnect_coroutine(mem, retry_connection_policy)))
        server = await asyncio.start_server(
            self._handle_client_connected,
            self._this.id[0], self._this.id[1],
            keep_alive=True)
        self._server_task = asyncio.create_task(server.serve_forever())


    async def _try_connnect_coroutine(self, member: TcpMember, retry_policy: Callable[[], float]):
        while (member > self._this and member not in self._connections.keys()):
            try:
                reader, writer = await asyncio.open_connection(member.id.ip, member.id.port)
                self._connections.update({member: (reader, writer)})
            except asyncio.CancelledError as e:
                raise e
            except:
                await asyncio.sleep(retry_policy())


    async def _handle_client_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        ip, port = writer.transport.get_extra_info("peername")
        member = TcpMember(ip, port)
        if member in self._other_members:
            self._connections[TcpMember(ip, port)] = (reader, writer)
        else:
            reader.feed_eof()
            writer.close()
            await writer.wait_closed()
