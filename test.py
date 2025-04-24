import asyncio
import sys
from raft.networking import TcpNetwork, TcpMember
from raft import RaftObject
import logging
from random import random

async def connect_stdin_stdout():
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    return reader, writer

n = 3

members = [TcpMember(address=('127.0.0.1', 4000 + i)) for i in range(n)]
i = input(f"1-{n}: ")
this = TcpMember(address=('127.0.0.1', 4000 + int(i)))
members.remove(this)

network = TcpNetwork(this, members, lambda: 0.05)
network.set_connected_callback(lambda m: logging.debug(f"Connected {m.id}"))
network.set_disconnected_callback(lambda m: logging.debug(f"Disconnected {m.id}"))

async def main():
    raft = RaftObject(network, lambda: 0.5 + 0.3 * random(), lambda: 0.1, retry_rpc_policy=lambda: 0.05)
    await network.run()

if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
