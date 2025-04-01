import asyncio
import sys
from networking import TcpNetwork, TcpMember

async def connect_stdin_stdout():
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    return reader, writer

members = [TcpMember(address=('127.0.0.1', 4001)), TcpMember(address=('127.0.0.1', 4002)), TcpMember(address=('127.0.0.1', 4003))]
i = input("1-3: ")
this = TcpMember(address=('127.0.0.1', 4000 + int(i)))
members.remove(this)

network = TcpNetwork(this, members)
network.set_read_callback(lambda m, b: print("from ", m, ": ", b))
network.set_connected_callback(lambda m: print("connected: ", m))
network.set_disconnected_callback(lambda m: print("disconnected: ", m))

async def main():
    task = asyncio.create_task(network.run())
    r, w = await connect_stdin_stdout()
    while True:
        w.write(b"0-1: ")
        await w.drain()
        i = str(await r.readline(), encoding="UTF-8")
        w.write(b"msg: ")
        await w.drain()
        msg = await r.readline()
        await network.send(members[int(i)], msg)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
