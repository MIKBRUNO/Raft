import asyncio
import sys
from networking import TcpNetwork, TcpMember

members = [TcpMember(address=('127.0.0.1', 4001)), TcpMember(address=('127.0.0.1', 4002)), TcpMember(address=('127.0.0.1', 4003))]
i = input("1-3: ")
this = TcpMember(address=('127.0.0.1', 4000 + int(i)))
members.remove(this)

async def ainput(string: str) -> str:
    await asyncio.get_event_loop().run_in_executor(
            None, lambda s=string: sys.stdout.write(s+' '))
    await asyncio.get_event_loop().run_in_executor(
            None, lambda s=string: sys.stdout.flush())
    return await asyncio.get_event_loop().run_in_executor(
            None, sys.stdin.readline)

async def main():
    try:
        network = TcpNetwork(this, members, lambda m, b: print("from ", m, ": ", b))
        network.set_connected_callback(lambda m: print("connected: ", m))
        network.set_disconnected_callback(lambda m: print("disconnected: ", m))
        while True:
            i = await ainput("0-1: ")
            msg = await ainput("msg: ")
            await network.send(members[int(i)], bytes(msg, encoding="UTF-8"))
    finally:
        await network.close()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
