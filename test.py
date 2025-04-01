import asyncio
import sys
from networking import TcpNetwork, TcpMember
from rpc import RPCManager, RPCCallable
import json

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

async def handler(m: TcpMember, d: dict) -> dict:
    print("from ", TcpMember, ": ", d)
    return {"answer": "wowo"}

async def main():
    net = TcpNetwork(this, members)
    rpc = RPCManager(net, handler)
    try:
        while True:
            i = await ainput("0-1: ")
            cl = rpc.get_rcp_endpoint(members[int(i)])
            msg = await ainput("msg: ")
            msg_dict = json.loads(msg)
            print("answer", await cl.call(msg_dict))
    finally:
        await rpc.close()
        await net.close()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
