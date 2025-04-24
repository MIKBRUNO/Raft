from fastapi import FastAPI

from dictionary import Dictionary

from random import random

from os import getenv

import asyncio

import logging

import sys

from raft import RaftObject
from raft.networking import TcpNetwork, TcpMember, Network


SERVER_COUNT = int(getenv('SERVER_COUNT'))
SERVER_INDEX = int(getenv('SERVER_INDEX'))


_network: Network = None
_raft: RaftObject = None
_dictionary: Dictionary = None


async def lifespan(app: FastAPI):
    global _network, _raft, _dictionary
    members = [TcpMember(address=('127.0.0.1', 4000 + i)) for i in range(SERVER_COUNT)]
    this = TcpMember(address=('127.0.0.1', 4000 + int(SERVER_INDEX)))
    members.remove(this)
    _network = TcpNetwork(this, members, lambda: 0.05)
    _raft = RaftObject(_network, lambda: 0.5 + 0.3 * random(), lambda: 0.1, retry_rpc_policy=lambda: 0.5)
    _dictionary = Dictionary(_raft)
    task = asyncio.create_task(_network.run())
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    yield
    task.cancel()

def get_dictionary() -> Dictionary:
    global _dictionary
    return _dictionary
