from rpc import RPCManager, RPCCallable
from networking import NetworkMember
from enum import Enum
import asyncio


class MemberStates(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class MessageTypes(Enum):
    APPENDENTRIESREQUEST = 0
    APPENDENTRIESRESPONSE = 1
    REQUESTVOTEREQUEST = 2
    REQUESTVOTERESPONSE = 3


class ServerState:
    _votedFor: str = None
    _currentTerm: int = 0
    
    @property
    def votedFor(self) -> str | None:
        return self._votedFor
    
    @votedFor.setter
    def votedFor(self, value: str):
        self._votedFor = value
    
    @property
    def currentTerm(self) -> int:
        return self._currentTerm
    
    @currentTerm.setter
    def currentTerm(self, value: int):
        self._currentTerm = value


class Raft:
    def __init__(self, member: NetworkMember, other_members: list[NetworkMember], **kwargs):
        self._this = member
        self._data = ServerState()
        self._state = MemberStates.FOLLOWER
        self._rpc = RPCManager(member, other_members, self._handle_rpcs, **kwargs)
        self._rpc_endpoints = {m.id : self._rpc.get_rcp_endpoint(m) for m in other_members}
        asyncio.


    def convert_to_follower(self, new_term: int):
        self._data.currentTerm = new_term
        self._data.votedFor = None
        self._state = MemberStates.FOLLOWER
        self._rpc.cancel_pending_rpcs()


    async def handle_append_entries_request(self, term: int) -> dict:
        if term < self._data.currentTerm:
            return {"term": self._data.currentTerm, "success": False}
        if term > self._data.currentTerm or self._state == MemberStates.CANDIDATE:
            self.convert_to_follower(term)
        return {"term": self._data.currentTerm, "success": True}
    

    async def handle_request_vote_request(self, term: int, candidateId: str) -> dict:
        if term < self._data.currentTerm:
            return {"term": self._data.currentTerm, "voteGranted": False}
        if term > self._data.currentTerm:
            self.convert_to_follower(term)
        if self._data.votedFor is None:
            self._data.votedFor = candidateId
            return {"term": self._data.currentTerm, "voteGranted": True}
        return {"term": self._data.currentTerm, "success": False}


    async def _handle_rpcs(self, member: NetworkMember, args: dict) -> dict | None:
        try:
            message_type = MessageTypes[args['type']]
            if message_type == MessageTypes.APPENDENTRIESREQUEST:
                return await self.handle_append_entries_request(args['term'])
            elif message_type == MessageTypes.REQUESTVOTEREQUEST:
                return await self.handle_append_entries_request(args['term'], args['candidateId'])
            else:
                return None
        except KeyError:
            return None
