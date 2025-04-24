from pydantic import BaseModel, ConfigDict

from enum import Enum


class RaftLogItem(BaseModel):
    term: int
    command: dict


class RaftRPCType(Enum):
    APPENDENTRIES = 'APPENDENTRIES'
    REQUESTVOTE = 'REQUESTVOTE'
    CLIENTCALL = 'CLIENTCALL'


class RaftRPCMessage(BaseModel):
    model_config = ConfigDict(extra='allow')
    
    raft_type: RaftRPCType


class AppendEntries(RaftRPCMessage):
    raft_type: RaftRPCType = RaftRPCType.APPENDENTRIES
    term: int
    prevLogIndex: int
    prevLogTerm: int
    entries: list[RaftLogItem]
    leaderCommit: int


class AppendEntriesResponse(RaftRPCMessage):
    raft_type: RaftRPCType = RaftRPCType.APPENDENTRIES
    term: int
    success: bool


class RequestVote(RaftRPCMessage):
    raft_type: RaftRPCType = RaftRPCType.REQUESTVOTE
    term: int
    lastLogIndex: int
    lastLogTerm: int


class RequestVoteResponse(RaftRPCMessage):
    raft_type: RaftRPCType = RaftRPCType.REQUESTVOTE
    term: int
    voteGranted: bool


class ClientCall(RaftRPCMessage):
    raft_type: RaftRPCType = RaftRPCType.CLIENTCALL
    command: dict


class ClientCallResponse(RaftRPCMessage):
    raft_type: RaftRPCType = RaftRPCType.CLIENTCALL
