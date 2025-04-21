from pydantic import BaseModel, ConfigDict

from enum import Enum


class RaftRPCType(Enum):
    APPENDENTRIES = 'APPENDENTRIES'
    REQUESTVOTE = 'REQUESTVOTE'


class RaftRPCMessage(BaseModel):
    model_config = ConfigDict(extra='allow')
    
    raft_type: RaftRPCType


class AppendEntries(RaftRPCMessage):
    raft_type: RaftRPCType = RaftRPCType.APPENDENTRIES
    term: int


class AppendEntriesResponse(RaftRPCMessage):
    raft_type: RaftRPCType = RaftRPCType.APPENDENTRIES
    term: int
    success: bool


class RequestVote(RaftRPCMessage):
    raft_type: RaftRPCType = RaftRPCType.REQUESTVOTE
    term: int


class RequestVoteResponse(RaftRPCMessage):
    raft_type: RaftRPCType = RaftRPCType.REQUESTVOTE
    term: int
    voteGranted: bool
