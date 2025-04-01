from raft_state import RaftState
from networking import Network, NetworkMember
from rpc import RPCManager, RPCCallable
from resettable_timer import ResettableTimer
from math import ceil
from typing import Callable
from statemachine import State, StateMachine
from enum import Enum
import asyncio


class RaftRPCTypes(Enum):
    APPENDENTRIES = 0
    REQUESTVOTE = 1


class RaftServer(StateMachine):
    follower = State(initial=True)
    candidate = State()
    leader = State()

    election_timeout = follower.to(candidate) | candidate.to(candidate)
    win_election = candidate.to(leader)
    higher_term = candidate.to(follower) | leader.to(follower)
    new_leader = candidate.to(follower)


    def __init__(self, network: Network, election_timeout_policy: Callable[[], float],
                 retry_rpc_policy: Callable[[], float] | None = None):
        super().__init__(allow_event_without_transition=True)
        self._network = network
        self._members = {m.id: m for m in self._network.members}
        self._rpcm = RPCManager(network, self._handle_rpc_call, retry_policy=retry_rpc_policy)
        self._state = RaftState()
        self._election_timeout = election_timeout_policy
        self._election_timer = ResettableTimer(self._election_timeout(), self.election_timeout, start=False)


    async def on_enter_follower(self):
        self._election_timer.reset(self._election_timeout())
        self._rpcm.cancel_pending_rpcs()
        ... # cancel leader tasks


    async def on_enter_candidate(self):
        self._election_timer.reset(self._election_timeout())
        self._rpcm.cancel_pending_rpcs()
        await self._run_election()


    async def on_enter_leader(self):
        self._election_timer.stop()
        self._rpcm.cancel_pending_rpcs()
        ... # start leader tasks


    async def _request_vote(self, rpc: RPCCallable) -> bool:
        result = await rpc.call({"type": RaftRPCTypes.REQUESTVOTE.name, "term": self._state.current_term})
        if not result:
            return False
        try:
            if await self._handle_term(result["term"]) > self._state.current_term:
                await self.higher_term()
            return result["voteGranted"]
        except KeyError:
            return False


    async def _run_election(self):
        self._state.current_term += 1
        self._state.voted_for = self._network.this
        quorum = ceil((len(self._members) + 1) / 2)
        votes = 1
        rpcs = [self._rpcm.get_rcp_endpoint(m) for m in self._members.values()]
        async with asyncio.TaskGroup() as tg:
            async def request(rpc: RPCCallable):
                if not await self._request_vote(rpc):
                    return
                votes += 1
                if votes >= quorum:
                    asyncio.current_task().cancel()
            for r in rpcs:
                tg.create_task(request(r))
        if votes >= quorum:
            await self.win_election()

    
    async def _handle_rpc_call(self, member: NetworkMember, args: dict) -> dict | None:
        try:
            rpc_type = RaftRPCTypes[args["type"]]
            if rpc_type == RaftRPCTypes.APPENDENTRIES:
                return self._handle_append_entries(member, args["term"])
            elif rpc_type == RaftRPCTypes.REQUESTVOTE:
                return self._handle_request_vote(member, args["term"])
        except KeyError:
            return None

    
    async def _handle_append_entries(self, member: NetworkMember, term: int) -> dict | None:
        answer = lambda success: {"term": self._state.current_term, "success": success}
        self._election_timer.reset(self._election_timeout())
        if not await self._handle_term(term):
            return answer(False)
        ... # redirect pending user requests
        return answer(True)


    async def _handle_request_vote(self, member: NetworkMember, term: int) -> dict | None:
        answer = lambda success: {"term": self._state.current_term, "voteGranted": success}
        self._election_timer.reset(self._election_timeout())
        if not await self._handle_term(term):
            return answer(False)
        if self._state.voted_for is None or member == self._state.voted_for:
            self._state.voted_for = member
            return answer(True)
        return answer(False)


    async def _handle_term(self, term: int) -> bool:
        if (term < self._state.current_term):
            return False
        if (term == self._state.current_term):
            await self.new_leader()
            return True
        if (term > self._state.current_term):
            await self.higher_term()
            return True
