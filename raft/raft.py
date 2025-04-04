from .raft_state import RaftState
from networking import Network, NetworkMember
from rpc import RPCManager, RPCCallable, RPCTerminatedException
from typing import Callable, Awaitable
from statemachine import State, StateMachine
from enum import Enum
import asyncio
import logging


__logger__ = logging.getLogger(__name__)


class RaftRPCTypes(Enum):
    APPENDENTRIES = 0
    REQUESTVOTE = 1


class ElectionCompleteException(BaseException):
    pass


class RaftServer(StateMachine):
    follower = State(initial=True)
    candidate = State()
    leader = State()

    election_timeout = follower.to(candidate) | candidate.to.itself()
    win_election = candidate.to(leader)
    higher_term = candidate.to(follower) | leader.to(follower)
    new_leader = candidate.to(follower)


    def on_enter_state(self, event, state):
        __logger__.debug(f"Entering '{state.id}' state from '{event}' event.")


    def __init__(self, network: Network, election_timeout_policy: Callable[[], float],
                 heartbeat_policy: Callable[[], float] | None = None,
                 retry_rpc_policy: Callable[[], float] | None = None):
        super().__init__(allow_event_without_transition=True)
        self._network = network
        self._members = {m.id: m for m in self._network.members}
        self._rpcm = RPCManager(network, retry_policy=retry_rpc_policy)
        self._state = RaftState(self._network.this)
        self._heartbeat_policy = heartbeat_policy if heartbeat_policy else lambda: 0
        self._election_timeout_policy = election_timeout_policy
        self._election_timeout: asyncio.Timeout | None = None
        self._running_task: asyncio.Task | None = None
        self._new_task: asyncio.Task | None = None


    async def _run_wrapper(self, aw: Awaitable):
        if self._running_task:
            self._rpcm.terminate_pending_rpcs()
            self._running_task.cancel()
        self._running_task = self._new_task
        await aw


    def _start_role(self, aw: Awaitable):
        self._new_task = asyncio.create_task(self._run_wrapper(aw))


    def on_enter_follower(self):
        self._start_role(self._run_follower())


    def on_enter_candidate(self):
        self._start_role(self._run_candidate())


    def on_enter_leader(self):
        self._start_role(self._run_leader())        


    async def _listen_forever(self):
        while True:
            await self._rpcm.listen(self._handle_rpc_call)


    def _reset_election_timeout(self):
        if self._election_timeout:
            self._election_timeout.reschedule(asyncio.get_event_loop().time() + self._election_timeout_policy())


    async def _run_follower(self):
        try:
            async with asyncio.timeout(self._election_timeout_policy()) as t:
                self._election_timeout = t
                await self._listen_forever()
        except asyncio.TimeoutError:
            self._election_timeout = None
            self.election_timeout()
        finally:
            self._election_timeout = None


    async def _run_candidate(self):
        self._state.current_term += 1
        self._state.voted_for = self._network.this.id
        try:
            async with asyncio.timeout(self._election_timeout_policy()) as t:
                self._election_timeout = t
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self._run_election())
                    tg.create_task(self._listen_forever())
        except asyncio.TimeoutError:
            self._election_timeout = None
            await self.election_timeout()
        finally:
            self._election_timeout = None


    async def _run_leader(self):
        rpcs = [self._rpcm.get_rcp_endpoint(m) for m in self._members.values()]
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._listen_forever())
                for r in rpcs:
                    tg.create_task(self._append_entries(r))
        except* RPCTerminatedException:
            pass


    async def _request_vote(self, rpc: RPCCallable) -> bool:
        __logger__.debug(f"call RequestVote({rpc}, {self._state.current_term})")
        result = await rpc.call({"type": RaftRPCTypes.REQUESTVOTE.name, "term": self._state.current_term})
        __logger__.debug(f"recv {result}")
        if not result:
            return False
        try:
            term = result["term"]
            if term > self._state.current_term:
                self.higher_term(term)
            return result["voteGranted"]
        except KeyError:
            return False


    async def _run_election(self):
        quorum = len(self._members) // 2 + 1
        __logger__.debug(f"Started election with {quorum} quorum on {self._state.current_term} term")
        votes = 1
        rpcs = [self._rpcm.get_rcp_endpoint(m) for m in self._members.values()]
        terminated = False
        try:
            async with asyncio.TaskGroup() as tg:
                async def request(rpc: RPCCallable):
                    nonlocal votes
                    if not await self._request_vote(rpc):
                        return
                    votes += 1
                    __logger__.debug(f"{votes} votes")
                    if votes >= quorum:
                        raise ElectionCompleteException()
                for r in rpcs:
                    tg.create_task(request(r))
        except* ElectionCompleteException:
            pass
        except* RPCTerminatedException:
            terminated = True
        if terminated:
            return
        if votes >= quorum:
            self.win_election()


    async def _append_entries(self, rpc: RPCCallable):
        while True:
            __logger__.debug(f"call AppendEntries({rpc}, {self._state.current_term})")
            result = await rpc.call({"type": RaftRPCTypes.APPENDENTRIES.name, "term": self._state.current_term})
            __logger__.debug(f"recv {result}")
            try:
                if result:
                    term = result["term"]
                    if term > self._state.current_term:
                        self.higher_term(term)
            except KeyError:
                pass
            await asyncio.sleep(self._heartbeat_policy())

    
    async def _handle_rpc_call(self, member: NetworkMember, args: dict) -> dict | None:
        try:
            rpc_type = RaftRPCTypes[args["type"]]
            if rpc_type == RaftRPCTypes.APPENDENTRIES:
                return await self._handle_append_entries(member, args["term"])
            elif rpc_type == RaftRPCTypes.REQUESTVOTE:
                return await self._handle_request_vote(member, args["term"])
        except KeyError:
            return None

    
    async def _handle_append_entries(self, member: NetworkMember, term: int) -> dict | None:
        answer = lambda success: {"term": self._state.current_term, "success": success}
        __logger__.debug(f"recv AppendEntries({member.id}, {term})")
        if self.current_state is not self.leader:
            self._reset_election_timeout()
        if self._state.current_term > term:
            return answer(False)
        if self._state.current_term < term:
            self._state.current_term = term
            self._state.voted_for = None
            self.higher_term(term)
        if self._state.current_term == term:
            self.new_leader()
        ... # redirect pending user requests
        return answer(True)


    async def _handle_request_vote(self, member: NetworkMember, term: int) -> dict | None:
        answer = lambda success: {"term": self._state.current_term, "voteGranted": success}
        __logger__.debug(f"recv RequestVote({member.id}, {term})")
        if self.current_state is not self.leader:
            self._reset_election_timeout()
        if self._state.current_term > term:
            return answer(False)
        if self._state.current_term < term:
            self._state.current_term = term
            self._state.voted_for = None
            self.higher_term(term)
        if self._state.voted_for is None or member.id == self._state.voted_for:
            self._state.voted_for = member.id
            return answer(True)
        return answer(False)
