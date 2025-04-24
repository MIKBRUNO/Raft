from typing import Callable, Awaitable

from statemachine import State, StateMachine

import asyncio

import logging

from pydantic import ValidationError

from abc import ABC, abstractmethod

from .raft_models import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
    RaftRPCMessage,
    RaftRPCType,
    RaftLogItem,
    ClientCall,
    ClientCallResponse
)
from .raft_state import RaftState
from .networking import Network, NetworkMember
from .rpc import RPCManager, RPCCallable, RPCTerminatedException


__logger__ = logging.getLogger(__name__)


class RaftSM(ABC):
    @abstractmethod
    def apply_commands(commands: list[RaftLogItem]): ...


class ElectionCompleteException(BaseException):
    pass


class RaftObject(StateMachine):
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
        self._client_queue: asyncio.Queue[dict] = asyncio.Queue()
        self._sm: RaftSM | None = None
        self._match_indecies: dict[str, int] = dict()
        self._network = network
        self._members = {m.id: m for m in self._network.members}
        self._quorum = (len(self._members) + 1) // 2 + 1
        self._rpcm = RPCManager(network, retry_policy=retry_rpc_policy)
        self._state = RaftState(self._network.this)
        self._heartbeat_policy = heartbeat_policy if heartbeat_policy else lambda: 0
        self._election_timeout_policy = election_timeout_policy
        self._election_timeout: asyncio.Timeout | None = None
        self._running_task: asyncio.Task | None = None
        self._new_task: asyncio.Task | None = None


    def set_raft_sm(self, sm: RaftSM):
        self._sm = sm


    async def add_log_item(self, command: dict):
        await self._client_queue.put(command)

    
    def _update_sm(self):
        last_applied = self._state.last_applied
        commit_index = self._state.commit_index
        if self._sm and commit_index > last_applied:
            self._state.last_applied = commit_index
            self._sm.apply_commands(self._state.log[last_applied + 1:commit_index + 1])


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


    async def _listen_rpc(self):
        while True:
            await self._rpcm.listen(self._handle_rpc_call)


    def _reset_election_timeout(self):
        if self._election_timeout:
            self._election_timeout.reschedule(asyncio.get_event_loop().time() + self._election_timeout_policy())


    async def _run_follower(self):
        try:
            async with asyncio.timeout(self._election_timeout_policy()) as t:
                self._election_timeout = t
                await self._listen_rpc()
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
                    tg.create_task(self._listen_rpc())
        except asyncio.TimeoutError:
            self._election_timeout = None
            await self.election_timeout()
        finally:
            self._election_timeout = None


    async def _run_leader(self):
        rpcs = [self._rpcm.get_rcp_endpoint(m) for m in self._members.values()]
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._listen_rpc())
                tg.create_task(self._listen_client())
                for r in rpcs:
                    tg.create_task(self._append_entries(r))
        except* RPCTerminatedException:
            pass


    async def _request_vote(self, rpc: RPCCallable) -> bool:
        log = self._state.log
        request = RequestVote(term=self._state.current_term, lastLogIndex=last_index(log), lastLogTerm=last_term(log))
        __logger__.debug(f"call RequestVote({rpc}, {request})")
        result = await rpc.call(request)
        __logger__.debug(f"recv {result}")
        if not result:
            return False
        try:
            response = RequestVoteResponse.model_validate(result)
            if response.term > self._state.current_term:
                self.higher_term(response.term)
            return response.voteGranted
        except ValidationError as e:
            __logger__.error(e)
            return False


    async def _run_election(self):
        __logger__.debug(f"Started election with {self._quorum} quorum on {self._state.current_term} term")
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
                    if votes >= self._quorum:
                        raise ElectionCompleteException()
                for r in rpcs:
                    tg.create_task(request(r))
        except* ElectionCompleteException:
            pass
        except* RPCTerminatedException:
            terminated = True
        if terminated:
            return
        if votes >= self._quorum:
            self.win_election()


    async def _listen_client(self):
        while True:
            cmd = await self._client_queue.get()
            self._client_queue.task_done()
            log = self._state.log
            log.append(RaftLogItem(term=self._state.current_term, command=cmd))
            self._state.log = log


    async def _try_commit(self):
        log = self._state.log
        matches = list(self._match_indecies.values()) + [last_index(log)]
        commit_index = self._state.commit_index
        for i in range(commit_index + 1, max(matches) + 1):
            if len([m for m in matches if m >= i]) >= self._quorum:
                commit_index = commit_index + 1
        self._state.commit_index = commit_index
        self._update_sm()

    async def _append_entries(self, rpc: RPCCallable):
        next_index = len(self._state.log)
        self._match_indecies[rpc.id] = -1
        while True:
            log = self._state.log
            items = log[next_index:]
            request = AppendEntries(
                term=self._state.current_term,
                prevLogIndex=next_index - 1,
                prevLogTerm=last_term(log, next_index),
                entries=items,
                leaderCommit=self._state.commit_index
            )
            __logger__.debug(f"call AppendEntries({rpc}, {request})")
            result = await rpc.call(request)
            __logger__.debug(f"recv {result}")
            try:
                if result:
                    response = AppendEntriesResponse.model_validate(result)
                    if response.term > self._state.current_term:
                        self.higher_term(response.term)
                    if response.success:
                        self._match_indecies[rpc.id] = last_index(log)
                        next_index = len(log)
                        await self._try_commit()
                    else:
                        next_index = next_index - 1
                        continue
            except ValidationError as e:
                __logger__.error(e)
            await asyncio.sleep(self._heartbeat_policy())

    
    async def _handle_rpc_call(self, member: NetworkMember, args: dict) -> AppendEntriesResponse | RequestVoteResponse:
        try:
            msg = RaftRPCMessage.model_validate(args)
            if msg.raft_type == RaftRPCType.APPENDENTRIES:
                append_entries = AppendEntries.model_validate(args)
                return await self._handle_append_entries(member, append_entries)
            elif msg.raft_type == RaftRPCType.REQUESTVOTE:
                requets_vote = RequestVote.model_validate(args)
                return await self._handle_request_vote(member, requets_vote)
            elif msg.raft_type == RaftRPCType.CLIENTCALL:
                client_call = ClientCall.model_validate(args)
                return await self._handle_client_call(member, client_call)
        except ValidationError as e:
            __logger__.error(e)
            return None

    
    async def _handle_append_entries(self, member: NetworkMember, command: AppendEntries) -> AppendEntriesResponse:
        answer = lambda success: AppendEntriesResponse(term=self._state.current_term, success=success)
        __logger__.debug(f"recv {command}")
        if self.current_state is not self.leader:
            self._reset_election_timeout()
        if self._state.current_term > command.term:
            return answer(False)
        if self._state.current_term < command.term:
            self._state.current_term = command.term
            self._state.voted_for = None
            self.higher_term(command.term)
        if self._state.current_term == command.term:
            self.new_leader()
        log = self._state.log
        if command.prevLogIndex != -1 and (len(log) <= command.prevLogIndex or log[command.prevLogIndex].term != command.prevLogTerm):
            return answer(False)
        log = log[:command.prevLogIndex + 1]
        log.extend(command.entries)
        self._state.log = log
        if command.leaderCommit > self._state.commit_index:
            self._state.commit_index = min(command.leaderCommit, last_index(log))
            self._update_sm()
        if self._client_queue.qsize() > 0:
            cmd = await self._client_queue.get()
            self._client_queue.task_done()
            request = ClientCall(command=cmd)
            __logger__.debug(f"call ClientCall({member}, {request})")
            asyncio.create_task(self._rpcm.call(member, request))
        return answer(True)


    async def _handle_request_vote(self, member: NetworkMember, command: RequestVote) -> RequestVoteResponse:
        answer = lambda success: RequestVoteResponse(term=self._state.current_term, voteGranted=success)
        __logger__.debug(f"recv {command}")
        if self.current_state is not self.leader:
            self._reset_election_timeout()
        if self._state.current_term > command.term:
            return answer(False)
        if self._state.current_term < command.term:
            self._state.current_term = command.term
            self._state.voted_for = None
            self.higher_term(command.term)
        log = self._state.log
        if (self._state.voted_for is None or member.id == self._state.voted_for) \
            and command.lastLogIndex >= last_index(log) \
            and command.lastLogTerm >= last_term(log):
            self._state.voted_for = member.id
            return answer(True)
        return answer(False)


    async def _handle_client_call(self, member: NetworkMember, command: ClientCall) -> ClientCallResponse:
        answer = ClientCallResponse()
        __logger__.debug(f"recv {command}")
        await self._client_queue.put(command.command)
        return answer


def last_index(log: list[RaftLogItem]):
    return len(log) - 1


def last_term(log: list[RaftLogItem], pos: int = 0):
    if len(log) == 0:
        return -1
    else:
        return log[pos - 1].term
