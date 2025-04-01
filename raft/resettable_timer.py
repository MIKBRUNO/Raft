from typing import Callable, Awaitable
import asyncio


class ResettableTimer:
    def __init__(self, timeout: float, callback: Callable[[], Awaitable], start=True):
        self._timeout = timeout
        self._cb = callback
        self._event = asyncio.Event()
        self._active = False
        self._task: asyncio.Task | None = None
        if start:
            self._active = True
            self._task = asyncio.create_task(self._coro())


    def reset(self, timeout: float | None = None):
        if timeout:
            self._timeout = timeout
        self._active = True
        if self._task:
            print("not created")
            self._event.set()
        else:
            print("actually created")
            self._task = asyncio.create_task(self._coro())

    
    def stop(self):
        self._active = False
        self._event.set()


    async def _coro(self):
        while self._active:
            try:
                async with asyncio.timeout(self._timeout):
                    await self._event.wait()
            except asyncio.TimeoutError:
                await self._cb()
                self._active = False
            self._event.clear()
        self._task = None
