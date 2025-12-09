import asyncio
from typing import Coroutine, Any, Set, Dict
from loguru import logger


class TaskManager:
    """TaskManager是一个任务管理器，支持动态添加和删除任务
    - TaskManager用于管理一组 asyncio 任务.
    - 与py3.11新增的TaskGroup类似，多用来管理常驻任务
    - 由于目前最低版本是 3.12, 所以直接优化逻辑使用TaskGroup来实现TaskManager
    - 不同于TaskGroup的是, TaskManager可以动态(在async with代码库之外)添加和删除任务, 这使得它在某些场景下更加灵活.
    - 另外会加入一些日志, 记录任务添加, 取消或报错的相关信息

    """

    def __init__(self):
        self._timeout = 3
        self._q = asyncio.Queue()
        self._tg = asyncio.TaskGroup()

    @property
    def tasks(self) -> Dict[str, asyncio.Task]:
        return {task.get_name(): task for task in self._tg._tasks}

    def add_task(self, coro: Coroutine[Any, None, None], *, name=None):
        logger.info(f"[add_task] add task - name={name}")
        self._q.put_nowait((coro, name))

    def del_task(self, task: asyncio.Task):
        logger.info(f"[add_task] del task - name={task.get_name()}")
        task.cancel()

    async def run(self):
        def _log_error(task):
            if task.cancelled():
                logger.info(f"Task<{task.get_name()}> being cancelled - task={task}")
            elif e := task.exception():
                logger.exception(f"Task<{task.get_name()}> run failed - exception={e} task={task}")
            else:
                logger.info(f"Task<{task.get_name()}> done - task={task}")

        async with self._tg:
            # init
            for _ in range(self._q.qsize()):
                coro, name = self._q.get_nowait()
                task: asyncio.Task = self._tg.create_task(coro, name=name)
                task.add_done_callback(_log_error)

            # wait for new tasks or done
            while self.tasks:
                try:
                    coro, name = await asyncio.wait_for(self._q.get(), timeout=self._timeout)
                    task: asyncio.Task = self._tg.create_task(coro, name=name)
                    task.add_done_callback(_log_error)
                except asyncio.TimeoutError:
                    continue
