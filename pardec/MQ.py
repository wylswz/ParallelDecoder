import queue
from concurrent.futures import Future
from multiprocessing import Queue
import threading
import psutil
import time
from pardec.Interfaces.MessageQueue import MemoryTaskQueue


class ScatteringQueueManager(MemoryTaskQueue):

    class TaskWrapper:

        def __init__(self, future, manager, timeout=10):
            self.future: Future = future
            self.timeout = timeout
            self.manager: ScatteringQueueManager = manager
            self.future.add_done_callback(self.callback)

        def __del__(self):
            self.future = None
            self.manager = None

        def callback(self, future):
            res = future.result()
            try:
                self.manager._add_result(res, timeout=self.timeout)
            except queue.Full:
                pass
            self.future.cancel()


    def __init__(self,
                 cache_size,
                 memory_ratio=0.95):
        super(ScatteringQueueManager, self).__init__(cache_size, memory_ratio)
        self.result_queue = Queue(maxsize=cache_size)
        self.task_count = Queue(maxsize=cache_size)
        self.cond = threading.Condition()
        self._stop = False
        self.supervisor_thread:threading.Thread = threading.Thread(target=self._supervisor)
        self.supervisor_thread.start()

    def _supervisor(self):
        while True:
            if self._stop:
                break

            if self.full():
                pass
            else:
                try:
                    with self.cond:
                        self.cond.notify_all()
                except RuntimeError as e:
                    if str(e) == 'cannot notify on un-acquired lock':
                        pass

    def not_full(self):
        """

        :return:
        """
        return not self.full()

    def full(self):
        """
        Either task queue is full or memory is full
        :return:
        """
        avai = psutil.virtual_memory().available
        total = psutil.virtual_memory().total

        mem_full = avai / total < 1 - self.memory_ratio
        task_full = self.task_count.full()
        return task_full or mem_full

    def add_task(self, future, timeout=10):
        """
        Add a future object to the queue. Once finished, the result
        is pushed to the result queue
        :param future: concurrency Future object
        :param timeout: Timeout when pushing result
        :return:
        """
        try:
            with self.cond:
                self.cond.wait_for(self.not_full)
            self.task_count.put(1, timeout=timeout)
            ScatteringQueueManager.TaskWrapper(future, manager=self, timeout=timeout)
        except queue.Full:
            pass

    def _add_result(self, result, timeout=10):
        """

        :param result:
        :param timeout:
        :return:
        """
        self.result_queue.put(result, block=True, timeout=timeout)
        if not self.task_count.empty():
            self.task_count.get()

    def get_result(self, timeout=10):

        res = self.result_queue.get(block=True, timeout=timeout)
        if not self.task_count.empty():
            self.task_count.get()
        return res

    def report(self):
        status = {}
        status["Task Queue"] = "{0}/{1}".format(self.task_count.qsize(), self.cache_size)
        status["Result Queue"] = "{0}/{1}".format(self.result_queue.qsize(), self.cache_size)
        avai = round(psutil.virtual_memory().available / (1024 * 1024 * 1024), 2)
        total = round(psutil.virtual_memory().total / (1024 * 1024 * 1024), 2)
        status["Memory"] = "{0}/{1}".format(avai, total)
        return status

    def stop(self):
        self._stop = True
