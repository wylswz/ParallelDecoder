from multiprocessing import Queue, Value, Lock
import threading,time, queue
import psutil

from concurrent.futures import Future


class TaskWrapper:

    def __init__(self, future, manager, timeout=10):
        self.future: Future = future
        self.timeout = timeout
        self.manager: QueueManager = manager
        self.future.add_done_callback(self.callback)

    def __del__(self):
        pass

    def callback(self, future):
        res = future.result()
        self.manager.add_result(res, timeout=self.timeout)
        self.__del__()


class QueueManager:
    def __init__(self,
                 cache_size):
        self.task_count: Queue = Queue(maxsize=cache_size)
        self.cache_size = cache_size
        self.result_queue = Queue(maxsize=cache_size)
        self.memory_ratio = 0.9

    def has_memory(self):
        avai = psutil.virtual_memory().available
        total = psutil.virtual_memory().total
        return avai/total > 1 - self.memory_ratio

    def full(self):
        task_full = self.task_count.full()
        mem_full = not self.has_memory()

        return task_full or mem_full

    def add_task(self, future, timeout=10):
        try:

            self.task_count.put(1, timeout=timeout)
            tw = TaskWrapper(future, manager=self, timeout=timeout)
        except queue.Full:
            pass

    def add_result(self, result, timeout=10):
        self.result_queue.put(result, block=True, timeout=timeout)
        if not self.task_count.empty():
            self.task_count.get()


    def get_result(self, timeout=10):

        res = self.result_queue.get(block=True, timeout=timeout)
        return res

    def report(self):
        status = {}
        status["Task Queue"] = "{0}/{1}".format(self.task_count.qsize(), self.cache_size)
        status["Result Queue"] = "{0}/{1}".format(self.result_queue.qsize(), self.cache_size)


