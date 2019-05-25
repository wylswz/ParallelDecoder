import queue
import threading
from concurrent.futures import ProcessPoolExecutor
import concurrent
import traceback
import multiprocessing
from pardec.MQ import ScatteringQueueManager
from ctypes import c_bool

def default_decoder(x):
    return x


def default_get_args(x):
    return (x,)


class ParallelDecoder:

    def __init__(self,
                 generator,
                 num_workers=2,
                 cache_size=10,
                 get_args=default_get_args,
                 decoder=default_decoder,
                 deque_timeout=10000,
                 enque_timeout=10000
                 ):
        """

        :param generator: The generator you wanna wrap
        :param num_workers:
        :param cache_size: The size of queue
        :param get_args: the function that takes one sample and return a set of args
        :param decoder: Convert a sample to a concrete data instance
        :param deque_timeout: Wait time of getting data out of empty queue
        :param enque_timeout: Wait time of putting data into full queue
        """

        self._stop = multiprocessing.Value(c_bool, False, lock=False)

        self.generator = generator
        self.pool = ProcessPoolExecutor(max_workers=num_workers)
        self.queue = ScatteringQueueManager(cache_size=cache_size)
        self.decoder = decoder
        self.get_args = get_args
        self.enque_timeout = enque_timeout
        self.deque_timeout = deque_timeout
        self.temp_cache = [None for i in range(cache_size)]
        self.feeder_p = threading.Thread(target=self._feeding_queue)
        self.feeder_p.daemon = True
        self.feeder_p.start()


    def report(self):
        return self.queue.report()

    def _feeding_queue(self):
        """
        Keep fetching data from the generator, decode it and generate future objects
        :return:
        """

        while not self._stop.value:
            if self._stop.value:
                break

            try:
                sample = next(self.generator)
                future = self.pool.submit(self.decoder,
                                          *self.get_args(sample))
                self.queue.add_task(future, timeout=self.enque_timeout)
            except StopIteration:

                break
            except RuntimeError:

                break
            except TypeError:

                break



    def _consuming_queue(self):
        """
        When __next__ is invoked, attempt fetch item from the queue.
        If the queue is empty after a period of waiting or closed, Stop Iteration
        is raised
        :return:
        """
        if self._stop:
            raise StopIteration
        else:
            try:
                res = self.queue.get_result(timeout=self.deque_timeout)
                return res
            except queue.Empty:
                raise StopIteration
            except OSError as ose:
                if str(ose) == "handle is closed":
                    raise StopIteration

    def _shutdown(self):
        """

        :param clean_cache:
        :return:
        """
        self._stop.value = True
        self.pool.shutdown(True)

    def stop(self):
        """
        Start a thread to send stop signals

        :return:
        """

        self._shutdown()
        while True:
            try:
                self._consuming_queue()
            except StopIteration:
                break
        self.queue.stop()

    def __iter__(self):
        return self

    def __next__(self):
        return self._consuming_queue()
