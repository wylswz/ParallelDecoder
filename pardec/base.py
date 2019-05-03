from multiprocessing import Queue
import threading
from concurrent.futures import ProcessPoolExecutor


class ParallelDecoder:

    def __init__(self,
                 generator,
                 num_workers=4,
                 cache_size=1000,
                 get_args=lambda x: (x,),
                 decoder=lambda x: x,
                 deque_timeout=5,
                 enque_timeout=300
                 ):
        self.generator = generator
        self.pool = ProcessPoolExecutor(max_workers=num_workers)
        self.cache_size = cache_size
        self.queue = Queue(maxsize=cache_size)
        self.decoder = decoder
        self.get_args = get_args
        self.temp_cache = [None for i in range(cache_size)]
        self.feeder_p = threading.Thread(target=self._feeding_queue)
        self.feeder_p.daemon = True
        self.feeder_p.start()
        self.enque_timeout = enque_timeout
        self.deque_timeout = deque_timeout

    def _queue_future(self, future):
        self.queue.put(future.result(),timeout=self.enque_timeout)

    def _feeding_queue(self):
        while not self.queue.full():
            try:
                sample = next(self.generator)
                future = self.pool.submit(self.decoder,
                                          *self.get_args(sample))
                future.add_done_callback(self._queue_future)
            except StopIteration:
                break

    def _consuming_queue(self):
        return self.queue.get(block=True, timeout=self.deque_timeout)

    def __iter__(self):
        return self
    
    def __next__(self):
        return self._consuming_queue()
