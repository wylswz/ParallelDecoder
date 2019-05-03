from multiprocessing import Queue
import threading,time, queue

from concurrent.futures import ProcessPoolExecutor

def default_decoder(x):
    return x

def default_get_args(x):
    return (x,)

class ParallelDecoder:

    def __init__(self,
                 generator,
                 num_workers=4,
                 cache_size=1000,
                 get_args=default_get_args,
                 decoder=default_decoder,
                 deque_timeout=1,
                 enque_timeout=10000,
                 cache_full_wait_time=5
                 ):
        """

        :param generator: The generator you wanna wrap
        :param num_workers:
        :param cache_size: The size of queue
        :param get_args: the function that takes one sample and return a set of args
        :param decoder: Convert a sample to a concrete data instance
        :param deque_timeout: Wait time of getting data out of empty queue
        :param enque_timeout: Wait time of putting data into full queue
        :param cache_full_wait_time: The wait time of fetching new data if the queue is full
        """

        self._stop = False
        self._terminated = False

        self.generator = generator
        self.pool = ProcessPoolExecutor(max_workers=num_workers)
        self.cache_size = cache_size
        self.queue = Queue(maxsize=cache_size)
        self.decoder = decoder
        self.get_args = get_args
        self.enque_timeout = enque_timeout
        self.deque_timeout = deque_timeout
        self.cache_full_wait_time = cache_full_wait_time
        self.temp_cache = [None for i in range(cache_size)]
        self.feeder_p = threading.Thread(target=self._feeding_queue)
        self.feeder_p.daemon = True
        self.feeder_p.start()


    def _queue_future(self, future):
        """
        Get a result from Future object and push it into the queue
        :param future:
        :return:
        """
        res = future.result()
        try:
            self.queue.put(res, timeout=self.enque_timeout)
        except AssertionError:
            pass
        except RuntimeError:
            pass

    def _feeding_queue(self):
        """
        Keep fetching data from the generator, decode it and generate future objects
        :return:
        """

        while not self._stop:
            if self._stop:
                break
            if not self.queue.full():
                try:
                    sample = next(self.generator)
                    future = self.pool.submit(self.decoder,
                                              *self.get_args(sample))
                    future.add_done_callback(self._queue_future)
                except StopIteration:
                    break
                except RuntimeError:
                    break
                except TypeError:
                    break
            else:
                time.sleep(self.cache_full_wait_time)

    def _consuming_queue(self):
        """
        When __next__ is invoked, attempt fetch item from the queue.
        If the queue is empty after a period of waiting or closed, Stop Iteration
        is raised
        :return:
        """
        if self._terminated:
            raise StopIteration
        else:
            try:
                res = self.queue.get(block=True, timeout=self.deque_timeout)
                return res
            except queue.Empty:
                raise StopIteration
            except OSError as ose:
                if str(ose) == "handle is closed":
                    raise StopIteration



    def _shutdown(self, clean_cache):
        """

        :param clean_cache:
        :return:
        """
        self._stop = True

        if clean_cache:
            self._terminated = True
            self.queue.close()
        self.pool.shutdown(True)



    def stop(self, clean_cache):
        """
        Start a thread to send stop signals

        :param clean_cache: If true, all the unfinished futures or unfetched queue data
            will be ignored
        :return:
        """
        shutdown_thread = threading.Thread(target=self._shutdown, args=(clean_cache,))
        shutdown_thread.start()

    def __iter__(self):
        return self

    def __next__(self):
        return self._consuming_queue()

