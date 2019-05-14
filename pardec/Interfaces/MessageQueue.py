class MemoryTaskQueue:
    def __init__(self, cache_size, memory_ratio):
        """

        :param cache_size: Max size of the queue
        :param memory_ratio: When memory usage is larger or equal than this
            ratio, the queue is identified as full
        """
        self.cache_size = cache_size
        self.memory_ratio = memory_ratio

    def get_result(self, timeout=10):
        """
        Get result if they are available in the queue. If not, the process blocks for
        a time out
        :param timeout: Time to wait
        :return: Any object
        """
        raise NotImplementedError

    def add_task(self, task, timeout=10):
        """

        :param task: The task should be a future-like object(All the methods in future should be implemented)
        :param timeout: Time to wait when the queue is full
        :return: None
        """
        raise NotImplementedError

    def full(self):
        """
        Whether the queue is full or not. This can either indicate queue is full or memory is full
        :return:
        """
        raise NotImplementedError

    def report(self):
        """
        Report the status of the Queue
        :return:
        """
        raise NotImplementedError
