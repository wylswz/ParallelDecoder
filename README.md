# ParallelDecoder

## Introduction
This is a wrapper for normal generator which supports multiprocess
custom decoding. All you need to do is providing a generator, a decoding
 function, some parameters, then it boosts multi-core performance 
 for you automatically.
 
## Example
```python
import time
from pardec import ParallelDecoder
def my_decoder(x):
    time.sleep(4)
    return x

def my_get_args(x):
    return (x, )

my_gen = (i for i in range(1000)) 
# This is your generator

par_dec = ParallelDecoder(my_gen,num_workers=12,cache_size=200)
# Create a decoder with 12 cores and queue size of 200
next(par_dec)
par_dec.stop(False)


``` 

## Methods 
- constructor
    - generator: Your generator that yield data sample
    - num_workers: Number of workers
    - cache_size: Size of queue
    - get_args: The function that take a sample and return a set of args
    - decoder: The function that convert a sample to a concrete piece of data
    - deque_timeout: Wait time of getting data out of empty queue
    - enque_timeout: Wait time of putting data into full queue
    - cache_full_wait_time: The wait time of fetching new data if the queue is full

- stop()
    - clean_cache: If True, all the unfinished decoding will be 
                    aborted and the cache will be cleaned.

