import time
from memory_profiler import profile
import gc


@profile
def my_func():
    a = [1] * (10 ** 6)
    b = [2] * (2 * 10 ** 7)
    time.sleep(10)
    del b
    # gc.collect()
    print len(a)
    del a
    print "+++++++++"
    gc.collect()
    time.sleep(10)
if __name__ == '__main__':
    my_func()
