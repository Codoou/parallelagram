import multiprocessing as mp
import time


print("Number of processors: ", mp.cpu_count())


def a():
    while True:
        print("a")
        time.sleep(3)

def b():
    while True:
        print("b")
        time.sleep(3)


if __name__ == '__main__':
    p1 = mp.Process(name='p1', target=a)
    p2 = mp.Process(name='p2', target=b)

    p1.start()
    p2.start()