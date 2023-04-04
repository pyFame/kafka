
from threading import Thread, enumerate as threads, current_thread,Lock
from multiprocessing import Process

def keepAlive(fc):
    def inner(*args):
        t = Thread(name=fc.__name__, target=fc, args=args, daemon=True)
        t.start()
    return inner

def undead(fc):
    def inner(*args):
        p = Process(name=fc.__name__, target=fc, args=args, daemon=True)
        p.start()
    return inner
