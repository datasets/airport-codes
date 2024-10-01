import itertools
import os
import multiprocessing as mp
import threading
import queue

from ..helpers import ResourceMatcher
from .. import PackageWrapper, ResourceWrapper


def init_mp(num_processors, row_func, q_in, q_internal):
    q_out = mp.Queue()
    processes = [mp.Process(target=work, args=(q_in, q_out, row_func)) for _ in range(num_processors)]
    for process in processes:
        process.start()
    t_fetch = threading.Thread(target=fetcher, args=(q_out, q_internal, num_processors))
    t_fetch.start()
    return (processes, t_fetch)


def fini_mp(processes, t_fetch):
    for process in processes:
        try:
            process.join(timeout=10)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass
        finally:
            if hasattr(process, 'close'):
                process.close()
    t_fetch.join()


def producer(res, q_in, q_internal, num_processors, predicate):
    try:
        for row in res:
            if predicate(row):
                q_in.put(row)
            else:
                q_internal.put(row)
        for _ in range(num_processors):
            q_in.put(None)
    except Exception:
        q_internal.put(None)
        return 1
    return 0


def fetcher(q_out, q_internal, num_processors):
    expected_nones = num_processors
    while True:
        row = q_out.get()
        if row is None:
            expected_nones -= 1
            if expected_nones == 0:
                q_internal.put(None)
                break
            continue
        q_internal.put(row)


def work(q_in: mp.Queue, q_out: mp.Queue, row_func):
    pid = os.getpid()
    try:
        while True:
            row = q_in.get()
            if row is None:
                break
            try:
                row_func(row)
            except Exception as e:
                print(pid, 'FAILED TO RUN row_func {}\n'.format(e))
                pass
            q_out.put(row)
    except Exception:
        pass
    finally:
        q_out.put(None)


def fork(res, row_func, num_processors, predicate):
    predicate = predicate or (lambda x: True)
    for row in res:
        if predicate(row):
            res = itertools.chain([row], res)
            q_in = mp.Queue()
            q_internal = queue.Queue()
            t_prod = threading.Thread(target=producer, args=(res, q_in, q_internal, num_processors, predicate))
            t_prod.start()

            processes, t_fetch = init_mp(num_processors, row_func, q_in, q_internal)

            while True:
                row = q_internal.get()
                if row is None:
                    break
                yield row
            t_prod.join()
            fini_mp(processes, t_fetch)
        else:
            yield row


def parallelize(row_func, num_processors=None, resources=None, predicate=None):
    num_processors = num_processors or 2*os.cpu_count()

    def func(package: PackageWrapper):
        yield package.pkg
        matcher = ResourceMatcher(resources, package.pkg)

        res: ResourceWrapper
        for res in package:
            if matcher.match(res.res.name):
                yield fork(res, row_func, num_processors, predicate)
            else:
                yield res

    return func
