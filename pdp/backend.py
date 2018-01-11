from enum import Enum, unique


@unique
class Backend(Enum):
    THREAD = 0
    PROCESS = 1


THREAD = Backend.THREAD
PROCESS = Backend.PROCESS


def check_backend(backend):
    if not isinstance(backend, Backend):
        raise TypeError('Wrong backend type, requires Backend type')
    else:
        if backend is PROCESS:
            raise ValueError('Process backend is not tested yet, use thread instead')
        return backend


def choose_backend(backend, thread_opt, process_opt):
    if check_backend(backend) is Backend.THREAD:
        return thread_opt
    else:
        return process_opt
