from enum import Enum, unique


@unique
class Backend(Enum):
    THREAD = 0
    PROCESS = 1


def check_backend(backend):
    if not isinstance(backend, Backend):
        raise ValueError('Wrong backend')
    else:
        return backend


def choose_backend(backend, thread_opt, process_opt):
    if backend is Backend.THREAD:
        return thread_opt
    else:
        return process_opt
