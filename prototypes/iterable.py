import time

def f():
    print('started')
    while True:
        print('before')
        time.sleep(5)
        yield 10
        print('after')


for a in f():
    print('value returned')