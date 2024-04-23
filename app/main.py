# Uncomment this to pass the first stage
import sys
import socket
from threading import Thread, Lock
from typing import Union
from enum import Enum
import time
import random
from dataclasses import dataclass

def millis():
    return time.time() * 1000

class RESP_parser(object):
    @classmethod
    def _parse_internal(cls, lines):
        if len(lines) == 0:
            return ['']

        startline = lines[0]
        lines.pop(0)

        params = []
        n = 1

        # integer
        if startline.startswith(':'):
            return n, int(startline[1:])

        # simple string
        elif startline.startswith('+'):
            return n, startline[1:]

        # bulk string
        elif startline.startswith('$'):
            nbytes = int(startline[1:])
            if len(lines) < 1:
                raise RuntimeError('Invalid data')
            if len(lines[0]) < nbytes:
                raise RuntimeError('Bulk string data fell short')
            param = lines[0][:nbytes]
            lines.pop(0)
            n += 1
            return n, param

        # array
        elif startline.startswith('*'):
            nparams = int(startline[1:])

            for i in range(nparams):
                nchild, paramschild = cls._parse_internal(lines)

                params.append(paramschild)
                n += nchild

        return n, params

    @classmethod
    def parse(cls, data):
        if len(data) == 0:
            return ['']

        lines = data.splitlines()
        nlines = len(lines)

        n, params = cls._parse_internal(lines)

        if nlines != n:
            raise RuntimeError('Invalid data')

        return params

    def __init__(self, data):
        self.params = self.parse(data)

class RESP_error(Enum):
    WRONG_ARGS = 1
    UNKNOWN_CMD = 2
    SYNTAX = 3
    CUSTOM = 4

class RESP_builder(object):

    @classmethod
    def null(cls):
        return '$-1\r\n'.encode()

    @classmethod
    def build(cls, data: Union[int, str, list], bulkstr: bool = True):
        typ = type(data)

        if typ == int:
            return f':{data}\r\n'

        elif typ == str:
            if len(data) == 0:
                return cls.null()

            if bulkstr:
                return f'${len(data)}\r\n{data}\r\n'.encode()
            else:
                return f'+{data}\r\n'.encode()

        elif typ == list:
            return f'*{len(data)}\r\n'.encode() +\
                   ''.encode().join(map(cls.build, data))

        else:
            raise TypeError(f"Unsupported type: {data_type}")

    @classmethod
    def error(
        cls, command: str = '', args: list = None,
        typ: RESP_error = RESP_error.WRONG_ARGS
    ):
        if typ == RESP_error.WRONG_ARGS:
            return '-ERR wrong number of arguments for '\
                    f'\'{command}\' command\r\n'.encode()

        elif typ == RESP_error.UNKNOWN_CMD:
            arg1 = args[0] if len(args) > 0 else ''
            return f'-ERR unknown command `{command}`, '\
                   f'with args beginning with: {arg1}\r\n'.encode()

        elif typ == RESP_error.SYNTAX:
            return '-ERR syntax error\r\n'.encode()

        elif typ == RESP_error.CUSTOM:
            return f'-ERR {args}\r\n'.encode()

        else:
            raise RuntimeError('Unknown error type')

@dataclass
class StoreElement(object):
    value: str
    expiry : float

class Store(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Store, cls).__new__(cls)
            # Put any initialization here.
        return cls._instance

    def __init__(self):
        self._lock: Lock = Lock()
        self._store: dict[str, StoreElement] = {}

    def expired(self, key):
        with self._lock:
            if key not in self._store:
                return True
    
            e = self._store[key]
            if e.expiry < 0:
                return False
            if e.expiry > int(time.time() * 1000):
                return False

            self._store.pop(key)
            return True

    def size(self):
        with self._lock:
            return len(self._store)

    def keys(self):
        with self._lock:
            return self._store.keys()

    # notice formatting
    def set(self, key, value, expiry=-1):
        with self._lock:
            self._store[key] = StoreElement(value, (int(time.time() * 1000) + expiry) if expiry != -1 else expiry)

    def get(self, key):
        with self._lock:
            if key not in self._store:
                return ''
            
            e = self._store[key]
            if e.expiry > 0 and e.expiry <= int(time.time() * 1000):
                self._store.pop(key)
                return ''
            return e.value

store = Store()

def process_command(command, args):
    response = None

    if command.upper() == 'COMMAND':
        response = RESP_builder.build(['ping', 'echo'])

    elif command.upper() == 'PING':
        response = RESP_builder.build('PONG', bulkstr=False)

    elif command.upper() == 'ECHO':
        if len(args) == 0:
            return RESP_builder.error(command)

        response = RESP_builder.build(args[0])

    elif command.upper() == 'SET':
        if len(args) < 2:
            return RESP_builder.error(command)

        exp = -1
        if len(args) > 2:
            expopt = args[2].upper()
            if expopt == 'PX' and len(args) == 4:
                exp = int(args[3])
            else:
                return RESP_builder.error(typ=RESP_error.SYNTAX)

        store.set(args[0], args[1], exp)

        response = RESP_builder.build('OK', bulkstr=False)

    elif command.upper() == 'GET':
        nargs = len(args)
        if nargs < 1:
            return RESP_builder.error(command)

        if nargs == 1:
            values = store.get(args[0])

        else:
            values = []
            for i in range(nargs):
                value = store.get(args[i])
                values.append(value)

        response = RESP_builder.build(values)

    return response

def handle_client(client_socket):
    with client_socket:
        while True:
            request_bytes = client_socket.recv(4096)
            if not request_bytes:
                break

            request = request_bytes.decode('utf-8')

            # Parse RESP packet
            command, *args = RESP_parser.parse(request)

            response = None

            try:
                response = process_command(command, args)

            except ValueError as v:
                sys.stderr.write(f'ValueError: {v}\n')
                response = RESP_builder.error(args='value is not an integer or out of range', typ=RESP_error.CUSTOM) 

            except Exception as e:
                sys.stderr.write(f'Exception occurred: {e}\n')

            finally:
                # Default response
                if response is None or response == '':
                    response =  RESP_builder.error(
                        command, args, typ=RESP_error.UNKNOWN_CMD
                    )

            client_socket.sendall(response)

def check_expiry():
    CHECK_INTERVAL = 30 # seconds

    while True:
        MAX_LOT_SIZE = 20

        store_keys = store.keys()
        store_size = len(store_keys)
        if store_size:
            lot_size = store_size if store_size < MAX_LOT_SIZE else MAX_LOT_SIZE
            selected = random.sample(store_keys, lot_size)
            for key in selected:
                print(f'Checking {key} expiry: {store.expired(key)}')

        time.sleep(CHECK_INTERVAL)

def main():
    # Start thread to check for key expiry
    expiry_thread = Thread(target=check_expiry)
    expiry_thread.start()

    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage

    server_socket = socket.create_server(("localhost", 6379),
                                         backlog=2,
                                         reuse_port=True)

    MAX_CONCURRENT_CONN = 5
    client_threads = []
    num_conn = 0

    while num_conn < MAX_CONCURRENT_CONN:
        client_socket, addr = server_socket.accept() # wait for client
        print('Incoming connection from', addr)
        t = Thread(target=handle_client, args=(client_socket,))
        client_threads.append(t)
        t.start()
        num_conn += 1

    for t in client_threads: t.join()

    expiry_thread.join()

if __name__ == "__main__":
    main()
