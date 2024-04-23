# Uncomment this to pass the first stage
import sys
import socket
from threading import Thread, Lock
from typing import Union
from enum import Enum
import time
import random
from dataclasses import dataclass
import argparse
import secrets

def millis():
    return int(time.time() * 1000)

class ServerRole(Enum):
    MASTER = 'master'
    SLAVE = 'slave'

class Server(object):
    _instance = None

    def __init__(self, port: int = 6379, role: ServerRole = ServerRole.MASTER):
        self._port = port
        self._role = role
        self.master_replid = secrets.token_hex(20)
        self.master_repl_offset = 0

    def __new__(cls, port: int = 6379, role: ServerRole = ServerRole.MASTER):
        if cls._instance is None:
            cls._instance = super(Server, cls).__new__(cls)
            cls._instance.__init__(port, role)
        return cls._instance

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, port_: int):
        self._port = port_

    @property
    def role(self):
        return self._role.value

    @role.setter
    def role(self, role_: ServerRole):
        self._role = role_

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
            if e.expiry > millis():
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
            self._store[key] = StoreElement(value, (millis() + expiry) if expiry != -1 else expiry)

    def get(self, key):
        with self._lock:
            if key not in self._store:
                return ''
            
            e = self._store[key]
            if e.expiry > 0 and e.expiry <= millis():
                self._store.pop(key)
                return ''
            return e.value

store = Store()
server = Server()

def process_command(command, args):
    global store
    response = None

    command = command.upper()

    if command == 'COMMAND':
        response = RESP_builder.build(['ping', 'echo'])

    elif command == 'PING':
        response = RESP_builder.build('PONG', bulkstr=False)

    elif command == 'ECHO':
        if len(args) == 0:
            return RESP_builder.error(command)

        response = RESP_builder.build(args[0])

    elif command == 'INFO':
        subcommand = ''
        if len(args) >= 1:
            subcommand = args[0].upper()
            
        if subcommand == 'REPLICATION':
            payload = f'# Replication\r\n'\
                      f'role:{server.role}\r\n'\
                      f'master_replid:{server.master_replid}\r\n'\
                      f'master_repl_offset:{server.master_repl_offset}\r\n'
            response = RESP_builder.build(payload)

        else:
            response = RESP_builder.error(args='not implemented', typ=RESP_error.CUSTOM)

    elif command == 'REPLCONF':
        # Hardcode +OK\r\n
        response = RESP_builder.build('OK', bulkstr=False)

    elif command == 'SET':
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

    elif command == 'GET':
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
    global store
    CHECK_INTERVAL = 300 # seconds

    while True:
        MAX_LOT_SIZE = 20

        store_keys = store.keys()
        store_size = len(store_keys)
        if store_size > 0:
            lot_size = store_size if store_size < MAX_LOT_SIZE else MAX_LOT_SIZE
            selected = random.sample(store_keys, lot_size)
            for key in selected:
                #print(f'Checking {key} expiry: {store.expired(key)}')
                store.expired(key)

        time.sleep(CHECK_INTERVAL)

def main():
    global server

    parser = argparse.ArgumentParser(description='Dummy Redis Server')
    parser.add_argument('--port', type=int, help='Port number')
    parser.add_argument('--replicaof', nargs=2, metavar=('host', 'port'), help='Set the host and port of the master to replicate')

    args = parser.parse_args()

    # Get port number
    if args.port:
        server.port = args.port

    print(f'Running on port: {server.port}')

    # Get master host and port
    if args.replicaof:
        server.role = ServerRole.SLAVE
        master_host = args.replicaof[0]
        master_port = args.replicaof[1]
        print(f'Set as slave replicating {args.replicaof[0]}:{args.replicaof[1]}')

        print('Connecting to master')

        master_socket = socket.create_connection((master_host, master_port))

        with master_socket:
            ping = RESP_builder.build(['ping'])
            master_socket.sendall(ping)
            master_socket.recv(4096)

            replconf = RESP_builder.build(['REPLCONF', 'listening-port', str(server.port)])
            master_socket.sendall(replconf)
            master_socket.recv(4096)

            replconf = RESP_builder.build(['REPLCONF', 'capa', 'psync2'])
            master_socket.sendall(replconf)
            master_socket.recv(4096)

    # Start thread to check for key expiry
    expiry_thread = Thread(target=check_expiry)
    expiry_thread.start()

    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage

    server_socket = socket.create_server(("localhost", server.port),
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
