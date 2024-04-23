# Uncomment this to pass the first stage
import sys
import socket
from threading import Thread
from typing import Union
from enum import Enum

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
        cls, command: str, args: list = None,
        typ: RESP_error = RESP_error.WRONG_ARGS
    ):
        if typ == RESP_error.WRONG_ARGS:
            return '-ERR wrong number of arguments for '\
                    f'\'{command}\' command\r\n'.encode()

        elif typ == RESP_error.UNKNOWN_CMD:
            arg1 = args[0] if len(args) > 0 else ''
            return f'-ERR unknown command `{command}`, '\
                   f'with args beginning with: {arg1}\r\n'.encode()

        else:
            raise RuntimeError('Unknown error type')

store = {}

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
                if command.upper() == 'COMMAND':
                    response = RESP_builder.build(['ping', 'echo'])

                elif command.upper() == 'PING':
                    response = RESP_builder.build('PONG', bulkstr=False)

                elif command.upper() == 'ECHO':
                    if len(args) == 0:
                        response =  RESP_builder.error(command)
                    else:
                        response = RESP_builder.build(args[0])

                elif command.upper() == 'SET':
                    if len(args) < 2:
                        response =  RESP_builder.error(command)
                    else:
                        global store
                        store[args[0]] = args[1]
                        response = RESP_builder.build('OK', bulkstr=False)

                elif command.upper() == 'GET':
                    nargs = len(args)
                    if nargs < 1:
                        response =  RESP_builder.error(command)
                    else:
                        if nargs == 1:
                            values = store.get(args[0], '')
                        else:
                            values = []
                            for i in range(nargs):
                                value = store.get(args[i], '')
                                values.append(value)

                        response = RESP_builder.build(values)

            except Exception as e:
                sys.stderr.write(f'Exception occurred: {e}\n')

            finally:
                # Error
                if response is None or response == '':
                    response =  RESP_builder.error(
                        command, args, typ=RESP_error.UNKNOWN_CMD
                    )

            client_socket.sendall(response)

def main():
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

if __name__ == "__main__":
    main()
