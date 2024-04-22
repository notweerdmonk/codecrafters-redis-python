# Uncomment this to pass the first stage
import sys
import socket
from threading import Thread

class redis_parser(object):
    @classmethod
    def parse(self, data):
        if len(data) == 0:
            return ['']

        lines = data.splitlines()
        startline = lines[0]
        lines.pop(0)

        issimple = True if startline[0] == '+' else False
        isarray = True if startline[0] == '*' else False

        if issimple:
            if lines[i][0] != '+':
                raise RuntimeError('Invalid data')

            return [lines[i][1:]]

        nparams = nparams = int(startline[1:]) if isarray else 1

        params = []
        for i in range(nparams):
            if lines[i * 2][0] != '$':
                raise RuntimeError('Invalid data')

            nbytes = int(lines[i * 2][1:])
            param = lines[i * 2 + 1][:nbytes]
            params.append(param)

        return params

    def __init__(self, data):
        self.params = self.parse(data)

def handle_client(client_socket):
    with client_socket:
        while True:
            request_bytes = client_socket.recv(4096)
            if not request_bytes:
                break

            request = request_bytes.decode('utf-8')

            # Parse redis packet
            command, *args = redis_parser.parse(request)

            response = None

            try:
                if command.upper() == 'COMMAND':
                    response = b'*2\r\n$4\r\nping\r\n$4\r\necho\r\n'

                elif command.upper() == 'PING':
                    response = b'+PONG\r\n'

                elif command.upper() == 'ECHO':
                    if len(args) > 0:
                        arglen = len(args[0])
                        response = f'${arglen}\r\n{args[0]}\r\n'.encode()

            except Exception as e:
                sys.stderr.write(f'Exception occurred: {e}\n')

            finally:
                # Error
                if response is None:
                    arg1 = args[0] if len(args) > 0 else ''
                    response = f'-ERR unknown command `{command}`, '\
                               f'with args beginning with: {arg1}\r\n'.encode()

            client_socket.sendall(response)

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage

    server_socket = socket.create_server(("localhost", 6379), backlog=2, reuse_port=True)

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
