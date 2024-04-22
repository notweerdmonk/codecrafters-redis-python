# Uncomment this to pass the first stage
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
        iscommand = True if startline[0] == '*' else False

        nparams = nparams = int(startline[1:]) if iscommand else 1

        params = []
        for i in range(nparams):
            if lines[i][0] != '$':
                raise RuntimeError('Invalid data')

            nbytes = int(lines[i][1:])
            param = lines[i+1][:nbytes]

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

            # Parser redis packet
            command, *args = redis_parser.parse(request)

            if command.upper() == 'COMMAND':
                response = b'*1\r\n$4\r\nping\r\n'

            elif command.upper() == 'PING':
                response = b'+PONG\r\n'

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
