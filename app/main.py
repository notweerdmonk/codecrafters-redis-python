# Uncomment this to pass the first stage
import socket

class redis_parser(object):
    @classmethod
    def parse(self, data):
        if len(data) == 0:
            return ['']

        lines = data.splitlines()
        startline = lines[0]
        lines.pop(0)

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

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage

    server_socket = socket.create_server(("localhost", 6379), backlog=2, reuse_port=True)

    MAX_CONN = 2
    num_conn = 0

    while num_conn < MAX_CONN:
        client_socket, addr = server_socket.accept() # wait for client

        print('Incoming connection from', addr)

        with client_socket:
            while True:
                request_bytes = client_socket.recv(4096)
                if not request_bytes:
                    break

                request = request_bytes.decode('utf-8')

                # Parser redis packet
                command, *args = redis_parser.parse(request)

                if command.upper() == 'PING':
                    client_socket.sendall(b'+PONG\r\n')

        num_conn += 1

if __name__ == "__main__":
    main()
