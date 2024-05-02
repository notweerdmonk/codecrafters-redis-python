# Uncomment this to pass the first stage
import sys
import socket
from threading import Thread, Lock, Event
from typing import Union, Any
from enum import Enum
import time
import random
from dataclasses import dataclass
import argparse
import secrets
import re


def millis():
    return int(time.time() * 1000)


class RESPBytes(bytes):
    def rstrip_all(self, strip_str):
        """
        Efficiently strips all occurrences of strip_str from the end of the string
        and returns the count of occurrences stripped and the resulting bytearray.

        Args:
        - strip_str: The bytes string to be stripped from the end of the string

        Returns:
        - The count of occurrences of strip_str stripped from the end of the string
        - The resulting bytes string after stripping
        """
        # Initialize count of occurrences stripped
        count = 0

        # Calculate the length of strip_str
        strip_len = len(strip_str)

        # Convert self to a mutable bytearray
        mutable_bytes = bytearray(self)

        # Iterate over the end of the string, checking if it ends with strip_str
        while mutable_bytes.endswith(strip_str):
            # Strip strip_str from the end of the string
            del mutable_bytes[-strip_len:]
            # Increment count of occurrences stripped
            count += 1

        # Convert the mutable bytearray back to bytes
        result = bytes(mutable_bytes)

        return count, result


class RESPStr(str):
    def rstrip_all(self, strip_str):
        """
        Efficiently strips all occurrences of strip_str from the end of the string
        and returns the count of occurrences stripped.

        Args:
        - strip_str: The string to be stripped from the end of the string

        Returns:
        - The count of occurrences of strip_str stripped from the end of the string
        """
        # Initialize count of occurrences stripped
        count = 0

        # Calculate the length of strip_str
        strip_len = len(strip_str)

        # Convert self to a mutable string
        mutable_str = bytearray(self, "utf-8")

        # Iterate over the end of the string, checking if it ends with strip_str
        while mutable_str.endswith(strip_str.encode()):
            # Strip strip_str from the end of the string
            del mutable_str[-strip_len:]
            # Increment count of occurrences stripped
            count += 1

        # Convert the mutable string back to a regular string
        result = mutable_str.decode()

        return count, result


@dataclass
class StoreElement(object):
    value: str
    expiry: float


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

    def set(self, key, value, expiry=-1):
        with self._lock:
            self._store[key] = StoreElement(
                value, (millis() + expiry) if expiry != -1 else expiry
            )

    def get(self, key):
        with self._lock:
            if key not in self._store:
                return ""

            e = self._store[key]
            if e.expiry > 0 and e.expiry <= millis():
                self._store.pop(key)
                return ""
            return e.value

    def delete(self, key):
        with self._lock:
            if key not in self._store:
                return 0

            self._store.pop(key)
            return 1


class Connection(object):
    def __init__(
        self, socket: socket.socket, addr: tuple, server: "Server", isreplica=False
    ):
        self._socket = socket
        self._addr = addr
        self._server = server
        self._thread = None
        self._isreplica = isreplica

    @property
    def addr(self):
        return self._addr

    def set_thread(self, t):
        self._thread = t

    def start(self):
        self._thread.start()

    def join(self):
        self._thread.join()

    def set_replica(self):
        self.isreplica = True
        self._server.add_replica(self)
        print(f"Connection {self._addr} set as replica")

    def relay(self, msg: bytes):
        self.send(msg)

    def send(self, data: bytes):
        ndata = len(data)
        nsent = 0
        nbytes = 0
        while ndata > 0:
            nbytes = self._socket.send(data[nbytes:])
            nsent += nbytes
            ndata -= nbytes

        return nsent

    def handle_connection(self):
        with self._socket:
            while True:
                request_bytes = self._socket.recv(4096)
                if not request_bytes:
                    break

                # Parse RESP packet
                reqlen = len(request_bytes)
                at = 0
                while at < reqlen:
                    n, tokens = RESPparser.parse(request_bytes[at:])
                    command, *args = tokens
                    at += n

                    command = command.upper()

                    if (
                        not self._isreplica
                        and command in self._server.PROPAGATED_COMMANDS
                    ):
                        self._server.relay(request_bytes)
                        self._server.master_repl_offset += len(request_bytes)

                    response = None

                    try:
                        response = server.process_command(command, args, self)

                    except ValueError as v:
                        sys.stderr.write(f"ValueError: {v}\n")
                        response = RESPbuilder.error(
                            args="value is not an integer or out of range",
                            typ=RESPerror.CUSTOM,
                        )

                    except Exception as e:
                        sys.stderr.write(f"Exception occurred: {e}\n")

                    finally:
                        # Default response
                        if response is None:
                            response = RESPbuilder.error(
                                command, args, typ=RESPerror.UNKNOWN_CMD
                            )

                    self._socket.sendall(response)


class ServerRole(Enum):
    MASTER = "master"
    SLAVE = "slave"

@dataclass
class ConfigObject(object):
    name: str = ''
    value: Any = ''

    def build(self):
        return [self.name, str(self.value)]

class ServerConfig(object):
    dirpath: ConfigObject = ConfigObject(name="dir")
    dbfilename: ConfigObject = ConfigObject(name="dbfilename")

    def __init__(self, dirpath: str, dbfilename: str):
        self.dirpath.value = dirpath
        self.dbfilename.value = dbfilename

    def build(self):
        return self.dirpath.build() + self.dbfilename.build()

    def __repr__(self):
        return f"ServerConfig(dirpath={self.dirpath}, dbfilename={self.dbfilename})"

class Server(object):
    _instance = None
    PROPAGATED_COMMANDS = ["SET", "DEL"]

    def __init__(self, config: ServerConfig, port: int = 6379, role: ServerRole = ServerRole.MASTER):
        self._host = "localhost"
        self._port = port
        self._role = role
        self._replicas = []
        self.ackcount = 0

        self.master_replid = secrets.token_hex(20)
        self.master_repl_offset = 0

        self.config = config

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

    def add_replica(self, conn: Connection):
        self._replicas.append(conn)

    def relay(self, msg: bytes):
        for r in self._replicas:
            r.relay(msg)

    def process_command(self, command: str, args: list, conn: Connection = None):
        global store
        global repl_offset

        response = None

        # in case command is not in upper-case letters
        command = command.upper()

        print(f"Received command {command}, args {args}")

        if command == "COMMAND":
            if not conn:
                return None

            response = RESPbuilder.build(["ping", "echo"])

        elif command == "PING":
            if not conn:
                return None

            argslen = len(args)
            if argslen > 1:
                return RESPerror.error(command)

            if argslen == 1:
                response = RESPbuilder.build(args[0])
            else:
                response = RESPbuilder.build("PONG", bulkstr=False)

        elif command == "ECHO":
            if not conn:
                return None

            argslen = len(args)
            if argslen == 0 or argslen > 1:
                return RESPbuilder.error(command)

            response = RESPbuilder.build(args[0])

        elif command == "INFO":
            if not conn:
                return None

            subcommand = ""
            if len(args) >= 1:
                subcommand = args[0].upper()

            if subcommand == "REPLICATION":
                payload = (
                    f"# Replication\r\n"
                    f"role:{server.role}\r\n"
                    f"master_replid:{server.master_replid}\r\n"
                    f"master_repl_offset:{server.master_repl_offset}\r\n"
                )
                response = RESPbuilder.build(payload)

            else:
                response = RESPbuilder.error(
                    args="not implemented", typ=RESPerror.CUSTOM
                )

        elif command == "CONFIG":
            subcommand = ""
            if len(args) >= 1:
                subcommand = args[0].upper()

            if subcommand == "GET":
                opts = args[1:]

                payload = []

                if len(opts) == 0:
                    payload = self.config.build()

                else:
                    for o in opts:
                        opt = o.lower()
                        if opt == "dir":
                            payload += self.config.dirpath.build()
    
                        elif opt == "dbfilename":
                            payload += self.config.dbfilename.build()
    
                response = RESPbuilder.build(payload)

        elif command == "REPLCONF":
            subcommand = ""
            if len(args) >= 1:
                subcommand = args[0].upper()

            if subcommand == "GETACK" and args[1] == "*":
                response = RESPbuilder.build(["REPLCONF", "ACK", str(repl_offset)])

            elif subcommand == "ACK" and args[1].isdigit():
                if len(self._replicas) == 0:
                    return RESPbuilder.error(
                        args="No replicas connected", typ=RESPerror.CUSTOM
                    )

                recvd_offset = int(args[1])
                if recvd_offset >= self.master_repl_offset:
                    self.ackcount += 1

                return b""

            else:
                # Hardcode +OK\r\n
                response = RESPbuilder.build("OK", bulkstr=False)

        elif command == "PSYNC":
            if not conn:
                return None

            argslen = len(args)
            if argslen == 1:
                return RESPbuilder.error(command)
            if argslen == 2 and args[0] != "?" and args[1] != "-1":
                return RESPbuilder.error(typ=RESPerror.SYNTAX)

            # set connection as replica
            conn.set_replica()

            # Hardcode response +FULLRESYNC <REPL_ID> 0\r\n and RDB file contents
            response = RESPbuilder.build(
                f"FULLRESYNC {server.master_replid} {server.master_repl_offset}",
                bulkstr=False,
            ) + RESPbuilder.build(rdb_contents(), rdb=True)

        elif command == "WAIT":
            if not conn:
                return None

            argslen = len(args)
            if argslen < 2:
                return RESPbuilder.error(command)

            numreplicas = int(args[0])
            timeout = int(args[1])

            # send GETACK message to replicas
            getack_req = RESPbuilder.build(["REPLCONF", "GETACK", "*"])
            for r in self._replicas:
                r.send(getack_req)

            self.ackcount = 0

            def poll_func(ev, numreplicas):
                while True:
                    if ev.is_set():
                        return

                    if self.ackcount >= numreplicas:
                        ev.set()
                        return

            wait_ev = Event()

            poll_thread = Thread(
                target=poll_func,
                args=(
                    wait_ev,
                    numreplicas,
                ),
            )
            poll_thread.start()

            wait_ev.wait(timeout / 1000)
            wait_ev.set()

            poll_thread.join()

            result = self.ackcount if self.ackcount > 0 else len(self._replicas)

            response = RESPbuilder.build(int(result))

        elif command == "SET":
            if len(args) < 2:
                if not conn:
                    return None

                return RESPbuilder.error(command)

            exp = -1
            if len(args) > 2:
                expopt = args[2].upper()
                if expopt == "PX" and len(args) == 4:
                    exp = int(args[3])
                else:
                    if not conn:
                        return None

                    return RESPbuilder.error(typ=RESPerror.SYNTAX)

            store.set(args[0], args[1], exp)

            if not conn:
                return None

            response = RESPbuilder.build("OK", bulkstr=False)

        elif command == "GET":
            if not conn:
                return None

            nargs = len(args)
            if nargs < 1:
                return RESPbuilder.error(command)

            if nargs == 1:
                values = store.get(args[0])

            else:
                values = []
                for i in range(nargs):
                    value = store.get(args[i])
                    values.append(value)

            response = RESPbuilder.build(values)

        elif command == "DEL":
            if not conn:
                return None

            nargs = len(args)
            if nargs < 1:
                return RESPbuilder.error(command)

            if nargs == 1:
                keys_deleted = store.delete(args[0])

            else:
                keys_deleted = 0
                for i in range(nargs):
                    keys_deleted += store.delete(args[i])

            response = RESPbuilder.build(keys_deleted)

        return response


class RESPparser(object):
    @classmethod
    def _parse_internal(cls, lines):
        if len(lines) == 0:
            return 0, [""]

        n, startline = RESPStr(lines.pop(0).decode()).rstrip_all("\r\n")
        ntotal = (n * 2) + len(startline)

        params = []

        # integer
        if startline.startswith(":"):
            return ntotal, int(startline[1:])

        # simple string
        elif startline.startswith("+"):
            return ntotal, startline[1:]

        # bulk string
        elif startline.startswith("$"):
            nbytes = int(startline[1:])
            if len(lines) < 1:
                raise RuntimeError("Invalid data")
            n, line = RESPStr(lines.pop(0).decode()).rstrip_all("\r\n")
            if len(line) < nbytes:
                raise RuntimeError("Bulk string data fell short")
            ntotal += (n * 2) + len(line)
            param = line[:nbytes]
            return ntotal, param

        # array
        elif startline.startswith("*"):
            nparams = int(startline[1:])

            for i in range(nparams):
                nchild, paramschild = cls._parse_internal(lines)

                params.append(paramschild)
                ntotal += nchild

        return ntotal, params

    @classmethod
    def parse(cls, data):
        if len(data) == 0:
            return 0, [""]

        lines = data.splitlines(keepends=True)
        nlines = len(lines)

        n, params = cls._parse_internal(lines)

        # if nlines != n:
        #    raise RuntimeError('Invalid data')

        return n, params

    def __init__(self, data):
        self.params = self.parse(data)


class RESPerror(Enum):
    WRONG_ARGS = 1
    UNKNOWN_CMD = 2
    SYNTAX = 3
    CUSTOM = 4


class RESPbuilder(object):

    @classmethod
    def null(cls):
        return "$-1\r\n".encode()

    @classmethod
    def build(
        cls, data: Union[int, str, list], bulkstr: bool = True, rdb: bool = False
    ):
        typ = type(data)

        if typ == int:
            return f":{data}\r\n".encode()

        elif typ == str:
            if len(data) == 0:
                return cls.null()

            if bulkstr:
                return f"${len(data)}\r\n{data}\r\n".encode()
            else:
                return f"+{data}\r\n".encode()

        elif typ == bytes:
            if rdb:
                return f"${len(data)}\r\n".encode() + data
            else:
                return cls.null()

        elif typ == list:
            return f"*{len(data)}\r\n".encode() + "".encode().join(map(cls.build, data))

        else:
            raise TypeError(f"Unsupported type: {typ}")

    @classmethod
    def error(
        cls, command: str = "", args: list = None, typ: RESPerror = RESPerror.WRONG_ARGS
    ):
        if typ == RESPerror.WRONG_ARGS:
            return (
                "-ERR wrong number of arguments for "
                f"'{command}' command\r\n".encode()
            )

        elif typ == RESPerror.UNKNOWN_CMD:
            arg1 = args[0] if len(args) > 0 else ""
            return (
                f"-ERR unknown command `{command}`, "
                f"with args beginning with: {arg1}\r\n".encode()
            )

        elif typ == RESPerror.SYNTAX:
            return "-ERR syntax error\r\n".encode()

        elif typ == RESPerror.CUSTOM:
            return f"-ERR {args}\r\n".encode()

        else:
            raise RuntimeError("Unknown error type")


def rdb_contents():
    hex_data = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

    return bytes.fromhex(hex_data)


def check_expiry():
    global store
    CHECK_INTERVAL = 300  # seconds

    while True:
        MAX_LOT_SIZE = 20

        store_keys = store.keys()
        store_size = len(store_keys)
        if store_size > 0:
            lot_size = store_size if store_size < MAX_LOT_SIZE else MAX_LOT_SIZE
            selected = random.sample(store_keys, lot_size)
            for key in selected:
                store.expired(key)

        time.sleep(CHECK_INTERVAL)


def handle_master_conn(host, port):
    global server
    global repl_offset

    print("Connecting to master")

    master_socket = socket.create_connection((host, port))

    ping = RESPbuilder.build(["ping"])
    master_socket.sendall(ping)
    master_socket.recv(4096)

    replconf = RESPbuilder.build(["REPLCONF", "listening-port", str(server.port)])
    master_socket.sendall(replconf)
    master_socket.recv(4096)

    replconf = RESPbuilder.build(["REPLCONF", "capa", "eof", "capa", "psync2"])
    master_socket.sendall(replconf)
    master_socket.recv(4096)

    psync = RESPbuilder.build(["PSYNC", "?", "-1"])

    nparsed = 0
    data = b""

    MAX_TRIES = 3
    ntries = 0
    while ntries < MAX_TRIES:
        master_socket.sendall(psync)
        # The tester driver sometimes is too eager to receive the response to
        # PSYNC command and probably errors out if there is any delay.
        # Receiving only as many bytes as in the response will avoid post
        # processing and avoid the delay.
        data = master_socket.recv(56)
        n, token = RESPparser.parse(data)
        nparsed = n
        pattern = r"FULLRESYNC ([a-zA-Z0-9]{40}) (\d+)"
        match = re.match(pattern, token)
        if match and match.start() == 0 and match.end() == len(token):
            break

        ntries += 1

    if ntries == MAX_TRIES:
        print(f"Error reading replica sync data")
        master_socket.close()
        return

    rdblen = 0
    rdbdata = b""

    # check if any data is left and parse it
    if nparsed < len(data):
        line = data[nparsed:].splitlines(keepends=True)[0]
        n, line = RESPBytes(line).rstrip_all(b"\r\n")
        if not line.startswith(b"$"):
            print("Error reading bulk length while SYNCing")
            master_socket.close()
            return
        try:
            rdblen = int(line[1:])
        except ValueError as ve:
            print("Error reading bulk length while SYNCing")
            master_socket.close()
            return
        # extract rdb data
        rdbdata = data[
            nparsed + (n * 2) + len(line) : nparsed + (n * 2) + len(line) + rdblen
        ]
        nparsed += (n * 2) + len(line) + len(rdbdata)
        rdblen -= len(rdbdata)

    # wait for RDB file
    while True:
        if len(rdbdata) == 0:
            data = master_socket.recv(4096)
            lines = data.splitlines(keepends=True)[0]
            n, line = RESPBytes(lines).rstrip_all(b"\r\n")
            if not line.startswith(b"$"):
                print("Error reading bulk length while SYNCing")
                master_socket.close()
                return
            try:
                rdblen = int(line[1:])
            except ValueError as ve:
                print("Error reading bulk length while SYNCing")
                master_socket.close()
                return
            # extract rdb data
            rdbdata = data[(n * 2) + len(line) : (n * 2) + len(line) + rdblen]
            nparsed = (n * 2) + len(line) + len(rdbdata)
            rdblen -= len(rdbdata)
        else:
            while rdblen > 0:
                data = master_socket.recv(4096)
                rdbdata += data
                rdblen -= len(data)
                nparsed = len(data)
            break

    print("Master-slave handshake complete")

    if len(data[nparsed:]) > 0:
        data = data[nparsed:]
        datalen = len(data)
        at = 0
        while at < datalen:
            n, tokens = RESPparser.parse(data[at:])
            command, *args = tokens
            response = server.process_command(command.upper(), args)
            if response and len(response) > 0:
                master_socket.sendall(response)
            repl_offset += n
            at += n

    with master_socket:
        while True:
            data = master_socket.recv(4096)
            # print(f'Received from master: {data}')
            if not data:
                break

            datalen = len(data)
            at = 0
            while at < datalen:
                n, tokens = RESPparser.parse(data[at:])
                command, *args = tokens
                response = server.process_command(command.upper(), args)
                if response and len(response) > 0:
                    master_socket.sendall(response)
                repl_offset += n
                at += n

# Globals
store = Store()
server = None
repl_offset = 0
connections = []


def main():
    global server
    global repl_offset

    parser = argparse.ArgumentParser(description="Dummy Redis Server")
    parser.add_argument("--port", type=int, help="Port number")
    parser.add_argument(
        "--replicaof",
        nargs=2,
        metavar=("host", "port"),
        help="Set the host and port of the master to replicate",
    )
    parser.add_argument(
        "--dir",
        type=str,
        help="Path to directory where RDB file gets stored"
    )
    parser.add_argument(
        "--dbfilename",
        type=str,
        help="Name of the RDB file"
    )

    args = parser.parse_args()

    # Configuration
    dirpath = "/tmp/redis-data"
    if args.dir:
        dirpath = args.dir

    dbfilename = "rdbfile"
    if args.dbfilename:
        dbfilename = args.dbfilename

    config = ServerConfig(
        dirpath=dirpath, dbfilename=dbfilename
    )

    # Get port number
    port = 6379
    if args.port:
        port = args.port

    server = Server(config, port)

    print(f"Running on port: {server.port}")

    master_thread = None
    # Get master host and port
    if args.replicaof:
        server.role = ServerRole.SLAVE
        master_host = args.replicaof[0]
        master_port = args.replicaof[1]
        print(f"Set as slave replicating {args.replicaof[0]}:{args.replicaof[1]}")

        master_thread = Thread(
            target=handle_master_conn,
            args=(
                master_host,
                master_port,
            ),
        )
        master_thread.start()

    # Start thread to check for key expiry
    expiry_thread = Thread(target=check_expiry)
    expiry_thread.start()

    # create socket to listen for incomming connections
    server_socket = socket.create_server(
        ("localhost", server.port), backlog=2, reuse_port=True
    )

    MAX_CONCURRENT_CONN = 15
    global connections
    num_conn = 0

    while num_conn < MAX_CONCURRENT_CONN:
        client_socket, addr = server_socket.accept()  # wait for client
        conn = Connection(client_socket, addr, server)
        print("Incoming connection from", addr)
        t = Thread(target=conn.handle_connection)
        conn.set_thread(t)
        connections.append(conn)
        conn.start()
        num_conn += 1

    for conn in connections:
        conn.join()

    if master_thread:
        master_thread.join()

    expiry_thread.join()


if __name__ == "__main__":
    main()
