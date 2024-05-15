import sys
import socket
from threading import Thread, Lock, Event, RLock, Condition
from typing import Union, Any
from enum import Enum
import time
import random
from dataclasses import dataclass
import argparse
import secrets
import re
import concurrent.futures

#import struct
#import fastcrc
#import crc


def millis():
    return int(time.time() * 1000)


class RESPbytes(bytes):
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


class RESPstr(str):
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


class RESPparser(object):
    @classmethod
    def _tokenize_internal(cls, lines):
        if len(lines) == 0:
            return 0, [""]

        n, startline = RESPstr(lines.pop(0).decode()).rstrip_all("\r\n")
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
            n, line = RESPstr(lines.pop(0).decode()).rstrip_all("\r\n")
            if len(line) < nbytes:
                raise RuntimeError("Bulk string data fell short")
            ntotal += (n * 2) + len(line)
            param = line[:nbytes]
            return ntotal, param

        # array
        elif startline.startswith("*"):
            nparams = int(startline[1:])

            for i in range(nparams):
                nchild, paramschild = cls._tokenize_internal(lines)

                params.append(paramschild)
                ntotal += nchild

        return ntotal, params

    @classmethod
    def tokenize(cls, data):
        if len(data) == 0:
            return 0, [""]

        lines = data.splitlines(keepends=True)
        nlines = len(lines)

        n, params = cls._tokenize_internal(lines)

        # if nlines != n:
        #    raise RuntimeError('Invalid data')

        return n, params

    def __init__(self, data):
        self.params = self.tokenize(data)


class RESPerror(Enum):
    WRONG_ARGS = 1
    UNKNOWN_CMD = 2
    WRONGTYPE = 3
    SYNTAX = 4
    CUSTOM = 5


class RESPbuilder(object):

    @classmethod
    def resolve_entry(cls, entry: "Entry"):
        l = []
        keys = list(entry.keys())[1:]
        for key in keys:
            l.append(key)
            l.append(entry[key])
        return [entry["id"], l]

    @classmethod
    def null(cls):
        return "$-1\r\n".encode()

    @classmethod
    def build(
        cls,
        data: Union[int, str, list, "Entry", "Stream"],
        bulkstr: bool = True,
        rdb: bool = False
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
                return f"${len(data)}\r\n".encode() + data + "\r\n".encode()

        elif typ == list:
            return f"*{len(data)}\r\n".encode() + "".encode().join(map(cls.build, data))

        elif typ == Entry:
            streamentryl = RESPbuilder.resolve_entry(data)

            return RESPbuilder.build(streamentryl)

        elif typ == Stream:
            map(RESPbuilder.resolve_entry, data)

            return RESPbuilder.build(data)

        else:
            raise TypeError(f"Unsupported type: {typ}")

    @classmethod
    def error(
        cls,
        command: str = "",
        args: list = None,
        typ: RESPerror = RESPerror.WRONG_ARGS
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

        elif typ == RESPerror.WRONGTYPE:
            return (
                "-WRONGTYPE Operation against a key holding the wrong kind of "
                "value\r\n".encode()
            )

        elif typ == RESPerror.SYNTAX:
            return "-ERR syntax error\r\n".encode()

        elif typ == RESPerror.CUSTOM:
            return f"-ERR {args}\r\n".encode()

        else:
            raise RuntimeError("Unknown error type")


class RDBparser(object):
    _states = [
        "start",
        "magic",
        "ver",
        "aux",
        "dbsel",
        "resizedb",
        "key_val_s",
        "key_val_ms",
        "key_val",
        "eof",
        "chksum",
    ]

    _datatypes = ["str", "int8", "int16", "int32", "lzf"]

    def reset(self):
        self._state = RDBparser._states[0]

    def __init__(self, rdbchecksum: bool = True):
        self.reset()
        self._rdbchecksum = rdbchecksum

    def decode_length_encoding(self, data, pos):
        datatype = "str"

        # Read the first byte
        first_byte = data[pos]

        # Extract the two most significant bits
        msb = first_byte >> 6

        if msb == 0:  # 00: 6-bit encoding
            length = first_byte & 0x3F
            return datatype, length, 1

        elif msb == 1:  # 01: 14-bit encoding
            second_byte = data[pos + 1]
            length = ((first_byte & 0x3F) << 8) | second_byte
            return datatype, length, 2

        elif msb == 2:  # 10: 32-bit encoding
            length = int.from_bytes(
                data[pos + 1 : pos + 5], byteorder="little", signed=False
            )
            return datatype, length, 5

        else:  # 11: special encoding
            fmt = first_byte & 0x3F
            if fmt == 0x3:
                datatype = "lzf"
                raise ValueError("Compressed strings not supported")

            if fmt == 0:
                datatype = "int8"
                return datatype, 1, 1

            elif fmt == 1:
                datatype = "int16"
                return datatype, 2, 1

            elif fmt == 2:
                datatype = "int32"
                return datatype, 4, 1
            else:
                raise ValueError("Invalid data")

    def decode_string_encoding(self, data, pos):
        datatype, str_len, consumed_bytes = self.decode_length_encoding(data, pos)
        pos += consumed_bytes

        string = ""
        if datatype == "str":
            #string = data[pos : pos + str_len].decode("utf-8")
            string = data[pos : pos + str_len]
        elif datatype[:3] == "int":
            string = int.from_bytes(
                data[pos : pos + str_len], byteorder="little", signed=True
            )

        return string, consumed_bytes + str_len

    def parse(self, stream: bytes):
        streamlen = len(stream)
        pos = 0

        # Verify magic string
        if self._state == "start":
            if b"REDIS" != stream[pos:5]:
                raise ValueError("Invalid magic string")

            pos += 5
            self._state = "magic"

        # Get RDB version
        version = 0
        if self._state == "magic":
            # Will raise ValueError on invalid data
            version = int.from_bytes(
                stream[pos:9], byteorder="little", signed=False
            )

            pos += 4
            self._state = "ver"
            yield self._state, "version", version, -1

        # Parse rest of the stream
        while pos < streamlen:
            opcode = stream[pos]
            pos += 1

            if opcode == 0xFD:  # Expiry time in seconds
                self._state = "key_val_s"

                expiry = int.from_bytes(
                    stream[pos : pos + 4], byteorder="little", signed=False
                )
                pos += 4

            elif opcode == 0xFC:  # Expiry time in milliseconds
                self._state = "key_val_ms"

                expiry = int.from_bytes(
                    stream[pos : pos + 8], byteorder="little", signed=False
                )
                pos += 8

            elif opcode == 0xFA:  # Auxiliary fields
                self._state = "aux"

                aux_key, consumed_bytes = self.decode_string_encoding(stream, pos)
                pos += consumed_bytes

                aux_value, consumed_bytes = self.decode_string_encoding(stream, pos)
                pos += consumed_bytes

                yield self._state, aux_key, aux_value, -1

            elif opcode == 0xFE:  # Database selector
                self._state = "dbsel"

                datatype, dbnum_len, consumed_bytes = self.decode_length_encoding(
                    stream, pos
                )
                pos += consumed_bytes

                if datatype == "str":
                    #dbnum = stream[pos : pos + dbnum_len].decode("utf-8")
                    dbnum = stream[pos : pos + dbnum_len]
                elif datatype[:3] == "int":
                    dbnum = int.from_bytes(
                        stream[pos : pos + dbnum_len],
                        byteorder="little",
                        signed=False
                    )

                yield self._state, "dbsel", dbnum, -1

            elif opcode == 0xFB:  # Resize database
                self._state = "resizedb"

                datatype, int_len, consumed_bytes = self.decode_length_encoding(
                    stream, pos
                )
                pos += consumed_bytes
                db_hash_tbl_size = int.from_bytes(
                    stream[pos : pos + int_len],
                    byteorder="little",
                    signed=False,
                )

                datatype, int_len, consumed_bytes = self.decode_length_encoding(
                    stream, pos
                )
                pos += consumed_bytes
                exp_hash_tbl_size = int.from_bytes(
                    stream[pos : pos + int_len],
                    byteorder="little",
                    signed=False
                )

                yield (
                    self._state,
                    "resizedb",
                    (db_hash_tbl_size, exp_hash_tbl_size),
                    -1
                )

            elif opcode == 0xFF:  # End of file marker
                # CRC64 checksum disabled
                if not self._rdbchecksum:
                    break

                # Verify CRC64 checksum
                #expected_crc = struct.unpack('>Q', stream[pos:pos+8])[0]
                #calculator = crc.Calculator(crc.Crc64.CRC64)
                #actual_crc = calculator.checksum(stream[:pos])
                #if actual_crc != expected_crc:
                #    print("CRC64 checksum verification failed")
                #    #raise ValueError("CRC64 checksum verification failed")

                break  # End of file, stop parsing

            else:
                if self._state[:3] != "key":
                    self._state = "key_val"

                pos -= 1

                value_type = stream[pos]
                pos += 1

                key, consumed_bytes = self.decode_string_encoding(stream, pos)
                pos += consumed_bytes
                if type(key) == bytes:
                    key = key.decode("utf-8")

                # Parse value based on type
                if value_type == 0:  # String encoded as a Redis string
                    value, consumed_bytes = self.decode_string_encoding(stream, pos)
                    pos += consumed_bytes
                elif value_type == 1:  # List
                    pass
                elif value_type == 2:  # Set
                    pass

                if "expiry" in locals():
                    yield self._state, key, value, expiry
                    del expiry
                else:
                    yield self._state, key, value, -1


class Entry(dict):
    def __init__(self, *args, **kwargs):
        if "id" not in kwargs:
            kwargs["id"] = f"{str(millis())}-0"
        super().__init__(*args, **kwargs)

    @staticmethod
    def parse_id(id: str):
        pattern = r"(\d+)(?:-(\d+|\*))?|(\*)"
        match = re.match(pattern, id)
        if match:
            groups = match.groups()
            if groups[0] is not None and groups[1] is not None:
                return int(groups[0]), -1 if groups[1] == "*" else int(groups[1])
            if groups[0] is not None:
                return int(groups[0]), -1

        return -1, -1

    def __lt__(self, other):
        if "id" in self and "id" in other:
            time, seq = Entry.parse_id(self["id"])
            other_time, other_seq = Entry.parse_id(other["id"])

            if time > other_time:
                return False
            elif time == other_time:
                if seq < other_seq:
                    return True
                else:
                    return False
            return True
        else:
            raise ValueError("Both objects must have an 'id' key for comparison")

    def __gt__(self, other):
        if "id" in self and "id" in other:
            time, seq = Entry.parse_id(self["id"])
            other_time, other_seq = Entry.parse_id(other["id"])

            if time < other_time:
                return False
            elif time == other_time:
                if seq > other_seq:
                    return True
                else:
                    return False
            return True
        else:
            raise ValueError("Both objects must have an 'id' key for comparison")

    def __eq__(self, other):
        if "id" in self and "id" in other:
            time, seq = Entry.parse_id(self["id"])
            other_time, other_seq = Entry.parse_id(other["id"])

            if time != other_time:
                return False
            if seq != other_seq:
                return False
            return True
        else:
            raise ValueError("Both objects must have an 'id' key for comparison")


class Stream(list):
    def __init__(self, *args):
        for arg in args:
            if isinstance(arg, Entry):
                self.append(arg)
                return
            elif isinstance(arg, list):
                for obj in arg:
                    if not isinstance(obj, Entry):
                        raise TypeError("Stream can only store Entry objects")
                super(Stream, self).__init__(arg)
                return
        raise ValueError(
            "Invalid input. Must be a Entry or a list of Entry objects."
        )

    def append(self, obj):
        if not isinstance(obj, Entry):
            raise TypeError("Stream can only store Entry objects")
        super(Stream, self).append(obj)

    def search(self, id: str, end=True):
        lo = 0
        hi = len(self) - 1
        closest = None

        if id == "-":
            return lo
        if id == "+":
            return hi

        time, seq = Entry.parse_id(id)
        if time == -1:
            return None

        if seq == -1:
            seq = Entry.parse_id(self[-1]["id"])[1] if end else 0
            id = f"{str(time)}-{str(seq)}"

        entry = Entry(id=id)

        while lo <= hi:
            mid = (lo + hi) // 2

            if entry < self[mid]:
                hi = mid - 1
            elif entry > self[mid]:
                closest = mid
                lo = mid + 1
            else:
                return mid

        return closest if end else lo


class StreamError(ValueError):
    def __init__(self, message):
        super(StreamError, self).__init__(message)


@dataclass
class StoreElement(object):
    value: Union[str, Stream]
    expiry: float


class Store(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Store, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self._lock: RLock = RLock()
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
            return list(self._store.keys())

    def type(self, key: str):
        with self._lock:
            if key not in self._store:
                return "none"

            e = self._store.get(key)
            if not e:
                return "none"

            if isinstance(e.value, Stream):
                return "stream"
            elif len(e.value) > 0:
                return "string"

            return "none"

    def append(self, key: str, value: Entry):
        with self._lock:
            try:
                key = int(key)
            except ValueError as v:
                try:
                    key = float(key)
                except ValueError as v:
                    pass

            time, seq = Entry.parse_id(value["id"])
            if time == 0 and seq == 0:
                raise StreamError("The ID specified in XADD must be greater than 0-0")

            if time == -1:
                time = millis()

            if key not in self._store:
                if seq == -1:
                    if time == 0:
                        seq = 1
                    else:
                        seq = 0

                    value["id"] = f"{str(time)}-{str(seq)}"

                self.set(key, Stream(value))
                return value["id"]

            if not isinstance(self._store[key].value, Stream):
                return ""

            # Validate entry ID
            top_entry = self._store[key].value[-1]
            top_time, top_seq = Entry.parse_id(top_entry["id"])

            if (
                time < top_time
                or (time == top_time and seq != -1 and seq <= top_seq)
            ):
                raise StreamError(
                    "The ID specified in XADD is equal or smaller than the "
                    "target stream top item"
                )

            if seq == -1:
                if time == top_time:
                    seq = top_seq + 1
                else:
                    seq = 0

                value["id"] = f"{str(time)}-{str(seq)}"

            self._store[key].value.append(value)
            return value["id"]

    def set(self, key: str, value: Union[str, Stream], expiry=-1):
        with self._lock:
            try:
                key = int(key)
            except ValueError as v:
                try:
                    key = float(key)
                except ValueError as v:
                    pass

            self._store[key] = StoreElement(value, expiry)

    def get(self, key):
        with self._lock:
            try:
                key = int(key)
            except ValueError as v:
                try:
                    key = float(key)
                except ValueError as v:
                    pass

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
        self._isreplica = isreplica
        self._thread = None
        self._exit_cond = False

    def shutdown(self):
        if self._exit_cond == True:
            return

        self._exit_cond = True
        self._socket.shutdown(socket.SHUT_RDWR)

    @property
    def addr(self):
        return self._addr

    @property
    def isprelica(self):
        return self._isreplica

    def set_replica(self):
        self.isreplica = True
        self._server.add_replica(self)
        print(f"Connection {self._addr} set as replica")

    def set_thread(self, t):
        if t is None or not isinstance(t, Thread):
            raise RuntimeError(
                "Argument is None or not an instance of threading.Thread"
            )
        self._thread = t

    def start(self):
        if self._thread is None or not isinstance(self._thread, Thread):
            raise RuntimeError(
                "_thread is None or not an instance of threading.Thread"
            )
        self._thread.start()

    def join(self):
        if self._thread is None or not isinstance(self._thread, Thread):
            raise RuntimeError(
                "_thread is None or not an instance of threading.Thread"
            )
        self._thread.join()

    def send(self, data: bytes):
        ndata = len(data)
        nsent = 0
        nbytes = 0
        while ndata > 0:
            nbytes = self._socket.send(data[nbytes:])
            if nbytes == 0:
                raise RuntimeError("socket connection broken")

            nsent += nbytes
            ndata -= nbytes

        return nsent

    def relay(self, msg: bytes):
        self.send(msg)

    def dispatch(self, data: bytes):
        datalen = len(data)
        at = 0
        while at < datalen:
            n, tokens = RESPparser.tokenize(data[at:])
            command, *args = tokens
            at += n

            command = command.upper()

            if (
                not self._isreplica
                and command in self._server.PROPAGATED_COMMANDS
            ):
                self._server.relay(data)

            response = None
            try:
                response = self._server.process_command(command, args, self)

            except StreamError as s:
                response = RESPbuilder.error(
                    args=str(s),
                    typ=RESPerror.CUSTOM,
                )

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

            self.send(response)

    def __call__(self):
        with self._socket:
            while not self._exit_cond:
                request_bytes = self._socket.recv(4096)
                if not request_bytes:
                    self._exit_cond = True
                    break

                # Parse RESP packet
                self.dispatch(request_bytes)

            self._socket.shutdown(socket.SHUT_RDWR)


class Master(Connection):
    _instance = None

    def __new__(
        cls,
        host: str,
        port: int,
        server: "Server"
    ):
        if cls._instance is None:
            cls._instance = super(Master, cls).__new__(cls)
        return cls._instance

    def __init__(self, host: str, port: int, server: "Server"):
        print(f"Connecting to master {host}:{port}")
        master_socket = socket.create_connection((host, port))
        super(Master, self).__init__(master_socket, (host, port), server)

    def parse_bulk_length(self, data: bytes):
        if len(data) == 0:
            return 0, 0

        line = data.splitlines(keepends=True)[0]
        n, line = RESPbytes(line).rstrip_all(b"\r\n")

        if not line.startswith(b"$"):
            print("Error reading bulk length while SYNCing")
            return -1, -1

        try:
            rdblen = int(line[1:])
        except ValueError as ve:
            print("Error reading bulk length while SYNCing")
            return -1, -1

        return n * 2 + len(line), rdblen

    def extract_rdb(self, data: bytes):
        n, rdblen = self.parse_bulk_length(data)
        if n < 0:
            return n, 0, b""

        # extract rdb data
        rdbdata = data[n : n + rdblen]

        rdbdatalen = len(rdbdata)
        n += rdbdatalen
        rdblen -= rdbdatalen

        return n, rdblen, rdbdata

    def dispatch(self, data: bytes):
        datalen = len(data)
        at = 0
        while at < datalen:
            n, tokens = RESPparser.tokenize(data[at:])
            command, *args = tokens
            at += n

            # Expect non None responses in few cases
            try:
                response = self._server.process_command(command, args)
            except StreamError as s:
                pass
            except ValueError as v:
                sys.stderr.write(f"ValueError: {v}\n")
            except Exception as e:
                sys.stderr.write(f"Exception occurred: {e}\n")

            finally:
                if response and len(response) > 0:
                    self.send(response)

            self._server.add_offset(n)

    def __call__(self):
        request = RESPbuilder.build(["ping"])
        self.send(request)
        self._socket.recv(4096)

        request = RESPbuilder.build(
            ["REPLCONF", "listening-port", str(self._server.port)]
        )
        self.send(request)
        self._socket.recv(4096)

        request = RESPbuilder.build(
            #["REPLCONF", "capa", "eof", "capa", "psync2"]
            ["REPLCONF", "capa", "psync2"]
        )
        self.send(request)
        self._socket.recv(4096)

        request = RESPbuilder.build(["PSYNC", "?", "-1"])

        nparsed = 0
        data = b""

        MAX_TRIES = 3
        ntries = 0
        while ntries < MAX_TRIES:
            self.send(request)

            # The tester driver sometimes is too eager to receive the response
            # to PSYNC command and probably errors out if there is any delay.
            # Receiving only as many bytes as in the response will avoid post
            # processing and avoid the delay.
            data = self._socket.recv(56)
            n, token = RESPparser.tokenize(data)
            nparsed = n
            pattern = r"FULLRESYNC ([a-zA-Z0-9]{40}) (\d+)"
            match = re.match(pattern, token)
            if match and match.start() == 0 and match.end() == len(token):
                break

            ntries += 1

        if ntries == MAX_TRIES:
            print("Error reading replica sync data")
            self._socket.close()
            return

        rdblen = 0
        rdbdata = b""

        # check if any data is left and parse it
        if nparsed < len(data):
            data = data[nparsed:]
            n, rdblen, rdbdata = self.extract_rdb(data)
            if n < 0:
                self._socket.close()
                return
            nparsed += n

        # wait for RDB file
        while True:
            if len(rdbdata) == 0:
                data = self._socket.recv(4096)
                n, rdblen, rdbdata = self.extract_rdb(data)
                if n < 0:
                    self._socket.close()
                    return
                nparsed = n

            else:
                while rdblen > 0:
                    data = self._socket.recv(4096)
                    rdbdata += data
                    datalen = len(data)
                    rdblen -= datalen
                    nparsed = datalen
                break

        self._server.read_rdb(rdbdata)

        print("Master-slave handshake complete")

        if len(data[nparsed:]) > 0:
            self.dispatch(data[nparsed:])

        with self._socket:
            while not self._exit_cond:
                request_bytes = self._socket.recv(4096)
                #print(f'Received from master: {request_bytes}')
                if not request_bytes:
                    self._exit_cond = True
                    break

                # Parse RESP packet
                self.dispatch(request_bytes)

            self._socket.shutdown(socket.SHUT_RDWR)


class Command(object):
    name: str = ""
    arity: int = 0
    flags: list = []
    firstkey: int = 0
    lastkey: int = 0
    step: int = 0

    def __init__(
        self,
        name: str,
        arity: int,
        flags: list,
        firstkey: int,
        lastkey: int,
        step: int
    ):
        self.name = name
        self.arity = arity
        self.flags = flags
        self.firstkey = firstkey
        self.lastkey = lastkey
        self.step = step 

    def build(self):
        pass


class ServerRole(Enum):
    MASTER = "master"
    SLAVE = "slave"


@dataclass
class ConfigObject(object):
    name: str = ""
    value: Any = None

    def build(self):
        return [self.name, str(self.value)]


class ServerConfig(object):
    _rdbchecksum: ConfigObject = ConfigObject(name="rdbchecksum", value=True)
    _dirpath: ConfigObject = ConfigObject(name="dir", value="./")
    _dbfilename: ConfigObject = ConfigObject(name="dbfilename", value="dump.rdb")

    def __init__(self, rdbchecksum: bool = True, **kwargs):
        self._rdbchecksum.value = rdbchecksum
        dirpath = kwargs.get("dirpath", None)
        if dirpath is not None:
            self._dirpath.value = dirpath
        dbfilename = kwargs.get("dbfilename", None)
        if dbfilename is not None:
            self._dbfilename.value = dbfilename

    @property
    def rdbchecksum(self):
        return self._rdbchecksum.value

    @rdbchecksum.setter
    def rdbchecksum(self, value):
        self._rdbchecksum.value = value

    def build_rdbchecksum(self):
        return self._rdbchecksum.build()

    @property
    def dirpath(self):
        return self._dirpath.value

    @dirpath.setter
    def dirpath(self, value):
        self._dirpath.value = value

    def build_dirpath(self):
        return self._dirpath.build()

    @property
    def dbfilename(self):
        return self._dbfilename.value

    @dbfilename.setter
    def dbfilename(self, value):
        self._dbfilename.value = value

    def build_dbfilename(self):
        return self._dbfilename.build()

    def build(self):
        return self._dirpath.build() + self._dbfilename.build()

    def __repr__(self):
        return f"ServerConfig(rdbchecksum={self.rdbchecksum}, "\
               f"dirpath={self.dirpath}, dbfilename={self.dbfilename})"


class Server(object):
    _instance = None
    PROPAGATED_COMMANDS = ["SET", "DEL", "XADD"]

    @classmethod
    def default_config(cls):
        return ServerConfig(
            rdbchecksum=True, dirpath="./", dbfilename="dump.rdb"
        )

    def __init__(
        self,
        store: Store,
        host: str = "localhost",
        port: int = 6379,
        config: ServerConfig = ServerConfig(),
        role: ServerRole = ServerRole.MASTER,
    ):
        if store is None:
            raise ValueError("Argument store cannot be None")
        if not isinstance(store, Store):
            raise TypeError("Argument store is not of type Store")

        if config is None:
            raise ValueError("Argument config cannot be None")
        if not isinstance(config, ServerConfig):
            raise TypeError("Argument config is not of type ServerConfig")

        self._store = store
        
        self._glock = Lock()

        self._config = config
        self._host = host
        self._port = port
        self._role = role

        self._xadd_ev = Event()
        self._xadd_streams = set()

        self._expiry_exit_cond = False
        self._expiry_cv = None
        self._expiry_thread = None

        self._master_conn = None
        """
        This attribute does not need thread locking as 
        """
        self._repl_offset = 0
        self._replicas = []
        self._ackcount = 0
        self._master_replid = secrets.token_hex(20)
        self._master_repl_offset = 0

        self.read_rdb()

    def __new__(
        cls,
        store: Store,
        host: str = "localhost",
        port: int = 6379,
        config: ServerConfig = ServerConfig(),
        role: ServerRole = ServerRole.MASTER
    ):
        if cls._instance is None:
            cls._instance = super(Server, cls).__new__(cls)
            #cls._instance.__init__(port, role)
        return cls._instance

    @property
    def config(self):
        return self._config

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def role(self):
        return self._role.value

    def start(self, backlog=0):
        # Start the expiry checker thread
        self._expiry_exit_cond = False
        self._expiry_cv = Condition()
        self._expiry_thread = Thread(target=self.check_expiry)
        self._expiry_thread.start()

        return socket.create_server(
            (self._host, self._port),
            backlog=backlog,
            reuse_port=True
        )

    def shutdown(self):
        if self._expiry_exit_cond:
            return

        self._expiry_exit_cond = True
        with self._expiry_cv:
            self._expiry_cv.notify()
        self._expiry_thread.join()

    def add_replica(self, conn: Connection):
        self._replicas.append(conn)

    def add_offset(self, n):
        with self._glock:
            self._repl_offset += n
        return self._repl_offset

    def relay(self, msg: bytes):
        if len(msg) == 0:
            return

        with self._glock:
            self._master_repl_offset += len(msg)

        for r in self._replicas:
            r.relay(msg)

    def connect_master(self, host, port):
        print(f"Set as slave replicating {host}:{port}")
        self._role = ServerRole.SLAVE

        self._master_conn = Master(host, port, self)
        if not self._master_conn:
            raise RuntimeError(
                f"Failed to connect to {master_host:master_port}"
            )

        self._master_conn.set_thread(Thread(target=self._master_conn))
        self._master_conn.start()

    def disconnect_master(self):
        if not self._master_conn:
            return

        self._master_conn.shutdown()
        self._master_conn.join()

    def get_entries(self, key_id_list: list, skip: bool = True):
        """
        Fetch a list of streams whose keys are provided. Each stream contains
        entries starting from the entry following the id provided.

        Parameters:
        key_id_list (list of typle): List of (key, id) tuple(s)
        skip (bool): Skip the stream when id is '$'

        Returns:
        list of Stream: List of streams
        """

        streams = []
        for key, id in key_id_list:
            stream = self._store.get(key)
            if not stream:
                continue
            if not isinstance(stream, Stream):
                return [], RESPbuilder.error(typ=RESPerror.WRONGTYPE)

            if skip == True and id == "$":
                continue

            idx = stream.search(id, end=False)
            if idx is None:
                return [], RESPbuilder.error(
                    args="Invalid stream ID specified as stream command "
                    "argument",
                    typ=RESPerror.CUSTOM,
                )

            streamlen = len(stream)
            if idx < streamlen and id >= stream[idx]["id"]:
                idx += 1
            if idx < streamlen:
                streams.append([key, stream[idx:]])

        return streams, None

    def process_command(
        self,
        command: str,
        args: list,
        conn: Connection = None
    ):
        response = None

        # in case command is not in upper-case letters
        command = command.upper()

        # in case there are quote enclosed arguments
        new_args = []
        for arg in args:
            for s in arg.split():
                new_args.append(s)
        args = new_args

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
                with self._glock:
                    master_repl_offset = self._master_repl_offset
                payload = (
                    f"# Replication\r\n"
                    f"role:{self.role}\r\n"
                    f"master_replid:{self._master_replid}\r\n"
                    f"master_repl_offset:{master_repl_offset}\r\n"
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
                        if opt == "rdbchecksum":
                            payload += self.config.build_rdbchecksum()

                        elif opt == "dir":
                            payload += self.config.build_dirpath()

                        elif opt == "dbfilename":
                            payload += self.config.build_dbfilename()

                response = RESPbuilder.build(payload)

        elif command == "REPLCONF":
            subcommand = ""
            if len(args) >= 1:
                subcommand = args[0].upper()

            if subcommand == "GETACK" and args[1] == "*":
                with self._glock:
                    repl_offset = self._repl_offset
                response = RESPbuilder.build(["REPLCONF", "ACK", str(repl_offset)])

            elif subcommand == "ACK" and args[1].isdigit():
                if len(self._replicas) == 0:
                    return RESPbuilder.error(
                        args="No replicas connected", typ=RESPerror.CUSTOM
                    )

                recvd_offset = int(args[1])
                with self._glock:
                    master_repl_offset = self._master_repl_offset
                if recvd_offset >= master_repl_offset:
                    with self._glock:
                        self._ackcount += 1

                return b""

            else:
                response = RESPbuilder.build("OK", bulkstr=False)

        elif command == "PSYNC":
            if not conn:
                return None

            argslen = len(args)
            if argslen == 1:
                return RESPbuilder.error(command)
            if argslen == 2 and args[0] != "?" and args[1] != "-1":
                return RESPbuilder.error(typ=RESPerror.SYNTAX)

            # Set connection as replica
            conn.set_replica()

            rdbdata = b""
            try:
                rdbpath = self.config.dirpath + "/" + self.config.dbfilename
                with open(rdbpath, "rb") as f:
                    rdbdata = f.read()

            except FileNotFoundError as e:
                print(f"RDB file {rdbpath} not found")
                rdbdata = rdb_contents()

            with self._glock:
                master_repl_offset = self._master_repl_offset
            response = RESPbuilder.build(
                f"FULLRESYNC {self._master_replid} {master_repl_offset}",
                bulkstr=False,
            ) + RESPbuilder.build(rdbdata, rdb=True)

        elif command == "WAIT":
            if not conn:
                return None

            argslen = len(args)
            if argslen < 2:
                return RESPbuilder.error(command)

            numreplicas = int(args[0])
            timeout = int(args[1])

            # Send GETACK message to replicas
            getack_req = RESPbuilder.build(["REPLCONF", "GETACK", "*"])
            for r in self._replicas:
                r.send(getack_req)

            with self._glock:
                self._ackcount = 0

            def poll_func(ev, numreplicas):
                while True:
                    if ev.is_set():
                        return

                    with self._glock:
                        if self._ackcount >= numreplicas:
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

            result = self._ackcount if self._ackcount > 0 else len(self._replicas)

            response = RESPbuilder.build(int(result))

        elif command == "KEYS":
            subcommand = ""
            if len(args) >= 1:
                subcommand = args[0]

            if subcommand == "*":
                keys = list(self._store.keys())
                response = RESPbuilder.build(keys)

        elif command == "TYPE":
            key = ""
            if len(args) >= 1:
                key = args[0]

            value_type = self._store.type(key)

            response = RESPbuilder.build(value_type, bulkstr=False)

        elif command == "XADD":
            if len(args) < 4:
                if not conn:
                    return None

                return RESPbuilder.error(command)

            key = args[0]
            entry_id = args[1]

            args = args[2:]
            argslen = len(args)

            entry = Entry(id=entry_id)

            for i in range(0, argslen, 2):
                k = args[i]
                if i + 1 < argslen:
                    v = args[i + 1]
                else:
                    return RESPbuilder.error(typ=RESPerror.SYNTAX)

                entry[k] = v

            stored_id = self._store.append(key, entry)

            # Set XADD event
            if key in self._xadd_streams:
                self._xadd_streams.remove(key)
                self._xadd_ev.set()

            if not conn:
                return None

            response = RESPbuilder.build(stored_id)

        elif command == "XRANGE":
            if len(args) < 3:
                if not conn:
                    return None

                return RESPbuilder.error(command)

            key = args[0]
            start_id = args[1]
            end_id = args[2]

            stream = self._store.get(key)
            if not stream:
                return RESPbuilder.null()
            if not isinstance(stream, Stream):
                return RESPbuilder.error(typ=RESPerror.WRONGTYPE)

            start_idx = stream.search(start_id, end=False)
            end_idx = stream.search(end_id)

            response = RESPbuilder.build(stream[start_idx : end_idx + 1])

        elif command == "XREAD":
            if len(args) < 1:
                if not conn:
                    return None

                return RESPbuilder.error(command)

            block = False
            block_time = 0
            if "block" in args:
                block = True
                block_time = int(args[args.index("block") + 1])
            elif "BLOCK" in args:
                block = True
                block_time = int(args[args.index("BLOCK") + 1])

            start_idx = 0
            if "streams" in args:
                start_idx = args.index("streams") + 1

            elif "STREAMS" in args:
                start_idx = args.index("STREAMS") + 1

            else:
                if not conn:
                    return None

                return RESPbuilder.error(command)

            key_id_len = len(args[start_idx:])
            if key_id_len % 2 != 0:
                return RESPbuilder.error(
                    args="Unbalanced XREAD list of streams: for each stream "
                    "key an ID or '$' must be specified",
                    typ=RESPerror.CUSTOM,
                )
            if key_id_len == 0:
                return RESPbuilder.error(command)

            key_id_len = key_id_len // 2

            key_id_list = []
            for i in range(start_idx, start_idx + key_id_len):
                key = args[i]
                id = args[i + key_id_len]
                key_id_list.append((key, id))

            streams, error = self.get_entries(key_id_list)
            if error is not None:
                return error

            if block and len(streams) == 0:
                for i in range(len(key_id_list)):
                    key, id = key_id_list[i]
                    self._xadd_streams.add(key)
                    if id == "$":
                        stream = self._store.get(key)
                        if not stream:
                            continue
                        key_id_list[i] = (key, stream[-1]["id"])

                self._xadd_ev.clear()
                if block_time > 0:
                    self._xadd_ev.wait(block_time / 1000)
                else:
                    self._xadd_ev.wait()

                self._xadd_streams.clear()
                
                streams, error = self.get_entries(key_id_list, skip=False)
                if error is not None:
                    return error

            response = (
                RESPbuilder.build(streams) if len(streams) > 0 else RESPbuilder.null()
            )

        elif command == "SET":
            if len(args) < 2:
                if not conn:
                    return None

                return RESPbuilder.error(command)

            expiry = -1
            if len(args) > 2:
                expopt = args[2].upper()
                if expopt == "PX" and len(args) == 4:
                    expiry = millis() + int(args[3])
                else:
                    if not conn:
                        return None

                    return RESPbuilder.error(typ=RESPerror.SYNTAX)

            self._store.set(args[0], args[1], expiry)

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
                values = self._store.get(args[0])
                if isinstance(values, Stream):
                    return RESPbuilder.error(typ=RESPerror.WRONGTYPE)

            else:
                values = []
                for i in range(nargs):
                    value = self._store.get(args[i])
                    if isinstance(value, Stream):
                        return RESPbuilder.error(typ=RESPerror.WRONGTYPE)
                    values.append(value)

            response = RESPbuilder.build(values)

        elif command == "DEL":
            if not conn:
                return None

            nargs = len(args)
            if nargs < 1:
                return RESPbuilder.error(command)

            if nargs == 1:
                keys_deleted = self._store.delete(args[0])

            else:
                keys_deleted = 0
                for i in range(nargs):
                    keys_deleted += self._store.delete(args[i])

            response = RESPbuilder.build(keys_deleted)

        return response

    def check_expiry(self):
        CHECK_INTERVAL = 300  # seconds
    
        while not self._expiry_exit_cond:
            MAX_LOT_SIZE = 20
    
            store_keys = list(self._store.keys())
            store_size = len(store_keys)
            if store_size > 0:
                lot_size = store_size if store_size < MAX_LOT_SIZE else MAX_LOT_SIZE
                selected = random.sample(store_keys, lot_size)
                for key in selected:
                    self._store.expired(key)
    
            with self._expiry_cv:
                self._expiry_cv.wait(CHECK_INTERVAL)

    def read_rdb(self, rdbdata: bytes = b""):

        if len(rdbdata) == 0:
            try:
                rdbpath = self.config.dirpath + "/" + self.config.dbfilename
                with open(rdbpath, "rb") as f:
                    rdbdata = f.read()

            except FileNotFoundError as e:
                print(f"RDB file {rdbpath} not found")
                rdbdata = rdb_contents()

        parser = RDBparser()
        for state, key, value, expiry in parser.parse(rdbdata):
            if state[:7] == "key_val" and (expiry == -1 or expiry > millis()):
                print(f"Importing {key}:{value}:{expiry}")
                self._store.set(key, value, expiry)


class ThreadPool:
    def __init__(self, max_workers=None):
        self._tasks = []
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def submit(self, task):
        if not callable(task):
            raise ValueError("Argument task should be callable")

        if not hasattr(task, "shutdown") or not callable(getattr(task, "shutdown")):
            raise ValueError("Argument task must have a callable 'shutdown' method")

        self._tasks.append(task)
        return self._executor.submit(task)

    def qsize(self):
        return self._executor._work_queue.qsize()

    def shutdown(self, wait=True):
        for task in self._tasks:
            task.shutdown()
        self._executor.shutdown(wait=wait)


def rdb_contents():
    hex_data =  "524544495330303131fa0972656469732d76657205372e322e30fa0a7265"\
                "6469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d"\
                "656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

    return bytes.fromhex(hex_data)


class parse_replicaof(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        new_values = []
        for value in values:
            for item in value.split():
                new_values.append(item)
        values = new_values
        if len(values) == 1:
            values.append("6379")
        elif len(values) > 2:
            argstr = ""
            for value in values[2:]:
                argstr += str(value) + " "
            raise ValueError(
                f"{sys.argv[0][sys.argv[0].rfind('/') + 1 : ]}: error: "
                f"unrecognized arguments: {argstr}"
            )
        setattr(namespace, self.dest, values)

def main():
    parser = argparse.ArgumentParser(description="Dummy Redis Server")
    parser.add_argument("--port", type=int, help="Port number")
    parser.add_argument(
        "--replicaof",
        nargs="+",
        metavar=("host", "port"),
        action=parse_replicaof,
        help="Set the host and port of the master to replicate",
    )
    parser.add_argument(
        "--dir", type=str, help="Path to directory where RDB file gets stored"
    )
    parser.add_argument("--dbfilename", type=str, help="Name of the RDB file")
    parser.add_argument(
        "--rdbchecksum", action="store_true", help="Enable CRC64 checksum"
    )

    try:
        args = parser.parse_args()
    except ValueError as v:
        sys.stderr.write(f"{v}\n")
        sys.exit(1)

    # Configuration
    config = ServerConfig()

    if args.dir:
        config.dirpath = args.dir 
    if args.dbfilename:
        config.dbfilename = args.dbfilename
    if args.rdbchecksum:
        config.rdbchecksum = args.rdbchecksum

    # Get port number
    port = 6379
    if args.port:
        port = args.port

    store = Store()
    server = Server(store, "localhost", port, config)

    print(f"Running on port: {server.port}")

    # TODO: move to threadpool
    # Get master host and port
    if args.replicaof:
        if not args.replicaof[1].isdigit():
            print("Master server port number should be an integer")
            sys.exit(1)

        master_host = args.replicaof[0]
        master_port = int(args.replicaof[1])
        server.connect_master(master_host, master_port)

    server_socket = server.start(backlog=2)

    MAX_CONCURRENT_CONN = 15
    tpool = ThreadPool(max_workers=MAX_CONCURRENT_CONN)
    try:
        while True:
            if (tpool.qsize() < MAX_CONCURRENT_CONN):
                client_socket, addr = server_socket.accept()
                print("Incoming connection from", addr)
                tpool.submit(Connection(client_socket, addr, server))

    except KeyboardInterrupt as intr:
        print("Caught Ctrl-C... Exiting")

    tpool.shutdown()

    server.disconnect_master()
    server.shutdown()


if __name__ == "__main__":
    main()
