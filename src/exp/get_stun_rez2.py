import httplib
import re
import socket

class HttpConnectionWithReusableAddress(httplib.HTTPConnection):
    def __init__(self, *args, **kargs):
        source_address = kargs.pop("source_address", None)
        httplib.HTTPConnection.__init__(self, *args, **kargs)
        self.source_address = source_address

    def connect(self):
        self.sock = create_connection((self.host,self.port),
                                      self.timeout, self.source_address)
        if self._tunnel_host:
            self._tunnel()

def create_connection(address, timeout=None, source_address=None):
    """Connect to *address* and return the socket object.

    Convenience function.  Connect to *address* (a 2-tuple ``(host,
    port)``) and return the socket object.  Passing the optional
    *timeout* parameter will set the timeout on the socket instance
    before attempting to connect.  If no *timeout* is supplied, the
    global default timeout setting returned by :func:`getdefaulttimeout`
    is used.  If *source_address* is set it must be a tuple of (host, port)
    for the socket to bind as a source address before making the connection.
    An host of '' or port 0 tells the OS to use the default.
    """

    host, port = address
    err = None
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
            sock.settimeout(timeout)
        if source_address:
            sock.bind(source_address)
        sock.connect(address)
        return sock
    except socket.error as err:
        if sock is not None:
            sock.close()
        raise

#host, port, url = ("localhost", 8080, "/my-public-info")
host, port, url = ("www.ipchicken.com", 80, "/")

import sys
local_port = int(sys.argv[1])

def read():
    #sock = create_connection((host, port), source_address=("", local_port))
    conn = HttpConnectionWithReusableAddress(
        host, port, source_address = ("", local_port))
    conn.request("GET", url)
    print conn.sock.getsockname()
    stream = conn.getresponse()
    data = stream.read() 
    address_match = re.compile(
        "(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})").search(data)
    port_match = re.compile("Port:\s*(\S+)").search(data)
    public_address = address_match.group(1) if address_match is not None else None
    public_port = port_match.group(1) if port_match is not None else None
    print public_address, public_port


read()
#import time
#time.sleep(0.5)
#read()


