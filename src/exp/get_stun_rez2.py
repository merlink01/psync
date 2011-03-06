# Copyright (c) 2011, Peter Thatcher
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   1. Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#   2. Redistributions in binary form must reproduce the above copyright notice,
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#   3. The name of the author may not be used to endorse or promote products
#      derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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


