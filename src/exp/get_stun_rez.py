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

# OSX Notes:
# SO_REUSEADDR works, but not on port 80, so we can't use ipchicken :(.

class HttpConnectionWithPremadeSocket(httplib.HTTPConnection):
    def __init__(self, host, sock):
	httplib.HTTPConnection.__init__(self, host)
        self.sock = sock

    def connect(self):
	if self._tunnel_host:
            self._tunnel()

def read_from_chicken(host, sock):
    conn = HttpConnectionWithPremadeSocket(host, sock)
    conn.request("GET", url)
    stream = conn.getresponse()
    data = stream.read() 
    address_match = re.compile(
        "(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})").search(data)
    port_match = re.compile("Port:\s*(\S+)").search(data)
    public_address = address_match.group(1) if address_match is not None else None
    public_port = port_match.group(1) if port_match is not None else None
    return public_address, public_port

host, port, url = ("localhost", 8080, "/my-public-info")
#host, port, url = ("www.ipchicken.com", 80, "/")
#host, port = ('208.70.149.225', 25259)

import sys
import time

def use_socket(address, local_port = None):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if local_port:
        sock.bind(("", local_port))
    sock.settimeout(1.0)
    sock.connect(address)
    local_port = sock.getsockname()[1]
    sock.close()
    time.sleep(0.1)
    return local_port

local_port1 = use_socket((host, port))
print local_port1
local_port2 = use_socket((host, port), local_port1)
print local_port2

# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# sock.settimeout(1.0)
# #sock.bind(("", local_port))
# try:
#     sock.connect((host, port))
#     print read_from_chicken(host, sock)
# except socket.timeout:
#     pass
# (local_host, local_port) = sock.getsockname()
# sock.close()

# time.sleep(0.5)

# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# sock.settimeout(1.0)
# sock.bind(("", local_port))
# try:
#     sock.connect((host, port))
#     print read_from_chicken(host, sock)
# except socket.timeout:
#     pass
# sock.close()

# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# sock.settimeout(0.5)
# sock.bind(("", local_port))
# sock.listen(1)
# try:
#     print sock.accept()
# except socket.timeout:
#     pass
# sock.close()



