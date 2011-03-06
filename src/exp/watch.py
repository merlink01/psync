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

from thirdparty import pyinotify

# Wow.  watching ~ uses A LOT of CPU!

class Handler(pyinotify.ProcessEvent):
    def process_default(self, event):
        print ("default", event)

    # also process_IN_CREATE and process_IN_DELETE
    def process_IN_MODIFY(self, event):
        print ("IN_MODIFY", event.path, event.name)

    def process_IN_CREATE(self, event):
        print ("IN_CREATE", event.path, event.name)

    def process_IN_DELETE(self, event):
        print ("IN_DELETE", event.path, event.name)

def on_loop(notifier):
    print ("on_loop", notifier)

if __name__ == "__main__":
    import sys
    root = sys.argv[1]

    # pyinotify.log.setLevel(10)
    handler = Handler()
    
    # Exclude patterns from list
    excl_lst = []  # ['(^|*/).bibble']
    excl = pyinotify.ExcludeFilter(excl_lst)

    wm = pyinotify.WatchManager()
    # first arg can be a list
    # can use pyinotify.ALL_EVENTS
    # rec=True means recursive.  Must have!
    wm.add_watch(root, pyinotify.IN_MODIFY | pyinotify.IN_CREATE | pyinotify.IN_DELETE,
                 rec=True, auto_add=True, exclude_filter=excl)
    notifier = pyinotify.Notifier(wm, default_proc_fun=handler)
    notifier.loop(callback = on_loop)
    # if daemonize = True, spawns another process
    # notifier.loop(daemonize=True, callback=on_loop,
    #              pid_file='/tmp/pyinotify.pid', stdout='/tmp/stdout.txt')
