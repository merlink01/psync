# Copyright (c) 2012, Peter Thatcher
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

# This file makes a handy "RunTimer" which can be used like this:
#
# def on_finished(rt):
#    print "Finished {0.name} = {1.result} in {2.elapsed} seconds".format(rt)
#
# timer = RunTimer(clock, on_finished)
#
# with timer("Read Database") as rt:
#   values = read_database
#   rt.set_result(len(values))
#
# Now, you have values and the time it took to complete got logged.

class RunTimer:
    def __init__(self, clock, logger):
        self.clock = clock
        self.logger = logger

    def __call__(self, name):
        return RunTime(name, self.clock, self.logger)

class RunTime:
    def __init__(self, name, clock, logger):
        self.name   = name
        self.clock  = clock
        self.logger = logger
        self.before = None
        self.after  = None
        self.result = None

    def __enter__(self):
        self.before = self.clock.unix_fine()
        return self

    def set_result(self, result):
        self.result = result

    def __exit__(self, *args):
        self.after = self.clock.unix_fine()
        self.logger(self)

    @property
    def elapsed(self):
        if self.before is None or self.after is None:
            return None
        else:
            return self.after - self.before
        
