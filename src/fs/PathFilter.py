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

# The purpose of this file is to define how to filter out paths we
# don't care about when scanning or comparing files.  We work really
# hard to make sure it is very fast.

import fnmatch
import re

from util import Record

class PathFilter(Record("patterns_to_ignore", "names_to_ignore",
                        "paths_to_ignore")):
    """Controls whether to ingore a path or not, which is mostly used
    for scanning and comparing files. ignore_path will be called a
    lot, and should memoize.  Also, names_to_ignore is used directly
    as a set in FileSystem scans.  For convenience, globs_to_ignore
    are converted to regular expressions to ignore."""
    def __new__(cls, globs_to_ignore, names_to_ignore):
        names_to_ignore = frozenset(names_to_ignore)
        patterns_to_ignore = compile_globs(globs_to_ignore)
        return cls.new(patterns_to_ignore, names_to_ignore, set())

    def ignore_path(self, path):
        if path in self.paths_to_ignore:
            return True
        elif (matches_any_name(path, self.names_to_ignore) or
              matches_any_pattern(path, self.patterns_to_ignore)):
            self.paths_to_ignore.add(path)
            return True
        else:
            return False

def compile_globs(globs):
    return [re.compile(fnmatch.translate(glob), re.IGNORECASE)
            for glob in globs]

def matches_any_name(path, names):
    return frozenset(path.split("/")) & names

def matches_any_pattern(path, patterns):
    return any(pattern.match(path) for pattern in patterns)
