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


# The purpose of this file is to record merge actions.  This is good
# for debugging purposes and "showing the user status" purposes.

from util import Record, sql

TABLE_NAME = "merges"
TABLE_FIELD_TYPES = ["utime integer",
                     "peerid varchar",
                     "action varchar",
                     "groupid varchar",
                     "path varchar",
                     "details varchar",
                     "author_peerid varchar"]
TABLE_FIELDS = [ft.split(" ")[0] for ft in TABLE_FIELD_TYPES]

class MergeLogEntry(Record(*TABLE_FIELDS)):
    pass

class MergeLog(Record("db", "clock")):
    def __new__(cls, db, clock):
        db.create(TABLE_NAME, TABLE_FIELD_TYPES)
        return cls.new(db, clock)

    def read_actions(self):
        return list(self.db.select(TABLE_NAME, TABLE_FIELDS,
                                   into=MergeLogEntry))

    def add_action(self, action):
        utime = self.clock.unix()
        peerid = action.newer.peerid
        action_type = str(action.type)
        (groupid, path) = action.gpath
        details = str(action.details) if action.details else ""
        author_peerid = action.newer.author_peerid
        entry = MergeLogEntry(utime, peerid, action_type,
                              groupid, path, details, author_peerid)

        self.db.insert(TABLE_NAME, TABLE_FIELDS, [entry])
