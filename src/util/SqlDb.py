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

import sqlite3

import sql
from Record import Record

class SqlDb(Record("db_conn")):
    """Abstracts the db_conn and cursor behavior a little bit."""
    def drop(self, table_name):
        db_cursor = self.db_conn.cursor()
        try:
            db_cursor.execute(sql.drop_table(table_name))
            self.db_conn.commit()
        except sqlite3.OperationalError:
            pass  # Already dropped?

    def create(self, table_name, field_types):
        db_cursor = self.db_conn.cursor()
        try:
            db_cursor.execute(sql.create_table(table_name, field_types))
            self.db_conn.commit()
        except sqlite3.OperationalError:
            pass  # Already exists?

    def insert(self, table_name, table_fields, tuples):
        db_cursor = self.db_conn.cursor()
        insert_statement = sql.insert(table_name, table_fields)
        for tup in tuples:
            db_cursor.execute(insert_statement, tup)
        self.db_conn.commit()

    def select(self, table_name, fields, into=None):
        db_cursor = self.db_conn.cursor()
        for values in db_cursor.execute(sql.select(table_name, fields)):
            if into is not None:
                values = into(*values)
            yield values
    
    
