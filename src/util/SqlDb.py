import sqlite3

import sql
from Record import Record

class SqlDb(Record("db_conn")):
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
    
    
