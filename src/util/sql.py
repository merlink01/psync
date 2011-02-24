# Copyright 2006 Uberan - All Rights Reserved

def drop_table(table_name):
    return "drop table {0}".format(table_name)

def create_table(table_name, fields):
    return "create table {0} ({1})".format(
        table_name, ", ".join(fields))

def insert(table_name, fields):
    return "insert into {0} ({1}) values ({2})".format(
        table_name, ", ".join(fields), ", ".join("?" for _ in fields))

def select(table_name, fields):
    return "select {1} from {0}".format(
        table_name, ", ".join(fields))


