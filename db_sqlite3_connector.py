
import sqlite3


db = sqlite3.connect('data/db/db.sqlite3')

table_1 = "CREATE TABLE IF NOT EXISTS db_status(\
        names STRING PRIMARY KEY, level INT, status INT)"
table_2 = "CREATE TABLE IF NOT EXISTS db_counter(\
        name STRING PRIMARY KEY, number INT)"
c = db.cursor()
c.execute(table_1)
c.execute(table_2)


def create_record(name, level, status):
    parameters = (name, level, status)
    query = "INSERT OR IGNORE INTO db_status VALUES (?, ?, ?)"
    c.execute(query, parameters)
    db.commit()


def create_counter(name, number):
    parameters = (name, number)
    query = "INSERT OR IGNORE INTO db_counter VALUES (?, ?)"
    c.execute(query, parameters)
    db.commit()


def check_counter(obj):
    parameters = (obj,)
    query = "SELECT number FROM db_counter WHERE name=(?)"
    data = c.execute(query, parameters)
    return data.fetchall()


def update_counter(number, name):
    parameters = (number, name)
    query = "UPDATE db_counter SET number=(?) WHERE name=(?)"
    c.execute(query, parameters)
    db.commit()


def get_names_level_status_0(level):
    parameters = (str(level))
    query = "SELECT names FROM db_status WHERE level=(?) AND status=0"
    data = c.execute(query, parameters)
    return data.fetchall()


def update_status(name):
    parameters = (name)
    query = "UPDATE db_status SET status=1 WHERE names=(?)"
    c.execute(query, parameters)
    db.commit()
