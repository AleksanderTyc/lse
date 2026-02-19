# SQL Alchemy tutorial 1
# Intro to Alchemy Connection and Session

print( '\n# SQL Alchemy version')
import sqlalchemy
print( sqlalchemy.__version__ )

# Engine is created once for the entire process
engine = sqlalchemy.create_engine("sqlite+pysqlite:///:memory:", echo=True)

# Use SQLite engine with Python plugin, do not create a persistent DB, use in-memory instead.
print( '\n# SQL Alchemy Hello World example')
with engine.connect() as conn:
    result=conn.execute(sqlalchemy.text("select 'hello world'"))
    print(result.all())
"""
2026-02-17 12:51:07,360 INFO sqlalchemy.engine.Engine BEGIN (implicit)
2026-02-17 12:51:07,360 INFO sqlalchemy.engine.Engine select 'hello world'
2026-02-17 12:51:07,361 INFO sqlalchemy.engine.Engine [generated in 0.00052s] ()
[('hello world',)]
2026-02-17 12:51:07,361 INFO sqlalchemy.engine.Engine ROLLBACK
"""
# Notice that the trasaction was rolled-back - no autocommit, end of connection block is the end of transaction.
# print result prints [('hello world',)] line only, the rest results from echo=True


print( '\n# SQL Alchemy create a table, load a record, show the record')
with engine.connect() as conn:
    conn.execute(sqlalchemy.text("create table some_table(x int, y int)"))
    conn.execute(sqlalchemy.text("insert into some_table values(1, 2)"))
    conn.execute(sqlalchemy.text("insert into some_table(x,y) values(:x,:y)"), [{'x':10,'y':11},{'x':20,'y':21}])
    conn.commit()
with engine.connect() as conn:
    result=conn.execute(sqlalchemy.text("select * from some_table"))
    print(result.all())
    result=conn.execute(sqlalchemy.text("select * from some_table where x > 5"))
    print(result.all())
# This is a 'commit as you go' style, requiring explicit commit

# Note how we pass values into parameterised SQL statement.
# We use `:x` as a named parameter and a single instance or a list of dictionary objects,
# which provide(s) a set of of name:value combinations for each named parameter.

# It is possible to receive row resulting from `execute` call for a single set of arguments.
# It is not possible to receive row resulting from `execute` call for a list of sets of arguments,
# except for special case of `insert` which must be called with specialised logic `Insert.returning()`.


print( '\n# SQL Alchemy load more records in ''begin once'' style')
with engine.begin() as conn:
    conn.execute(sqlalchemy.text("insert into some_table(x,y) values(:x,:y)"), [{'x':30,'y':31},{'x':32,'y':33}])
with engine.connect() as conn:
    result=conn.execute(sqlalchemy.text("select * from some_table"))
    for row in result:
        print(f"x: {row.x}, y:{row.y}")
    print(result.all())
    for row in result:
        print(f"x: {row.x}, y:{row.y}")
# No explicit commit is required, as `begin` assumes it is a connection block
# and a committed transaction (unless fails).

# Neither `print` nor the second loop create any output. This is because `result` is an iterator.
# Once it has been exhausted, it does not yield any data.

# It is a good practice to separate DDL statements (such as create table)
# from data manipulation statements (such as insert into) into different transactions.
# There should be a commit after `create table` and before `insert into`.


print( '\n# SQL Alchemy Soft intro to ORM Session pattern')
from sqlalchemy.orm import Session
sqlal_text = sqlalchemy.text("select x, y from some_table where y > :y order by x desc")
with Session(engine) as session:
    result = session.execute(sqlal_text, {'y':6})
    for row in result:
        print(f"x:{row.x}, y:{row.y}")

with Session(engine) as session:
    result = session.execute(sqlalchemy.text('update some_table set y=:y where x=:x'), [{'x':10,'y':19},{'x':30,'y':29}])
    session.commit()
    result = session.execute(sqlalchemy.text('select * from some_table'))
    print(result.all())


# ##########################################
#
# Testing only below this line
#

# import inspect

import datetime
print( datetime.datetime.today() )
with engine.connect() as conn:
    # sql_insert_expr = sqlalchemy.insert(events_table).values(load_timestamp=datetime.datetime.today())
    sql_insert_expr = sqlalchemy.insert(events_table).values()
    sql_insert_expr = sqlalchemy.insert(events_table).values(load_type='B')
    print( sql_insert_expr )
    compiled = sql_insert_expr.compile()
    print( compiled.params )
    result = conn.execute(sqlalchemy.text("insert into load_events(load_timestamp) values(:load_timestamp)"), {'load_timestamp': datetime.datetime.today()})
    result = conn.execute(sql_insert_expr)
    print( result.inserted_primary_key[0] )
    conn.commit()
    # pk = result.inserted_primary_key
    # print( f"INSERT pk: {pk}" )
    # members = inspect.getmembers(result)
    # print( f"members: {members}" )
    # print( f"INSERT pk: {result.inserted_primary_key}" )

import os
print( os.getcwd() ) # get current working directory

import datetime
print( datetime.datetime.today() )

