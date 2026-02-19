# SQL Alchemy tutorial 2
# Metadata objects (MetaData, Table and Column)

import sqlalchemy
import sqlalchemy.orm

# Engine is created once for the entire process
engine = sqlalchemy.create_engine("sqlite+pysqlite:///:memory:", echo=True)

# MetaData object is a collection, which stores Table objects.
# Table definitions may be given explicitly or imported from existing database (a.k.a. reflection).

from sqlalchemy import MetaData, Table, Column, Integer, String

# MetaData object is usually created once and used for the entire data schema of the application.
metadata_obj = MetaData()

# A Table object is linked to MetaData by pointing to the MetaData object.
# It then becomes easy and logical to correctly process linked tables with INSERT and DELETE statements
# and to DDL them (CREATE and DROP).
user_table = Table(
    "user_account",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String(30)),
    Column("fullname", String)
)
print( '\n# SQL Alchemy Table and Column - user')
print( user_table.c.fullname )
print( user_table.c.keys() )
print( user_table.primary_key )


# Let's now define another table, Address, which will show addresses of every user account (one-2-many)
address_table = Table(
    "address",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("user_id", sqlalchemy.ForeignKey('user_account.id'), nullable=False),
    Column("email_address", String, nullable=False)
)
print( '\n# SQL Alchemy Table and Column - address')
print( address_table.c.email_address )
print( address_table.c.keys() )
print( address_table.primary_key )

# We are now ready to send MetaData to the engine to create the tables.
print( '\n# SQL Alchemy Table and Column - create tables using MetaData object and engine')
metadata_obj.create_all(engine)
