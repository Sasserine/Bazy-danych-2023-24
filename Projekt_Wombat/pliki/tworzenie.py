import json
import sqlalchemy
from sqlalchemy import text,MetaData,inspect
from sqlalchemy.schema import DropConstraint,ForeignKeyConstraint
from pliki.warunki_bazy import *

def clearing_base(engine):
    con=engine.connect()
    metadata = MetaData() 
    metadata.reflect(engine)
    clearing_triggers(engine) 
    for table in metadata.sorted_tables:
        for f_keys in table.constraints:
            if type(f_keys)==ForeignKeyConstraint:
                q=DropConstraint(f_keys)
                con.execute(q)
    for table in metadata.sorted_tables:
        table.drop(con)
    con.close()

def creating_base(engine, fi):
    con=engine.connect()
    with open(fi, mode="r", encoding="utf-8") as file:
        data = json.load(file)
    for table_name, column_list in data.items():
        query=(f"CREATE OR REPLACE TABLE {table_name} (")
        for column in column_list:
            for column_name, column_specification in column.items():
                if column_name!="CONSTRAINT":
                    query+=column_name
                    query+=" "
                    for detail in column_specification:
                        query+=detail
                        query+=" "
                    en=len(query)
                    query=query[0:en-1]
                    query+=", "
                else:
                    for key in column_specification:
                        query+=column_name
                        query+=" "
                        query+=key
                        query+=", "
                    en=len(query)
                    query=query[0:en-2]
                    query+=", "
        en=len(query)
        query=query[0:en-2]
        query+=");"
        query=text(query)
        con.execute(query)
    con.close()