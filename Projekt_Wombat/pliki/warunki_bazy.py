import sqlalchemy
from sqlalchemy import text

def clearing_triggers(engine):
    con=engine.connect() 
    query = text("SELECT TRIGGER_NAME FROM information_schema.triggers")
    q_res = con.execute(query)
    tr_map = q_res.mappings().all()
    triggers = [row['TRIGGER_NAME'] for row in tr_map]

    for tr in triggers:
        q=text(f"DROP TRIGGER IF EXISTS {tr}")
        con.execute(q)
    con.close()

def creating_triggers(engine, name, table, doings):
    con=engine.connect()
    query =f"""
    CREATE TRIGGER {name} BEFORE INSERT ON {table}
    FOR EACH ROW
    BEGIN 
        {doings}
    END;
    """
    query=text(query)
    con.execute(query)
    con.close()