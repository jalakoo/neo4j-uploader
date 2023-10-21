# import neomodel
from neo4j import GraphDatabase
from neomodel import db, clear_neo4j_database

def execute_query(creds: (str, str, str), query, params={}):
    host, user, password = creds
    host = f'neo4j+s://{host}'
    # Returns a tuple of records, summary, keys
    with GraphDatabase.driver(host, auth=(user, password)) as driver:
        records, summary, keys =  driver.execute_query(query, params)
        # Only interested in list of result records
        return records

def reset_with_cypher(creds : (str, str, str)):
    host, user, password = creds
    # Clears nodes and relationships - but labels remain and can only be cleared via GUI
    query = """
    MATCH (n)
    REMOVE n
    """
    execute_query(host, user, password, query, params = {})

def reset(creds : (str, str, str)):
    host, user, password = creds
    db.set_connection(f'neo4j+s://{user}:{password}@{host}')
    clear_neo4j_database(db, clear_constraints=True, clear_indexes=True)