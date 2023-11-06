# import neomodel
from neo4j import GraphDatabase
# from neomodel import db, clear_neo4j_database
from neo4j_uploader.logger import ModuleLogger
from urllib.parse import urlparse, unquote

def execute_query(creds: (str, str, str), query, params={}):
    host, user, password = creds
    ModuleLogger().debug(f'Using host: {host}, user: {user} to execute query: {query}')
    # Returns a tuple of records, summary, keys
    with GraphDatabase.driver(host, auth=(user, password)) as driver:
        records, summary, keys =  driver.execute_query(query, params)
        # Only interested in list of result records
        return records

def reset(creds : (str, str, str)):
    # Clears nodes and relationships - but labels remain and can only be cleared via GUI
    query = """MATCH (n) DETACH DELETE n"""
    result = execute_query(creds, query)
    ModuleLogger().info(f"Reset results: {result}")


# def reset_w_neomodel(creds : (str, str, str)):
#     host, user, password = creds

#     parse = urlparse(host)
#     hostname = parse.hostname

#     # TODO: this scheme fails for neo4j+s://
    
#     # db.set_connection(f'neo4j+s://{user}:{password}@{host}')
#     db.set_connection(f'bolt://{user}:{password}@{hostname}')

#     clear_neo4j_database(db, clear_constraints=True, clear_indexes=True)