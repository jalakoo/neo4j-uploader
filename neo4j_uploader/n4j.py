# import neomodel
from neo4j import GraphDatabase
from neo4j_uploader.logger import ModuleLogger
from urllib.parse import urlparse, unquote

def execute_query(creds: (str, str, str), query, params={}):
    host, user, password = creds
    ModuleLogger().debug(f'Using host: {host}, user: {user} to execute query: {query}')
    # Returns a tuple of records, summary, keys
    with GraphDatabase.driver(host, auth=(user, password)) as driver:
        return driver.execute_query(query, params)

def reset(creds : (str, str, str)):
    # Clears nodes and relationships - but labels remain and can only be cleared via GUI
    query = """MATCH (n) DETACH DELETE n"""
    result = execute_query(creds, query)
    ModuleLogger().info(f"Reset results: {result}")