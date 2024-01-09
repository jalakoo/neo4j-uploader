import asyncio
from neo4j import GraphDatabase
from neo4j_uploader._logger import ModuleLogger

def validate_credentials(creds: (str, str, str)):
    host, user, password = creds
    with GraphDatabase.driver(host, auth=(user, password)) as driver:
        driver.verify_connectivity()

def upload_query(creds: (str, str, str), query, params={}, database: str = "neo4j"):
    host, user, password = creds
    with GraphDatabase.driver(host, auth=(user, password)) as driver:
        _, summary, _ = driver.execute_query(query, params, database=database) 
        return summary

def execute_query(creds: (str, str, str), query, params={}, database: str = "neo4j"):
    host, user, password = creds
    ModuleLogger().debug(f'Using host: {host}, user: {user} to execute query: {query}')
    # Returns a tuple of records, summary, keys
    with GraphDatabase.driver(host, auth=(user, password)) as driver:
        return driver.execute_query(query, params, database=database)

def drop_constraints(creds: (str, str, str), database: str = "neo4j"):
    query = 'SHOW CONSTRAINTS'
    result = execute_query(creds, query, database=database)

    ModuleLogger().info(f"Drop constraints results: {result}")

    # Have to make a drop constraint request for each individually!
    for record in result.records:
        constraint_name = record.get("name", None)
        if constraint_name is not None:
            drop_query = f"DROP CONSTRAINT {constraint_name}"
            drop_result = execute_query(creds, drop_query, database=database)

        ModuleLogger().info(f"Drop constraint {constraint_name} results: {drop_result}")

    # This should now show empty
    result = execute_query(creds, query, database=database)

    return result

def reset(creds : (str, str, str), database: str = "neo4j"):

    drop_constraints(creds, database)

    # Clears nodes and relationships - but labels remain and can only be cleared via GUI
    query = """MATCH (n) DETACH DELETE n"""
    records, summary, keys = execute_query(creds, query, database=database)

    ModuleLogger().info(f"Reset results: {summary}")
    return summary

def create_new_node_constraints(
        creds: (str, str, str),
        node_key: str,
        database: str = "neo4j"
):
    query = f"""CREATE CONSTRAINT node_key IF NOT EXISTS FOR (u:`{node_key}`)\nREQUIRE u.`node_key` IS UNIQUE"""
    result = execute_query(creds, query, database=database)

    ModuleLogger().info(f"Create new constraints results: {result}")
    return result
