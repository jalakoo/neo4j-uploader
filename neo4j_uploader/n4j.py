import asyncio
from neo4j import AsyncGraphDatabase
from neo4j_uploader.logger import ModuleLogger

async def execute_query(creds: (str, str, str), query, params={}, database: str = "neo4j"):
    host, user, password = creds
    ModuleLogger().debug(f'Using host: {host}, user: {user} to execute query: {query}')
    # Returns a tuple of records, summary, keys
    with AsyncGraphDatabase.driver(host, auth=(user, password)) as driver:
        return driver.execute_query(query, params, database=database)

async def reset(creds : (str, str, str)):
    # Clears nodes and relationships - but labels remain and can only be cleared via GUI
    query = """MATCH (n) DETACH DELETE n"""
    result = execute_query(creds, query)
    ModuleLogger().info(f"Reset results: {result}")