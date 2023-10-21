from neo4j_uploader.n4j import execute_query, reset
from logger import ModuleLogger
import json

def upload_node_records_query(
    label: str,
    nodes: list[dict]
    ):
    
    query = ""
    keys = None
    count = 0
    for node_record in nodes:
        count += 1
        query += f"""\nMERGE ({label}{count}:{label} {{_uid: "{node_record['_uid']}"}})\nSET {label}{count} += {{"""

        for key, value in node_record.items():
            if isinstance(value, str):
                query += f"{key}: '{value}',"
            else:
                query += f"{key}: {value},"

        # Remove last comma
        query = query[:-1]
        query += f"}}"

    return query

def upload_nodes(
    neo4j_creds:(str, str, str),
    nodes: dict
):
    """
    Uploads a list of dictionary objects as nodes.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance

        nodes: A dictionary of objects to upload. Each key is a unique node label and contains a list of records as dictionary objects.
    
    Raises:
        Exceptions if data is not in the correct format or if the upload fails.
    """
    # TODO: Generate a batch query to upload all nodes at once
    query = """
    """
    for node_label, nodes_list in nodes.items():
        # Process all similar labeled nodes together
        query += upload_node_records_query(node_label, nodes_list)

    ModuleLogger().debug(f'upload query: {query}')
    execute_query(neo4j_creds, query)


def upload_relationship_records_query(
    label: str,
    relationships: list[dict]
    ):
    
    query = ""
    keys = None
    count = 0
    for rel in relationships:
        count += 1
        # TODO:
        # query += f"""\nMERGE ({label}{count}:{label} {{_uid: "{rel['_uid']}"}})\nSET {label}{count} += {{"""
        query += f""""""

        for key, value in rel.items():
            if isinstance(value, str):
                query += f"{key}: '{value}',"
            else:
                query += f"{key}: {value},"

        # Remove last comma
        query = query[:-1]
        query += f"}}"

    return query

def upload_relationships(
    neo4j_creds:(str, str, str),
    relationships: dict
):
    """
    Uploads a list of dictionary objects as relationships.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance

        nodes: A dictionary of objects to upload. Each key is a unique relationship type and contains a list of records as dictionary objects.
    
    Raises:
        Exceptions if data is not in the correct format or if the upload ungracefully fails.
    """
    # TODO: Generate a batch query to upload all nodes at once
    query = """
    """
    for rel_type, nodes_list in relationships.items():
        # Process all similar labeled nodes together
        query += upload_node_records_query(rel_type, nodes_list)

    print(f'upload query: {query}')
    records = execute_query(neo4j_creds, query)
    print(f'Query results: {records}')

def upload(
        neo4j_creds:(str, str, str), 
        data: str | dict,
        clear_on_upload: bool = True
        ) -> bool:
    """
    Uploads a dictionary of records to a target Neo4j instance.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance. The host name should contain only the database name and not the protocol. For example, if the host name is 'neo4j+s://<unique_db_id>.databases.neo4j.io', the host string to use is '<unique_db_id>.databases.neo4j.io'.

        data: A .json string or dictionary of records to upload. The dictionary keys must contain a 'nodes' and 'relationships' key. The value of which should be a list of dictionaries, each of these dictionaries contain the property keys and values for the nodes and relationships to be uploaded, respectively.
    
    Returns:
        True if the upload was successful, False otherwise
    
    Raises:
        Exceptions if data is not in the correct format or if the upload ungracefully fails.
    """
    # Convert to dictionary if data is string
    if isinstance(data, str) is True:
        try:
            data = json.loads(data)
        except Exception as e:
            raise Exception(f'Input data string not a valid JSON format: {e}')
        
    # TODO: Upload nodes data first
    nodes = data.get('nodes', None)
    if nodes is None:
        raise Exception('No nodes data found in input data')
    
    if clear_on_upload:
        reset(neo4j_creds)

    upload_nodes(neo4j_creds, nodes)

    # TODO: Upload relationship data next