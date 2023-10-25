from neo4j_uploader.n4j import execute_query, reset
from neo4j_uploader.logger import ModuleLogger
import json

def start_logging():
    """
    Enables logging from the graph-data-generator module. Log level matches the existing log level of the calling module.
    """
    logger = ModuleLogger()
    logger.is_enabled = True
    logger.info("Graph-Data-Generator logging enabled")

def stop_logging():
    """
    Surpresses logging from the graph-data-generator module.
    """
    ModuleLogger().info(f'Discontinuing logging')
    ModuleLogger().is_enabled = False

def upload_node_records_query(
    label: str,
    nodes: list[dict]
    ):
    
    query = ""
    count = 0
    for node_record in nodes:
        count += 1
        # Cypher does not support labels with whitespaces
        query += f"""
        MERGE (`{label}{count}`:`{label}` {{_uid: "{node_record['_uid']}"}})
        SET `{label}{count}` += {{"""

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
    query = """"""
    expected_count = 0
    for node_label, nodes_list in nodes.items():
        # Process all similar labeled nodes together
        query += upload_node_records_query(node_label, nodes_list)
        expected_count += len(nodes_list)

    ModuleLogger().debug(f'upload nodes query: {query}')

    execute_query(neo4j_creds, query)


def upload_relationship_records_query(
    type: str,
    relationships: list[dict]
    ) -> (str, str):
    """
    Creates a Cypher query for uploading a batch of relationships.

    Args:
        type: The relationship type of the relationship.

        relationships: A list of dictionaries of property data for a list relationship records.
    
    Raises:
        A tuple of strings. The first string are all the MATCH statements, the second string are all the CREATE statements that much come after the matches.
    """
    match_query = ""
    create_query = ""
    count = 0
    for rel in relationships:
        count += 1

        from_node = rel.get("_from__uid", None)
        to_node = rel.get("_to__uid", None)

        if from_node is None:
            ModuleLogger().warning(f'Relationship missing _from__uid property. Skipping relationship {rel}')
            continue
        if to_node is None:
            ModuleLogger().warning(f'Relationship missing _to__uid property. Skipping relationship {rel}')
            continue

        # Relationship creation is different from node creations
        # All the MATCH statements must be done prior to CREATE statements
        match_query += f"""
        MATCH (`fn{type}{count}` {{_uid : '{from_node}'}}), (`tn{type}{count}` {{_uid: '{to_node}'}})"""

        create_query +=f"""
        CREATE (`fn{type}{count}`)-[r{type}{count}:`{type}` {{"""

        for key, value in rel.items():

            # Skip the from and to uuid identifiers
            if key == "_from__uid":
                continue
            if key == "_to__uid":
                continue

            if isinstance(value, str):
                create_query += f"{key}: '{value}',"
            else:
                create_query += f"{key}: {value},"

        # Remove last comma
        create_query = create_query[:-1]

        # Close out relationship and target node
        create_query += f"}}]->(tn{type}{count})"

    return match_query, create_query

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
    match_queries = """"""
    create_queries = """"""
    for rel_type, rel_list in relationships.items():
        # Process all similar labeled nodes together
        matches, creates = upload_relationship_records_query(rel_type, rel_list)
        ModuleLogger().debug(f'Processed relationships for type: {rel_type}, from list: {rel_list}: \nmatches: {matches}, \ncreates: {creates}')
        match_queries += matches
        create_queries += creates

    final_query = match_queries + create_queries
    ModuleLogger().debug(f'upload relationships final query: {final_query}')
    execute_query(neo4j_creds, final_query)

def upload(
        neo4j_creds:(str, str, str), 
        data: str | dict,
        should_overwrite: bool = False
        ) -> bool:
    """
    Uploads a dictionary of records to a target Neo4j instance.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance. The host name should contain only the database name and not the protocol. For example, if the host name is 'neo4j+s://<unique_db_id>.databases.neo4j.io', the host string to use is '<unique_db_id>.databases.neo4j.io'.

        data: A .json string or dictionary of records to upload. The dictionary keys must contain a 'nodes' and 'relationships' key. The value of which should be a list of dictionaries, each of these dictionaries contain the property keys and values for the nodes and relationships to be uploaded, respectively.

        should_overwrite: A boolean indicating whether the upload should overwrite existing data. If set to True, the upload will delete all existing nodes and relationships before uploading. Default is False.
    
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
    
    if should_overwrite is True:
        reset(neo4j_creds)

    upload_nodes(neo4j_creds, nodes)

    # Upload relationship data next
    rels = data.get('relationships', None)
    if rels is not None:
        ModuleLogger().info(f'Begin processing relationships: {rels}')
        upload_relationships(neo4j_creds, rels)

    # Check data has been uploaded successfully
    # TODO: Verify against labels and also check relationships
    expected_nodes = len(nodes)

    query="""
        MATCH (n) 
        RETURN count(n) as count
    """
    result = execute_query(neo4j_creds, query)
    ModuleLogger().info(f"Upload results: {result}")
    result_count = result[0]["count"]
    if result_count < expected_nodes:
        return False
    return True
    