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
    nodes: list[dict],
    key: str = "_uid"
    ):
    
    query = ""
    count = 0 # Using count to distinguish node variables

    for node_record in nodes:
        count += 1
        
        # Add a newline if this is not the first node
        if count != 1:
            query += "\n"

        # Cypher does not support labels with whitespaces
        query += f"""MERGE (`{label}{count}`:`{label}` {{`{key}`:"{node_record[key]}"}})\nSET `{label}{count}` += {{"""

        # for a_key, value in node_record.items():
        #  Sort keys so we have a consistent output
        sorted_keys = sorted(list(node_record.keys()))

        for idx, a_key in enumerate(sorted_keys):
            value = node_record[a_key]

            # Do not set properties with a None/Null/Empty value
            if value is None:
                continue
            if isinstance(value, str):
                if value.lower() == "none":
                    continue
                if value.lower() == "null":
                    continue
                if value.lower() == "empty":
                    continue

            if idx!= 0:
                query += ", "
            if isinstance(value, str):
                query += f'`{a_key}`:"{value}"'
            else:
                query += f'`{a_key}`:{value}'

        # Close out query
        query += f"}}"

    return query

def upload_nodes(
    neo4j_creds:(str, str, str),
    nodes: dict,
    key: str = '_uid'

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
    ModuleLogger().debug(f'Uploading node records: {nodes}')
    for node_label, nodes_list in nodes.items():
        # Process all similar labeled nodes together
        try: 
            query += upload_node_records_query(node_label, nodes_list, key)
            expected_count += len(nodes_list)
        except Exception as e:
            ModuleLogger().error(f'Problem converting node records labeled {node_label} to Nodes query: {e}')

    ModuleLogger().debug(f'upload nodes query: {query}')

    try:
        execute_query(neo4j_creds, query)
    except Exception as e:
        ModuleLogger().error(f'Problem uploading Nodes: {e}')


def upload_relationship_records_query(
    type: str,
    relationships: list[dict],
    nodes_key: str = "_uid"
    ) -> (str, str):
    """
    Creates a Cypher query for uploading a batch of relationships.

    Args:
        type: The relationship type of the relationship.

        relationships: A list of dictionaries of property data for a list relationship records.
    
        nodes_key: The property key that uniquely identifies Nodes.

    Raises:
        A tuple of strings. The first string are all the MATCH statements, the second string are all the CREATE statements that much come after the matches.
    """
    match_query = ""
    create_query = ""
    count = 0
    for rel in relationships:
        count += 1

        # TODO: Change this to use any custom Nodes key
        from_node = rel.get("_from__uid", None)
        to_node = rel.get("_to__uid", None)

        if from_node is None:
            ModuleLogger().warning(f'Relationship missing _from__uid property. Skipping relationship {rel}')
            continue
        if to_node is None:
            ModuleLogger().warning(f'Relationship missing _to__uid property. Skipping relationship {rel}')
            continue

        
        # TODO: Use subqueries to avoid missing node errors if from or to nodes can not be found: https://aura.support.neo4j.com/hc/en-us/articles/6636607056147-Can-we-set-cypher-lenient-create-relationship-true-in-Aura-


        # Relationship creation is different from node creations
        # All the MATCH statements must be done prior to CREATE statements
        if count != 1:
            match_query += "\n"
            create_query += "\n"

        match_query += f"""MATCH (`fn{type}{count}` {{`{nodes_key}`:'{from_node}'}}),(`tn{type}{count}` {{`{nodes_key}`:'{to_node}'}})"""
        create_query +=f"""CREATE (`fn{type}{count}`)-[`r{type}{count}`:`{type}`"""

        # Filter out from and to node keyword identifiers
        filtered_keys = [key for key in rel.keys() if key not in ["_from__uid", "_to__uid"]]
        sorted_keys = sorted(list(filtered_keys))

        # Add all the properties to the relationship
        if len(sorted_keys) > 0:
            create_query += " {"
            for idx, key in enumerate(sorted_keys):
                value = rel[key]

                # Do not set properties with a None/Null/Empty value
                if value is None:
                    continue
                if isinstance(value, str):
                    if value.lower() == "none":
                        continue
                    if value.lower() == "null":
                        continue
                    if value.lower() == "empty":
                        continue

                if idx!= 0:
                    create_query += ", "
                if isinstance(value, str):
                    create_query += f'`{key}`:"{value}"'
                else:
                    create_query += f'`{key}`:{value}'

            # Close out relationship props
            create_query += "}"

        # Close out relationship and target node
        create_query += f"]->(`tn{type}{count}`)"

    return match_query, create_query

def upload_relationships(
    neo4j_creds:(str, str, str),
    relationships: dict,
    nodes_key: str = "_uid"
):
    """
    Uploads a list of dictionary objects as relationships.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance

        nodes: A dictionary of objects to upload. Each key is a unique relationship type and contains a list of records as dictionary objects.

        nodes_key: The property key that uniquely identifies Nodes.
    
    Raises:
        Exceptions if data is not in the correct format or if the upload ungracefully fails.
    """
    match_queries = """"""
    create_queries = """"""
    ModuleLogger().debug(f'upload relationships source data: {relationships}')
    for rel_type, rel_list in relationships.items():
        # Process all similar labeled nodes together
        matches, creates = upload_relationship_records_query(rel_type, rel_list, nodes_key)
        ModuleLogger().debug(f'Processed relationships for type: {rel_type}, from list: {rel_list}: \nmatches: {matches}, \ncreates: {creates}')
        match_queries += matches
        create_queries += creates

    final_query = match_queries + create_queries
    ModuleLogger().debug(f'upload relationships final query: {final_query}')
    try:
        execute_query(neo4j_creds, final_query)
    except Exception as e:
        ModuleLogger().error(f'Problem uploading Relationships: {e}')

def upload(
        neo4j_creds:(str, str, str), 
        data: str | dict,
        node_key : str = "_uid",
        should_overwrite: bool = False
        ) -> bool:
    """
    Uploads a dictionary of records to a target Neo4j instance.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance. The host name should contain only the database name and not the protocol. For example, if the host name is 'neo4j+s://<unique_db_id>.databases.neo4j.io', the host string to use is '<unique_db_id>.databases.neo4j.io'.

        data: A .json string or dictionary of records to upload. The dictionary keys must contain a 'nodes' and 'relationships' key. The value of which should be a list of dictionaries, each of these dictionaries contain the property keys and values for the nodes and relationships to be uploaded, respectively.

        should_overwrite: A boolean indicating whether the upload should overwrite existing data. If set to True, the upload will delete all existing nodes and relationships before uploading. Default is False.

        node_key: The key in the dictionary that contains the unique identifier for the node. Relationship generation will also use this to find the from and to Nodes it connects to. Default is '_uid'.
    
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
        
    # Upload nodes data first
    nodes = data.get('nodes', None)
    if nodes is None:
        raise Exception('No nodes data found in input data')
    
    if should_overwrite is True:
        reset(neo4j_creds)

    upload_nodes(neo4j_creds, nodes, node_key)

    # Upload relationship data next
    rels = data.get('relationships', None)
    if rels is not None and len(rels) > 0:
        ModuleLogger().info(f'Begin processing relationships: {rels}')
        upload_relationships(neo4j_creds, rels, node_key)

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
    