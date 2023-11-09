from timeit import default_timer as timer
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

# TODO: Export tuple, string query with parameter
def prop_subquery(record: dict, prefix : str, exclude_keys: list[str] = [])-> (str, dict):
    """
    Generates a Cypher substring statement to set properties for a record, excluding given keys

    Args:
        record: Dict of Node or Relatioship properties to convert

        prefix: String prefix to make parameter values unique

        exclude_keys: List of dictionary key values to ignore from substring generation

    
    Returns:
        A tuple containing the substring and a dict of parameters
    """

    # Embed any prop data within brackets { }
    params = {}
    query = " {"

    filtered_keys = [key for key in record.keys() if key not in exclude_keys]
    sorted_keys = sorted(list(filtered_keys))

    for idx, a_key in enumerate(sorted_keys):
        value = record[a_key]

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

        # Prefix multiple items in Cypgher with comma
        if idx!= 0:
            query += ", "

        # Mark value for parameterization and add to params return dict
        param_key = f'{a_key}_{prefix}'
        params[param_key] = value

        # Cypher string query requires { } to designate parameter values
        query += f'`{a_key}`:{{{param_key}}}'

        # if isinstance(value, str):
        #     escaped_value = value.replace('"','\\"')
        #     query += f'`{a_key}`:"{escaped_value}"'
        # else:
        #     query += f'`{a_key}`:{value}'

    # Close out query
    query += "}"

    return query, params

class HashableDict:
    def __init__(self, dictionary):
        self.dictionary = dictionary

    def __hash__(self):
        return hash(frozenset(self.dictionary.items()))
    
def upload_node_records_query(
    label: str,
    nodes: list[dict],
    dedupe : bool = True
    )->(str, dict):
    """
    Generate Cypher Node update query.

    Args:
        label: Label of Nodes to generate

        nodes: A list of dictionaries representing Node properties

        dedupe: Remove duplicate entries
    
    Returns:
        A tuple with the full Cypher query statment and dictionary of parameters to pass to the Neo4j driver

    Raises:
        Exceptions if data is not in the correct format or if the upload fails.
    """
        
    if nodes is None:
        return None
    if len(nodes) == 0:
        return None
    
    query = ""
    result_params = {}

    if dedupe == True:
        nodes = [dict(t) for t in {tuple(n.items()) for n in nodes}]

    for idx, node_record in enumerate(nodes):
        
        # Add a newline if this is not the first node
        if idx == 0:
            query += "\n"

        # Convert contents into a subquery specifying node properties
        subquery, params = prop_subquery(node_record, prefix=f'n{idx}')

        # Cypher does not support labels with whitespaces or accepts them as parameters
        query += f"""MERGE (`{label}{idx}`:`{label}`{subquery})"""
        result_params = result_params | params

    return query, result_params

def upload_nodes(
    neo4j_creds:(str, str, str),
    nodes: dict,
    dedupe : bool = True
)-> (int, int):
    """
    Uploads a list of dictionary objects as nodes.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance

        nodes: A dictionary of objects to upload. Each key is a unique node label and contains a list of records as dictionary objects.

        dedupe: Remove duplicate entries. Default True
    
    Returns:
        A tuple containing the number of nodes created and properties set.

    Raises:
        Exceptions if data is not in the correct format or if the upload fails.
    """
    if nodes is None:
        return None
    if len(nodes) == 0:
        return None
    
    # For reporting
    nodes_created = 0
    props_set = 0

    query = """"""
    params = {}
    expected_count = 0
    ModuleLogger().debug(f'Uploading node records: {nodes}')
    for node_label, nodes_list in nodes.items():
        # Process all similar labeled nodes together
        node_query, node_params = upload_node_records_query(node_label, nodes_list, dedupe=dedupe)
        query += node_query
        params = params | node_params
        expected_count += len(nodes_list)

    ModuleLogger().debug(f'upload nodes query: {query}')

    records, summary, keys = execute_query(neo4j_creds, query, params=params)

    # Sample summary
    # {'metadata': {'query': '<query>', 'parameters': {}, 'query_type': 'w', 'plan': None, 'profile': None, 'notifications': None, 'counters': {'_contains_updates': True, 'labels_added': 17, 'nodes_created': 17, 'properties_set': 78}, 'result_available_after': 73, 'result_consumed_after': 0}
    nodes_created += summary.counters.nodes_created
    props_set += summary.counters.properties_set
    
    ModuleLogger().info(f'Results from upload nodes: \n\tRecords: {records}\n\tSummary: {summary.__dict__}\n\tKeys: {keys}')
    
    return (nodes_created, props_set)


def with_relationship_elements(
    relationships: list[dict],
    prefix: str,
    nodes_key: str = "_uid",
    dedupe: bool = False
    ) -> (str, dict):
    """
    Returns elements to be added into a batch relationship creation query.

    Args:
        relationships: A list of dictionary records for each relationship property.

        prefix: String to uniquely identifiy parameters.

        nodes_key: The property key that uniquely identifies Nodes. 
    
    Returns:
        A tuple of the query substring and parameters dictionary
    Raises:
        Exceptions if data is not in the correct format or if the upload ungracefully fails.
    """
    # TODO: Possible cypher injection entry point?

    result = []
    params = {}

    if dedupe == True:
        relationships = [dict(t) for t in {tuple(r.items()) for r in relationships}]
    
    for rel in relationships:

        # Find from and to node key identifiers
        from_key = f"_from_{nodes_key}"
        to_key = f"_to_{nodes_key}"

        # Get unique key of from and to nodes
        from_node = rel.get(from_key, None)
        if isinstance(from_node, str):
            from_node = f"'{from_node}'"

        to_node = rel.get(to_key, None)
        if isinstance(to_node, str):
            to_node = f"'{to_node}'"

        # Validate we from and to nodes to work with
        if from_node is None:
            ModuleLogger().warning(f'{type} Relationship missing {from_key} property. Skipping relationship {rel}')
            continue
        if to_node is None:
            ModuleLogger().warning(f'{type} Relationship missing {to_key} property. Skipping relationship {rel}')
            continue

        # string and params for props
        props_string, params = prop_subquery(rel, exclude_keys=[from_key, to_key])

        # Add from and to node identifiers as params
        from_node_key = f"from_{prefix}"
        to_node_key = f"to_{prefix}"
        params[from_node_key] = from_node
        params[to_node_key] = to_node

        with_element = f"[{from_node_key},{to_node_key},{props_string}]"
        result.append(with_element)
    
    return result, params


def upload_relationships_list_query(
        prefix: str,
        type: str,
        relationships: list[dict],
        nodes_key: str,
        dedupe: bool = False,
) -> (str, dict):

        ModuleLogger().debug(f'Starting to process relationships type: {type} ...')
        
        # Process all similar labeled nodes together
        with_elements, params = with_relationship_elements(relationships, nodes_key, prefix=prefix, dedupe=dedupe)

        if len(with_elements) is None:
            ModuleLogger().warning(f'Could not process relationships type {type}. Check if data exsists and matches expected schema')
            return (None, None)
        
        with_elements_str = ",".join(with_elements)

        # Assemble final query
        rel_upload_query = f"""WITH [{with_elements_str}] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {{`{nodes_key}`:tuple[0]}})\nMATCH (toNode {{`{nodes_key}`:tuple[1]}})"""

        if dedupe == True:
            rel_upload_query += f"\nMERGE (fromNode)-[r:`{type}`]->(toNode)"
        else:
            rel_upload_query += f"\nCREATE (fromNode)-[r:`{type}`]->(toNode)"
        rel_upload_query +=f"\nSET r = tuple[2]"

        return rel_upload_query, params

def upload_relationships(
    neo4j_creds:(str, str, str),
    relationships: dict,
    nodes_key: str = "_uid",
    dedupe : bool = True
)-> (int, int):
    """
    Uploads a list of dictionary objects as relationships.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance

        nodes: A dictionary of objects to upload. Each key is a unique relationship type and contains a list of records as dictionary objects.

        nodes_key: The property key that uniquely identifies Nodes.

        dedupe: False means a new relationship will always be created for the from and to nodes. True if existing relationships should only be updated. Note that if several relationships already exist, all matching relationships will get their properties updated. Default True.
    
    Returns:
        A tuple of relationships created, properties set

    Raises:
        Exceptions if data is not in the correct format or if the upload ungracefully fails.
    """

    # Final query needs to look something like this
    # WITH [['1202109692044412','1204806322568817', {name:"fish"}],['1202109692044411','test', {}]] AS from_to_id_pairs

    # UNWIND from_to_id_pairs as pair

    # MATCH (fromNode { `gid`: pair[0]})
    # MATCH (toNode { `gid`: pair[1]})

    # CREATE (fromNode)-[r:`WITHIN`]->(toNode)
    # SET r = pair[2]


    # Validate
    if relationships is None:
        return None
    if len(relationships) ==0:
        return None
    

    ModuleLogger().debug(f'upload relationships source data: {relationships}')

    # Upload counts
    relationships_created = 0
    props_set = 0

    # Sort so we get a consistent output
    filtered_keys = [key for key in relationships.keys()]
    sorted_keys = sorted(list(filtered_keys))

    # NOTE: Need to process each relationship type separately as batching mixed relationships fails
    for idx, rel_type in enumerate(sorted_keys):

        rel_list = relationships[rel_type]

        rel_query, rel_params = upload_relationships_list_query(
            prefix=f'r{idx}',
            type=rel_type,
            relationships=rel_list,
            nodes_key=nodes_key,
            dedupe=dedupe)
        # ModuleLogger().debug(f'Starting to process relationships type: {rel_type} ...')
        
        # # Process all similar labeled nodes together
        # with_elements, params = with_relationship_elements(rel_list, nodes_key, prefix=f'r{idx}', dedupe=dedupe)

        # if len(with_elements) is None:
        #     ModuleLogger().warning(f'Could not process relationships type {rel_type}. Check if data exsists and matches expected schema')
        #     continue
        
        # with_elements_str = ",".join(with_elements)

        # # Assemble final query
        # rel_upload_query = f"""WITH [{with_elements_str}] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {{`{nodes_key}`:tuple[0]}})\nMATCH (toNode {{`{nodes_key}`:tuple[1]}})"""

        # if dedupe == True:
        #     rel_upload_query += f"\nMERGE (fromNode)-[r:`{rel_type}`]->(toNode)"
        # else:
        #     rel_upload_query += f"\nCREATE (fromNode)-[r:`{rel_type}`]->(toNode)"
        # rel_upload_query +=f"\nSET r = tuple[2]"

        records, summary, keys = execute_query(neo4j_creds, rel_query,params=rel_params)

        # Sample summary result
        # {'metadata': {'query': "<rel_upload_query>", 'parameters': {}, 'query_type': 'w', 'plan': None, 'profile': None, 'notifications': None, 'counters': {'_contains_updates': True, 'relationships_created': 1, 'properties_set': 2}, 'result_available_after': 209, 'result_consumed_after': 0}

        # Can we have optionals yet?
        relationships_created += summary.counters.relationships_created
        props_set += summary.counters.properties_set

        ModuleLogger().info(f'Results from uploading relationships type: {rel_type}: \n\tRecords: {records}\n\tSummary: {summary.__dict__}\n\tKeys: {keys}')

    return (relationships_created, props_set)


def upload(
        neo4j_creds:(str, str, str), 
        data: str | dict,
        node_key : str = "_uid",
        dedupe_nodes : bool = True,
        dedupe_relationships : bool = True,
        should_overwrite: bool = False
        )-> (float, int, int, int):
    """
    Uploads a dictionary of records to a target Neo4j instance.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance. The host name should contain only the database name and not the protocol. For example, if the host name is 'neo4j+s://<unique_db_id>.databases.neo4j.io', the host string to use is '<unique_db_id>.databases.neo4j.io'.

        data: A .json string or dictionary of records to upload. The dictionary keys must contain a 'nodes' and 'relationships' key. The value of which should be a list of dictionaries, each of these dictionaries contain the property keys and values for the nodes and relationships to be uploaded, respectively.

        node_key: The key in the dictionary that contains the unique identifier for the node. Relationship generation will also use this to find the from and to Nodes it connects to. Default is '_uid'.

        dedupe_nodes: Should nodes only be created once. False means a new node will always be created. True means if an existing node exists, only the properties will be updated. Default True.

        dedupe_relationships: Should relationships only create 1 of a given relationship between the same from and to node. False means a new relationship will always be created. True means if an existing relationship exists between the target nodes, only the properties will be updated. If no prior relationship, a new one will be created. Default True.

        should_overwrite: A boolean indicating whether the upload should overwrite existing data. If set to True, the upload will delete all existing nodes and relationships before uploading. Default is False.
    
    Returns:
        Tuple of result data: float of time to complete, int of nodes created, int of relationships created, int of total node and relationship properties set.
    
    Raises:
        Exceptions if data is not in the correct format or if the upload ungracefully fails.
    """
    # Convert to dictionary if data is string
    if isinstance(data, str) is True:
        try:
            data = json.loads(data)
        except Exception as e:
            raise Exception(f'Input data string not a valid JSON format: {e}')

    # Start clock
    start = timer()

    # Upload nodes data first
    nodes = data.get('nodes', None)
    if nodes is None:
        raise Exception('No nodes data found in input data')
    
    if should_overwrite is True:
        reset(neo4j_creds)

    nodes_created, node_props_set = upload_nodes(neo4j_creds, nodes, dedupe=dedupe_nodes)
    relationships_created = 0,
    relationship_props_set = 0

    # Upload relationship data next
    rels = data.get('relationships', None)
    if rels is not None and len(rels) > 0:
        ModuleLogger().info(f'Begin processing relationships: {rels}')
        relationships_created, relationship_props_set = upload_relationships(neo4j_creds, rels, node_key, dedupe = dedupe_relationships)

    # TODO: Verify uploads successful
    stop = timer()
    time_to_complete = round((stop - start), 4)
    all_props_set = node_props_set + relationship_props_set

    return (time_to_complete, nodes_created, relationships_created, all_props_set)