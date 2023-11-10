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

def prop_subquery(
        record: dict, 
        suffix : str = "", 
        exclude_keys: list[str] = []
        )-> (str, dict):
    """
    Generates a Cypher substring statement to set properties for a record, excluding given keys

    Args:
        record: Dict of Node or Relatioship properties to convert

        suffix: String suffix to make parameter values unique

        exclude_keys: List of dictionary key values to ignore from substring generation

    
    Returns:
        A tuple containing the substring and a dict of parameters
    """

    # Validate
    if isinstance(record, dict) == False:
        return ("", {})
    if len(record) == 0:
        return ("", {})
    
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
        param_key = f'{a_key}_{suffix}'
        params[param_key] = value

        # Cypher string query requires { } to designate parameter values
        query += f'`{a_key}`:{{{param_key}}}'

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
    node_key: str = "_uid",
    dedupe : bool = True
    )->(str, dict):
    """
    Generate Cypher Node update query.

    Args:
        label: Label of Nodes to generate

        nodes: A list of dictionaries representing Node properties

        dedupe: Remove duplicate entries. Default True.
    
    Returns:
        A tuple with the full Cypher query statment and dictionary of parameters to pass to the Neo4j driver

    Raises:
        Exceptions if data is not in the correct format or if the upload fails.
    """
        
    if nodes is None:
        return ("", {})
    if len(nodes) == 0:
        return ("", {})
    
    query = ""
    result_params = {}

    if dedupe == True:
        nodes = [dict(t) for t in {tuple(n.items()) for n in nodes}]

    # For some reason, input order of nodes may NOT be maintained. 
    # Force sort by node_key
    nodes = sorted(nodes, key=lambda x: x[node_key])

    for idx, node_record in enumerate(nodes):
        
        # Add a newline if this is not the first node
        if idx != 0:
            query += "\n"

        # Convert contents into a subquery specifying node properties
        subquery, params = prop_subquery(node_record, suffix=f'n{idx}')

        if dedupe == True:
            # Cypher does not support labels with whitespaces or accepts them as parameters
            query += f"""MERGE (`{label}{idx}`:`{label}`{subquery})"""
        else:
            query += f"""CREATE (`{label}{idx}`:`{label}`{subquery})"""
        result_params = result_params | params

    return query, result_params

async def upload_nodes(
    neo4j_creds:(str, str, str),
    nodes: dict,
    node_key: str = "_uid",
    database : str = "neo4j",
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
        return (0, 0)
    if len(nodes) == 0:
        return (0, 0)
    
    # For reporting
    nodes_created = 0
    props_set = 0

    query = """"""
    params = {}
    expected_count = 0
    ModuleLogger().debug(f'Uploading node records: {nodes}')
    for node_label, nodes_list in nodes.items():
        # Process all similar labeled nodes together
        node_query, node_params = upload_node_records_query(
            node_label, 
            nodes_list, 
            dedupe=dedupe, 
            node_key=node_key)
        query += node_query
        params = params | node_params
        expected_count += len(nodes_list)

    ModuleLogger().debug(f'upload nodes query: {query}')

    records, summary, keys = execute_query(
        neo4j_creds, 
        query, 
        params=params, 
        database=database)

    # Sample summary
    # {'metadata': {'query': '<query>', 'parameters': {}, 'query_type': 'w', 'plan': None, 'profile': None, 'notifications': None, 'counters': {'_contains_updates': True, 'labels_added': 17, 'nodes_created': 17, 'properties_set': 78}, 'result_available_after': 73, 'result_consumed_after': 0}
    nodes_created += summary.counters.nodes_created
    props_set += summary.counters.properties_set
    
    ModuleLogger().info(f'Results from upload nodes: \n\tRecords: {records}\n\tSummary: {summary.__dict__}\n\tKeys: {keys}')
    
    return (nodes_created, props_set)


def with_relationship_elements(
    type: str,
    relationships: list[dict],
    nodes_key: str = "_uid",
    dedupe: bool = True
    ) -> (str, dict):
    """
    Returns elements to be added into a batch relationship creation query.

    Args:
        type: The relationship type.

        relationships: A list of dictionary records for each relationship property.

        nodes_key: The property key that uniquely identifies Nodes. 

        dedupe: Should duplicate relationships be purged from final results. Default True.
    
    Returns:
        A tuple of the query substring and parameters dictionary
    Raises:
        Exceptions if data is not in the correct format or if the upload ungracefully fails.
    """
    # TODO: Possible cypher injection entry point?

    results_list = []
    results_params = {}

    if dedupe == True:
        relationships = [dict(t) for t in {tuple(r.items()) for r in relationships}]

    # Find from and to node key identifiers - applies to all relationships with existing schema
    from_key = f"_from_{nodes_key}"
    to_key = f"_to_{nodes_key}"

    # Force sort bc list is not being consistent
    relationships = sorted(relationships, key=lambda x: (x[from_key], x[to_key]))
    
    for idx, rel in enumerate(relationships):

        suffix = f"r{idx}"

        # Get unique key-value of from and to nodes
        from_node_value = rel.get(from_key, None)
        if isinstance(from_node_value, str):
            from_node_value = f"'{from_node_value}'"

        to_node_value = rel.get(to_key, None)
        if isinstance(to_node_value, str):
            to_node_value = f"'{to_node_value}'"

        # Validate we from and to nodes to work with
        if from_node_value is None:
            ModuleLogger().warning(f'{type} Relationship missing {from_key} property. Skipping relationship {rel}')
            continue
        if to_node_value is None:
            ModuleLogger().warning(f'{type} Relationship missing {to_key} property. Skipping relationship {rel}')
            continue

        # string and params for props
        props_string, params = prop_subquery(
            rel,
            suffix=suffix,
            exclude_keys=[from_key, to_key])

        # Add from and to node identifiers as params
        from_node_key = f"_from_{nodes_key}_{suffix}"
        to_node_key = f"_to_{nodes_key}_{suffix}"

        params[from_node_key] = from_node_value
        params[to_node_key] = to_node_value

        with_element = f"[{{{from_node_key}}},{{{to_node_key}}},{props_string}]"
        results_list.append(with_element)
        results_params = results_params | params
    
    return results_list, results_params


def upload_relationship_records_query(
        type: str,
        relationships: list[dict],
        nodes_key: str,
        dedupe: bool = True,
) -> (str, dict):

        if relationships is None:
            return ("", {})
        if len(relationships) == 0:
            return ("", {})
        
        ModuleLogger().debug(f'Starting to process relationships type: {type} ...')
        
        # Process all similar labeled nodes together
        with_elements, params = with_relationship_elements(
            type,
            relationships, 
            nodes_key=nodes_key, 
            dedupe=dedupe)

        if len(with_elements) is None:
            ModuleLogger().warning(f'Could not process relationships type {type}. Check if data exsists and matches expected schema')
            return (None, None)
        
        with_elements_str = ",".join(with_elements)

        # Assemble final query
        rel_upload_query = f"""WITH [{with_elements_str}] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {{`{nodes_key}`:tuple[0]}})\nMATCH (toNode {{`{nodes_key}`:tuple[1]}})"""

        # Merge only updates, creates new if not already existent. Create ALWAYS creates a new relationship
        if dedupe == True:
            rel_upload_query += f"\nMERGE (fromNode)-[r:`{type}`]->(toNode)"
        else:
            rel_upload_query += f"\nCREATE (fromNode)-[r:`{type}`]->(toNode)"
        
        # Update Relationship properties if any
        rel_upload_query +=f"\nSET r += tuple[2]"

        return rel_upload_query, params

async def upload_relationships(
    neo4j_creds:(str, str, str),
    relationships: dict,
    nodes_key: str = "_uid",
    dedupe : bool = True,
    database: str = "neo4j"
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

        rel_query, rel_params = upload_relationship_records_query(
            type=rel_type,
            relationships=rel_list,
            nodes_key=nodes_key,
            dedupe=dedupe)

        records, summary, keys = await execute_query(
            neo4j_creds, 
            rel_query,params=rel_params, 
            database=database)

        # Sample summary result
        # {'metadata': {'query': "<rel_upload_query>", 'parameters': {}, 'query_type': 'w', 'plan': None, 'profile': None, 'notifications': None, 'counters': {'_contains_updates': True, 'relationships_created': 1, 'properties_set': 2}, 'result_available_after': 209, 'result_consumed_after': 0}

        # Can we have optionals yet?
        relationships_created += summary.counters.relationships_created
        props_set += summary.counters.properties_set

        ModuleLogger().info(f'Results from uploading relationships type: {rel_type}: \n\tRecords: {records}\n\tSummary: {summary.__dict__}\n\tKeys: {keys}')

    return (relationships_created, props_set)


async def upload(
        neo4j_creds:(str, str, str), 
        data: str | dict,
        node_key : str = "_uid",
        dedupe_nodes : bool = True,
        dedupe_relationships : bool = True,
        should_overwrite: bool = False,
        database_name: str = 'neo4j'
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

    if node_key is None or node_key == "":
        raise Exception(f'node_key cannot be None or an empty string')
    
    # Start clock
    start = timer()

    # Upload nodes data first
    nodes = data.get('nodes', None)
    if nodes is None:
        raise Exception('No nodes data found in input data')
    
    if should_overwrite is True:
        reset(neo4j_creds)

    nodes_created, node_props_set = await upload_nodes(
        neo4j_creds, 
        nodes, 
        node_key= node_key, 
        dedupe=dedupe_nodes, 
        database= database_name)
    relationships_created = 0,
    relationship_props_set = 0

    # Upload relationship data next
    rels = data.get('relationships', None)
    if rels is not None and len(rels) > 0:
        ModuleLogger().info(f'Begin processing relationships: {rels}')
        relationships_created, relationship_props_set = await upload_relationships(
            neo4j_creds, 
            rels, 
            node_key, 
            dedupe = dedupe_relationships, 
            database=database_name)

    # TODO: Verify uploads successful
    stop = timer()
    time_to_complete = round((stop - start), 4)
    all_props_set = node_props_set + relationship_props_set

    return (time_to_complete, nodes_created, relationships_created, all_props_set)