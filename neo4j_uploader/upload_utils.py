from neo4j_uploader.n4j import execute_query
from neo4j_uploader.logger import ModuleLogger

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
        query += f'`{a_key}`:${param_key}'

    # Close out query
    query += "}"

    return query, params

class HashableDict:
    def __init__(self, dictionary):
        self.dictionary = dictionary

    def __hash__(self):
        return hash(frozenset(self.dictionary.items()))
    
# def upload_node_records_query(
#     label: str,
#     nodes: list[dict],
#     node_key: str = "_uid",
#     dedupe : bool = True
#     )->(str, dict):
#     """
#     Generate Cypher Node update query.

#     Args:
#         label: Label of Nodes to generate

#         nodes: A list of dictionaries representing Node properties

#         dedupe: Remove duplicate entries. Default True.
    
#     Returns:
#         A tuple with the full Cypher query statment and dictionary of parameters to pass to the Neo4j driver

#     Raises:
#         Exceptions if data is not in the correct format or if the upload fails.
#     """
        
#     if nodes is None:
#         return ("", {})
#     if len(nodes) == 0:
#         return ("", {})
    
#     query = ""
#     result_params = {}

#     if dedupe == True:
#         nodes = [dict(t) for t in {tuple(n.items()) for n in nodes}]

#     # For some reason, input order of nodes may NOT be maintained. 
#     # Force sort by node_key
#     nodes = sorted(nodes, key=lambda x: x[node_key])

#     for idx, node_record in enumerate(nodes):
        
#         # Add a newline if this is not the first node
#         if idx != 0:
#             query += "\n"

#         # Convert contents into a subquery specifying node properties
#         subquery, params = prop_subquery(node_record, suffix=f'n{idx}')

#         if dedupe == True:
#             # Cypher does not support labels with whitespaces or accepts them as parameters
#             query += f"""MERGE (`{label}{idx}`:`{label}`{subquery})"""
#         else:
#             query += f"""CREATE (`{label}{idx}`:`{label}`{subquery})"""
#         result_params = result_params | params

#     return query, result_params

def with_node_elements(
    nodes: list[dict],
    node_key: str,
    dedupe : bool = True
) -> (str, dict):
    """
    Returns elements to be added into a batch node creation query.

    Args:
        label: The node label.

        nodes: A list of dictionary records for each node to process.

        nodes_key: The property key that uniquely identifies Nodes. 

        dedupe: Should duplicate nodes be purged from final results. Default True.
    
    Returns:
        A tuple of the query substring and parameters dictionary
    """
    results_list = []
    results_params = {}

    if dedupe == True:
        nodes = [dict(t) for t in {tuple(n.items()) for n in nodes}]

    nodes = sorted(nodes, key=lambda x: x[node_key])

    for idx, node in enumerate(nodes):

        # For helping id params later
        suffix = f"n{idx}"

        # string and params for props
        props_string, params = prop_subquery(
            record = node,
            suffix=suffix,
            exclude_keys=[])
        
        # Only processing batches of properties for Nodes. Still recommended way to handle batches vs. series of MATCH or CREATE statements
        with_element = f"{props_string}"
        results_list.append(with_element)
        results_params = results_params | params

    return results_list, results_params

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
    
    ModuleLogger().debug(f'Starting to process node labeled: {label} ...')

    query = ""

    with_elements, params = with_node_elements(
        nodes,
        node_key,
        dedupe
    )

    if len(with_elements) is None:
        ModuleLogger().warning(f'Could not process nodes labeled {label}. Check if data exsists and matches expected schema')
        return ("", {})

    with_elements_str = ",".join(with_elements)

    # Assemble final query
    query = f"""WITH [{with_elements_str}] AS node_data\nUNWIND node_data AS node"""
    
    if dedupe == True:
        query += f"\nMERGE (n:`{label}` {{`{node_key}`:node.`{node_key}`}})"
    else:
        query += f"\nCREATE (n:`{label}`)"
    query += "\nSET n += node"

    return query, params

def upload_nodes(
    neo4j_creds:(str, str, str),
    nodes: dict,
    node_key: str = "_uid",
    database : str = "neo4j",
    dedupe : bool = True,
    max_batch_size: int = 500
)-> (int, int):
    """
    Uploads a list of dictionary objects as nodes.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance

        nodes: A dictionary of objects to upload. Each key is a unique node label and contains a list of records as dictionary objects.

        node_key: String property key that uniquely identifies each node. Default '_uid'.

        database: Name of Neo4j database to connect to. Default 'neo4j'.

        dedupe: Remove duplicate entries. Default True

        max_batched_size: Maximum number of nodes to upload in a single batch. Default 500.
    
    Returns:
        A tuple containing the number of nodes created and properties set.

    Raises:
        Exceptions if data is not in the correct format or if the upload fails.
    """
    if nodes is None:
        return 0, 0
    if len(nodes) == 0:
        return 0, 0
    
    # For reporting
    nodes_created = 0
    props_set = 0

    query = """"""

    ModuleLogger().debug(f'Uploading node records: {nodes}')

    # Sort Nodes to guarantee consistent output
    filtered_keys = [key for key in nodes.keys()]
    sorted_keys = sorted(list(filtered_keys))

    # NOTE: Process each Node label separately so as to not timeout connection with db. Somewhere between 100-1000 Nodes.
    for node_label in sorted_keys:

        total_nodes_list = nodes.get(node_label, None)

        # TODO: Make a calculation of node * properties for a better batch calculation

        # Break relationships into batches of 500
        b = max_batch_size
        chunked_nodes_list = [total_nodes_list[i * b:(i + 1) * b] for i in range((len(total_nodes_list) + b - 1) // b )]  

        for nodes_list in chunked_nodes_list:
            if nodes_list is None:
                ModuleLogger().error(f'No values for node label {node_label} found. Skipping.')
                continue

            # Process all similar labeled nodes together
            nodes_query, nodes_params = upload_node_records_query(
                node_label, 
                nodes_list, 
                dedupe=dedupe, 
                node_key=node_key)

            ModuleLogger().debug(f'upload nodes query: {query}')

            records, summary, keys = execute_query(
                neo4j_creds, 
                nodes_query, 
                params=nodes_params, 
                database=database)

            # Sample summary
            # {'metadata': {'query': '<query>', 'parameters': {}, 'query_type': 'w', 'plan': None, 'profile': None, 'notifications': None, 'counters': {'_contains_updates': True, 'labels_added': 17, 'nodes_created': 17, 'properties_set': 78}, 'result_available_after': 73, 'result_consumed_after': 0}
            try:
                created = summary.counters.nodes_created
                nodes_created += created
            except Exception as e:
                ModuleLogger().debug(f'No nodes labeled: {node_label} created')

            try:
                props = summary.counters.properties_set
                props_set += props
            except Exception as e:
                ModuleLogger().debug(f'No node properties for nodes labeled: {node_label} created')
            
            ModuleLogger().info(f'Results from upload nodes: \n\tRecords: {records}\n\tSummary: {summary.__dict__}\n\tKeys: {keys}')
    
    return nodes_created, props_set


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
        to_node_value = rel.get(to_key, None)

        if from_node_value is None:
            ModuleLogger().warning(f'{type} Relationship from node missing target value for key: {from_key}')
            continue
        if to_node_value is None:
            ModuleLogger().warning(f'{type} Relationship to node missing target value for key: {to_key}')
            continue


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

        with_element = f"[${from_node_key},${to_node_key},{props_string}]"
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
            return ("", {})
        
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

def upload_relationships(
    neo4j_creds:(str, str, str),
    relationships: dict,
    nodes_key: str = "_uid",
    dedupe : bool = True,
    database: str = "neo4j",
    max_batch_size: int = 500
)-> (int, int):
    """
    Uploads a list of dictionary objects as relationships.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance

        nodes: A dictionary of objects to upload. Each key is a unique relationship type and contains a list of records as dictionary objects.

        nodes_key: The property key that uniquely identifies Nodes.

        dedupe: False means a new relationship will always be created for the from and to nodes. True if existing relationships should only be updated. Note that if several relationships already exist, all matching relationships will get their properties updated. Default True.

        database: String name of target Neo4j database

        max_batch_size: Integer maximum number of relationships to upload in a single Cypher batch. Default 500.
    
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
        return 0,0
    if len(relationships) == 0:
        return 0,0
    
    ModuleLogger().debug(f'upload relationships source data: {relationships}')

    # Upload counts
    relationships_created = 0
    props_set = 0

    # Sort so we get a consistent output
    filtered_keys = [key for key in relationships.keys()]
    sorted_keys = sorted(list(filtered_keys))

    # NOTE: Need to process each relationship type separately as batching mixed relationships fails
    for rel_type in sorted_keys:

        # TODO: Break up into batches of 500 relationships max. Exact number with props may vary but this appears to be a decent batch size

        total_rel_list = relationships[rel_type]

        # Break relationships into batches of 500
        b = max_batch_size
        chunked_rel_list = [total_rel_list[i * b:(i + 1) * b] for i in range((len(total_rel_list) + b - 1) // b )]  

        for rel_list in chunked_rel_list:

            rel_query, rel_params = upload_relationship_records_query(
                type=rel_type,
                relationships=rel_list,
                nodes_key=nodes_key,
                dedupe=dedupe)

            records, summary, keys = execute_query(
                neo4j_creds, 
                rel_query,params=rel_params, 
                database=database)

            # Sample summary result
            # {'metadata': {'query': "<rel_upload_query>", 'parameters': {}, 'query_type': 'w', 'plan': None, 'profile': None, 'notifications': None, 'counters': {'_contains_updates': True, 'relationships_created': 1, 'properties_set': 2}, 'result_available_after': 209, 'result_consumed_after': 0}

            # Can we have optionals yet?
            try:
                created = summary.counters.relationships_created
                relationships_created += created
            except Exception as e:
                ModuleLogger().debug(f'No relationship type: {rel_type} created')

            try:
                props = summary.counters.properties_set
                props_set += props
            except Exception as _:
                ModuleLogger().debug(f'No node properties for relationship type: {rel_type} created')

            ModuleLogger().info(f'Results from uploading relationships type: {rel_type}: \n\tRecords: {records}\n\tSummary: {summary.__dict__}\n\tKeys: {keys}')

    return relationships_created, props_set
