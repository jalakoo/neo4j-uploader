from neo4j_uploader.models import GraphData, Nodes, Relationships, TargetNode, Neo4jConfig
from neo4j_uploader._logger import ModuleLogger
from enum import Enum
from copy import deepcopy
import json

class ElementType(Enum):
    UNKNOWN = 0
    NODE = 1
    RELATIONSHIP = 2

def convert_to_hashable(obj):
    if isinstance(obj, dict):
        return tuple({k: convert_to_hashable(v) for k, v in obj.items()}.items())
    elif isinstance(obj, list):
        return tuple(convert_to_hashable(i) for i in obj)
    else:
        return obj
    
def deduped(data: list[any])-> list[any]:
    unique = [] 
    seen = set()

    for d in data:
        # Deepcopy to avoid modifying original object
        tmp = deepcopy(d)
        # Convert nested dicts and lists to tuples for hashing
        tmp = convert_to_hashable(tmp)
        if isinstance(tmp, tuple):
            t = tmp
        else:
            t = tuple(tmp.items())
        
        if t not in seen:
            unique.append(d)
            seen.add(t)
    
    return unique
    

def properties(
        suffix: str,
        record: dict,
        exclude_keys: list[str] = []
    ) -> (str, dict):

    # Sample string output
    # " {`age`:$age_test_0, `name`:$name_test_0}"

    # Sample dict output
    # {
    #   "age_test_0": 30,
    #   "name_test_0": "John Wick"
    # }

    # Convert each batch of records
    result_params = {}
    
    # Filter out unwanted keys
    filtered_keys = [key for key in record.keys() if key not in exclude_keys]

    # Sort keys for consistent testing
    sorted_keys = sorted(list(filtered_keys))


    query = " {"
    for k_idx, a_key in enumerate(sorted_keys):

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
            if value.lower() == "":
                continue

        # Nested dicts and lists not supported
        if isinstance(value, dict) or isinstance(value, list):
            value = str(value)

        # Prefix multiple items in Cypher with comma
        if k_idx != 0:
            query += ", "

        # Add params for query
        param_key = f'{a_key}_{suffix}'
        result_params[param_key] = value

        # Add string representation of property data
        query += f'`{a_key}`:${param_key}'

    # Close out query
    query += "}"


    return (query, result_params)

# TODO: Update so string portion is formatted as list like relationships
def node_elements(
        batch: str,
        records: list[dict],
        key: str,
        dedupe: bool = True,
        exclude_keys: list[str] = []
    ) -> (str, dict):

    # Sample string output
    # " {`age`:$age_test_0, `name`:$name_test_0}"

    # Sample dict output
    # {
    #   "age_test_0": 30,
    #   "name_test_0": "John Wick"
    # }

    # Remove any duplicates
    # if dedupe == True:
    #     records = [dict(t) for t in {tuple(n.items()) for n in records}]
    if dedupe == True:
        records = deduped(records)

    # Convert each batch of records
    # result_str_list = []
    result_str = ""
    result_params = {}
    for idx, record in enumerate(records):

        # Suffix to uniquely id params
        suffix = f"{batch}{idx}"

        query, param = properties(suffix, record, exclude_keys)
        result_params.update(param)

        key_placeholder = f'{key}_{suffix}'
        key_value = record[key]

        # Nested dicts and lists are not supported
        if isinstance(key_value, dict) or isinstance(key_value, list):
            key_value = str(key_value)

        result_params.update({
            key_placeholder : key_value
        })

        if idx != 0:
            result_str += ", "
        result_str += f"[${key_placeholder}, {query}]"

    # Compile results
    # if len(result_str_list) == 0:
    #     result_str = None
    # else:
    #     result_str = ",".join(result_str_list)
    return (result_str, result_params)

def nodes_query(
        batch: str,
        records: list[dict],
        key: str,
        labels: list[str],
        exclude_keys : list[str] = [],
        dedupe : bool = True
    ) -> (str, dict):
    """Returns a Cypher query for batch uploading node records.

    Args:
        batch (str): String identifier to separate upload batches
        records (list[dict]): List of dictionaries containing Node properties
        labels (list[str]): List of strings designating Node labels
        constraints (list[str], optional): Optional constraints for defining unique Node Property values. Stub for future feature. Defaults to [].
        dedupe (bool, optional): Should duplicates be prevented. True means the Cypher MERGE command will be used. Defaults to True.

    Returns:
        str, dict: Cypher query and params for uploading data.
    """

    # Sample query output
    # WITH [{`uid`:$uid_b0n0, `name`:$name_b0n0},{`uid`:$uid_b0n1, `name`:$name_b1n1}] AS node_data
    # UNWIND node_data as node
    # MERGE (n:`Person` {`uid`:node.`uid`})
    # SET n += node

    # Sample params output
    # {
    #   "uid_test_0":"abc",
    #   "uid_test_1":"cde"
    #   "name_test_0":"John"
    #   "name_test_1":"Cane"
    # }

    if len(records) == 0:
        return None, {}
    
    elements_str, params = node_elements(
        batch = batch,
        records = records,
        key = key,
        dedupe= dedupe,
        exclude_keys= exclude_keys
    )

    if dedupe == True:
        merge_create = "MERGE"
    else:
        merge_create = "CREATE"

    query = f"""WITH [{elements_str}] AS node_data\nUNWIND node_data AS node\n{merge_create} (n:`{labels[0]}` {{ `{key}`:node[0]}} )\nSET n += node[1]"""
    
    if len(labels) > 1:
        for label in labels[1:]:
            query += f'\nSET n:`{label}`'

    return query, params

def relationship_elements(
        batch: str,
        records: list[dict],
        from_node: TargetNode,
        to_node: TargetNode,
        dedupe: bool = True,
        exclude_keys: list[str] = []
    ) -> (str, dict):

   # Sample string output
    # " [$_from__uid_b0r0,$_to__uid_b0r0, {`since`:$since_b0r0}],[...]"

    # Sample dict output
    # {
    #   "_from__uid_b0r0":"123",
    #   "_to__uid_b0r0":"456",
    #   "since_b0r0":2022
    # }

    if len(records) == 0:
        return None, {}
    
    result_str = ""
    result_params = {}

   # Remove any duplicates
    # if dedupe == True:
    #     records = [dict(t) for t in {tuple(n.items()) for n in records}]
    if dedupe == True:
        records = deduped(records)

    for idx, record in enumerate(records):
        
        suffix = f"{batch}{idx}"

        # This is complicated - so we are going to insert the param keys into the string query so that it will be replaced by the value in the params dictionary later to avoid Cypher injection attacks. The same key will be added to the params dict with actual value there. A suffix is necessary to distinguish all the record keys that are aggregated in the in the final params dictionary passed to the Neo4j driver. 
        from_param_key = f"{from_node.record_key}_{suffix}"
        to_param_key = f"{to_node.record_key}_{suffix}"

        props_str, props_params = properties(
            suffix=suffix,
            record=record,
            exclude_keys=exclude_keys
        )

        # Update string
        if idx != 0:
            result_str += ", "
        result_str += f"[${from_param_key}, ${to_param_key},{props_str}]"

        # Update param dict
        result_params.update(props_params)
        result_params.update(
            {
                from_param_key: record[from_node.record_key],
                to_param_key: record[to_node.record_key]
            }
        )

    return result_str, result_params

def relationships_query(
        batch: str,
        records: list[dict],
        from_node: TargetNode,
        to_node: TargetNode,
        type: str,
        exclude_keys : list[str] = [],
        dedupe : bool = True   
    ) -> (str, dict):

    # Sample output
    # WITH [{$from_key_b0r0,$to_key_b0r0, {`a_key`:$a_value}}] AS from_to_data
    # UNWIND from_to_data AS tuple
    # MATCH (fromNode {`$fromKey`:tuple[0]})
    # MATCH (toNode {`$toKey`:tuple[1]})
    # MERGE (fromNode)-[r:`type`]->(toNode)
    # SET r += tuple[2]

    # Sample params output
    # {
    #   "fromKey":"abc",
    #   "uid_test_1":"cde"
    #   "name_test_0":"John"
    #   "name_test_1":"Cane"
    # }

    if len(records) == 0:
        return None, {}
    
    elements_str, params = relationship_elements(
        batch = batch,
        records = records,
        from_node = from_node,
        to_node = to_node,
        dedupe= dedupe,
        exclude_keys= exclude_keys
    )

    # Handle optional Node Label
    from_node_label = from_node.node_label
    to_node_label = to_node.node_label
    if from_node_label == None:
        from_node_label = ""
    else:
        from_node_label = f":`{from_node_label}`"
    if to_node_label == None:
        to_node_label = ""
    else:
        to_node_label = f":`{to_node_label}`"

    # Required Node Key
    from_node_key = f"{from_node.node_key}"
    to_node_key = f"{to_node.node_key}"

    if dedupe == True:
        merge_create = "MERGE"
    else:
        merge_create = "CREATE"

    query = f"""WITH [{elements_str}] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode{from_node_label} {{`{from_node_key}`:tuple[0]}})\nMATCH (toNode{to_node_label} {{`{to_node_key}`:tuple[1]}})\n{merge_create} (fromNode)-[r:`{type}`]->(toNode)\nSET r += tuple[2]"""


    return query, params

def chunked_query(
        spec: Nodes | Relationships,
        config: Neo4jConfig
    ) -> list[(str, dict)]:
    """Returns a list of Cypher queries for batch uploading nodes.

    Args:
        type: 
        records (Any): Nodes or Relationhips model specifying node creation specifications and records
        config (Neo4jConfig): Configuration containing max_batch_size

    Returns:
        list[(str, dict)]: List of queries and params to run for uploading data
    """
    
    # Break up large batches of records
    b = config.max_batch_size
    records = spec.records
    chunked_records = [records[i * b:(i + 1) * b] for i in range((len(records) + b - 1) // b )]  

    # Process each batch into separate query statements
    result = []
    for idx, records in enumerate(chunked_records):
        if isinstance(spec, Nodes):
            query_str, query_params = nodes_query(
                    f"b{idx}n",
                    records,
                    spec.key,
                    spec.labels,
                    spec.exclude_keys,
                    spec.dedupe
                )
        if isinstance(spec, Relationships):

            # Shorthand for automatically excluding keys used to specify source and target nodes
            if spec.auto_exclude_keys is True:
                exclude_keys = [spec.from_node.record_key, spec.to_node.record_key]
            else:
                exclude_keys = spec.exclude_keys
            
            query_str, query_params = relationships_query(
                f"b{idx}r",
                records,
                spec.from_node,
                spec.to_node,
                spec.type,
                exclude_keys,
                spec.dedupe
            )
        if query_str is not None:
            result.append((query_str, query_params))
    return result


def specification_queries(
        specifications: list[Nodes | Relationships],
        config: Neo4jConfig) -> list[(str, dict)]:
    """Returns a list of Cypher queries and params for batch uploading nodes.

    Args:
        specifications (list[Nodes | Relationships]): Nodes and/or Relationships specifications and properties to upload
        config (Neo4jConfig): Configuration containing max_batch_size

    Returns:
        list[(str, dict)]: List of queries and params to run for uploading data
    """

    result = []
    for spec in specifications:
        result.extend(
            chunked_query(
                spec,
                config
            )
        )
    return result