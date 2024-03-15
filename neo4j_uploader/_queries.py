from neo4j_uploader.models import GraphData, Nodes, Relationships, TargetNode, Neo4jConfig
from neo4j_uploader._logger import ModuleLogger
from neo4j_uploader._queries_relationships import relationship_elements, relationships_query, new_relationships_from_relationships_with_lists
from neo4j_uploader._queries_utils import does_keypath_contain_list, properties, deduped

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

def chunked_query(
        spec: Nodes | Relationships,
        config: Neo4jConfig
    ) -> list[(str, dict)]:
    """Returns a list of Cypher queries for batch uploading nodes or relationships.

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
            result.append((query_str, query_params))
        if isinstance(spec, Relationships):

            # Shorthand for automatically excluding keys used to specify source and target nodes
            if spec.auto_exclude_keys is True:
                exclude_keys = [spec.from_node.record_key, spec.to_node.record_key]
            else:
                exclude_keys = spec.exclude_keys
            
            # If specs and records include lists as from or to targets, then these need to be broken up into separate queries for processing
            # NOTE: This presumes all records have same schema
            if does_keypath_contain_list(
                spec.to_node.record_key,
                records[0]
            ) == True:
                
                new_relationships = new_relationships_from_relationships_with_lists(
                    spec
                )
                for nr in new_relationships:
                    expanded_records = chunked_query(
                        nr,
                        config
                )
                result.extend(expanded_records)

            else:
                # Process 
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
    """Returns a list of Cypher queries and params for batch uploading nodes or relationships.

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