from neo4j_uploader.models import GraphData, Nodes, Relationships, TargetNode, Neo4jConfig
from neo4j_uploader.logger import ModuleLogger

def elements(
        batch: str,
        records: list[dict],
        dedupe: bool = True,
        exclude_keys: list[str] = []
    ) -> (str, dict):

    # Remove any duplicates
    if dedupe == True:
        records = [dict(t) for t in {tuple(n.items()) for n in records}]

    # Convert each batch of records
    result_str_list = []
    result_params = {}
    for idx, record in enumerate(records):
    
        # Filter out unwanted keys
        filtered_keys = [key for key in record.keys() if key not in exclude_keys]

        # Sort keys for consistent testing
        sorted_keys = sorted(list(filtered_keys))

        # Skip if empty record
        if len(sorted_keys) == 0:
            continue

        # Suffix to uniquely id params
        suffix = f"{batch}_{idx}"

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

        # Add query to list for compilation later
        result_str_list.append(query)


    # Compile results
    if len(result_str_list) == 0:
        result_str = None
    else:
        result_str = ",".join(result_str_list)
    return (result_str, result_params)

def nodes_query(
        batch: str,
        records: list[dict],
        labels: list[str],
        constraints: list[str] = [],
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

    # Sample output
    # WITH [{`uid`:"abc", `name`:"John Wick"},{`uid`:"bcd", `name`:"Cane"}] AS node_data
    # UNWIND node_data as node
    # MERGE (n:`Person` {`uid`:node.`uid`})
    # SET n += node

    if len(records) == 0:
        return None, {}
    
    elements_str, params = elements(
        batch = batch,
        records = records,
        dedupe= dedupe,
        exclude_keys= exclude_keys
    )

    if dedupe == True:
        merge_create = "MERGE"
    else:
        merge_create = "CREATE"

    query = f"""WITH [{elements_str}] AS node_data
    UNWIND node_data AS node
    {merge_create} (n:`{labels[0]}`)
    SET n += node"""
    
    if len(labels) > 1:
        for label in labels[1:]:
            query += f'\nSET n:`{label}`'

    return query, params

def chunked_nodes_query(
        nodes: Nodes,
        config: Neo4jConfig
    ) -> list[(str, dict)]:
    """Returns a list of Cypher queries for batch uploading nodes.

    Args:
        nodes (Nodes): Nodes model specifying node creation specifications and records
        config (Neo4jConfig): Configuration containing max_batch_size

    Returns:
        list[(str, dict)]: List of queries and params to run for uploading data
    """
    
    # Break up large batches of records
    b = config.max_batch_size
    records = nodes.records
    chunked_records = [records[i * b:(i + 1) * b] for i in range((len(records) + b - 1) // b )]  

    # Process each batch into separate query statements
    result = []
    for idx, records in enumerate(chunked_records):
        query_str, query_params = nodes_query(
                batch = f"n{idx}",
                records = records,
                labels = nodes.labels,
                dedupe = nodes.dedupe,
                constraints= nodes.constraints
            )
        if query_str is not None:
            result.append((query_str, query_params))
    return result


def all_node_queries(
        all_nodes: list[Nodes],
        config: Neo4jConfig) -> list[(str, dict)]:
    result = []
    for nodes in all_nodes:
        result.extend(
            chunked_nodes_query(
                nodes,
                config
            )
        )
    return result