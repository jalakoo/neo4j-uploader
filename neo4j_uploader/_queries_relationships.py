from neo4j_uploader.models import GraphData, Nodes, Relationships, TargetNode, Neo4jConfig
from neo4j_uploader._dynamic_dict import DynamicDict
from neo4j_uploader._queries_utils import deduped, properties, keypath_list_to_1st_list, keypath_to_1st_list, value_for_keypath

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

    # Optionally remove duplicates
    if dedupe == True:
        records = deduped(records)

    for idx, record in enumerate(records):
        
        # Generate a suffix to uniquely separate placeholder/param data
        suffix = f"{batch}{idx}"

        # Insert the param keys into the string query so that it will be replaced by the value in the params dictionary later to avoid Cypher injection attacks. The same key will be added to the params dict with actual value there. 

        # TODO: Determine if the record key is a single key reference, a nested dictionary reference to a single key, or points to a list of dictionaries.

        # TODO: If the target key-value is a list of dictionaries, then a relationship will be created for each dictionary in the list.
        from_param_key = f"{from_node.record_key}_{suffix}"
        to_param_key = f"{to_node.record_key}_{suffix}"

        # Generate strings and params for the node or relationship properties, this will be the entirety of the original dictionary data passed in to the record arg, minus the key-value pairs specified by the exclude_keys arg. Reuse the same suffix as the main keys above - to avoid collisions with param references for the batch Cypher commands later.
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

        # Record key -> Record value mapping
        # The from_ or to_param_keys may reference nested data via dot notation
        from_to_params = relationship_from_to_dict(
            from_param_key,
            to_param_key,
            from_node.record_key,
            to_node.record_key,
            record)

        result_params.update(from_to_params)

    return result_str, result_params

def relationship_from_to_dict(
        from_param_key: str,
        to_param_key: str,
        from_node_key: str,
        to_node_key: str,
        record: dict
    )-> dict:
    """Generates a dictionary specifying a relationship's from and to nodes from record data. Supports strings specifying dot-notation paths within a record dictionary.

    Args:
        from_param_key (str): The key to use for the from node
        to_param_key (str): The key to use for the to node
        from_node_key (str): String key or string dot-path route to value within the records dictionary to use as the from node key property.
        to_node_key (str): String key or string dot-path route to value within the records dictionary to use as the to node key property.
        record (dict): Dictionary containing both from and to node key property mappings and relationships properties

    Returns:
        dict: Dictionary containing only the to and from key-values
    """
    from_node_value = record.get(from_node_key, None)
    dynamic_record = DynamicDict(record)
    if from_node_value is None:
        # Key does not exist in record, attempt to search via dot-path
        from_node_path = from_node_key.split(".")
        from_node_value = dynamic_record.getval(from_node_path)
    
    to_node_value = record.get(to_node_key, None)
    if to_node_value is None:
        # Key does not exist in record, attempt to search via dot-path
        to_node_path = to_node_key.split(".")
        # dynamic_record = DynamicDict(record)
        to_node_value = dynamic_record.getval(to_node_path)
    
    return {
        from_param_key: from_node_value,
        to_param_key: to_node_value
    }

# def relationships_contain_list_targets(
#         from_node: TargetNode,
#         to_node: TargetNode,
#         records: list[dict]
# )-> bool:
    
#     # Presuming list is made of dictionaries all of the same schema
#     param_dict = relationship_from_to_dict(
#         from_node.record_key,
#         to_node.record_key,
#         from_node.record_key,
#         to_node.record_key,
#         records[0])
#     from_value = param_dict[from_node.record_key]
#     to_value = param_dict[to_node.record_key]
#     if isinstance(from_value, list) or isinstance(to_value, list):
#         return True
#     else:
#         return False

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

def new_relationships_from_singled_relationship_with_lists(
        spec: Relationships,
        record: list[dict])->list[Relationships]:
        """Generate a list of new relationships if spec targets lists.

        Args:
            spec (Relationships): Relationships object
            record (dict): Single record that contains a list to be split into a new Relationships object 

        Returns:
            list[tuple]: List of Relationships
        """

        # Create new Relationships object and feed into a chunked_query function. Extend result
        keypath = keypath_to_1st_list(
            spec.to_node.record_key,
            record
        )
        if keypath is None:
            return None
        
        new_records = value_for_keypath(keypath, record)
        if new_records is None:
            return None

        # Need a new record key for new to nodes in new relationships                    
        new_record_key = spec.to_node.record_key.removeprefix(keypath)
        if new_record_key.startswith("."):
            new_record_key = new_record_key[1:]
        if new_record_key is None or new_record_key == "":
            raise Exception(f'No new record key to assign to new to_node.record_key. Original record key: {spec.to_node.record_key}, first record: {record}')
        
        new_to_node = TargetNode(
            node_label = spec.to_node.node_label,
            node_key = spec.to_node.node_key,
            record_key = new_record_key # Updated
        )
        new_relationships = Relationships(
            type = spec.type,
            from_node = spec.from_node,
            to_node = new_to_node, # Updated
            records = new_records, # Updated
            auto_exclude_keys = spec.auto_exclude_keys,
            exclude_keys = spec.exclude_keys,
            dedupe = spec.dedupe
        )
        return new_relationships

def new_relationships_from_relationships_with_lists(
        spec: Relationships)->list[Relationships]:

        result = []
        for record in spec.records:
            new_relationships = new_relationships_from_singled_relationship_with_lists(spec, record)
            if new_relationships is not None:
                result.append(new_relationships)
        return result