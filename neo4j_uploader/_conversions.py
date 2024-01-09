# used for converting legacy or alternate schemas to default schema
from neo4j_uploader.models import Nodes, Relationships, TargetNode

def convert_legacy_node_records(
        data: dict,
        dedupe: bool,
        node_key: str
    ) -> list[Nodes]:
    """Convert simple nodes data into new Nodes model object

    Args:
        data (dict): Dictionary containing simple nodes data

    Returns:
        list[Nodes]: List of Nodes objects
    """

    result = []

    # Each key will become a new Nodes specification object
    for key in data.keys():
        node_label = key
        node_key = node_key
        new_node_spec = Nodes(
            labels= [node_label],
            key=node_key,
            records = data[key],
            dedupe=dedupe,
        )
        result.append(new_node_spec)
    
    return result

def convert_legacy_relationship_records(
        data: dict, 
        dedupe: bool,
        node_key: str
    ) -> list[Relationships]:
    """Convert simple relationship data to new Relationships model object

    Args:
        data (dict): Dictionary containing nodes and relationships data

    Returns:
        list[Relationships]: List of Relationships objects
    """

    result = []

    # Each key will become a new Relationships specification object
    for key in data.keys():
        rel_type = key
        from_node = TargetNode(
            node_key = node_key,
            record_key = f"_from_{node_key}",
        )
        to_node = TargetNode(
            node_key = node_key,
            record_key = f"_to_{node_key}",
        )
        records = data[key]

        new_rel_spec = Relationships(
            type = rel_type,
            from_node = from_node,
            to_node = to_node,
            records = records,
            dedupe = dedupe,
        )
        result.append(new_rel_spec)
    
    return result