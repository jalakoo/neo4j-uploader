from pydantic import BaseModel, Field
from typing import Optional

# Specify Google doctstring type for pdoc auto doc generation
__docformat__ = "google"

class Neo4jConfig(BaseModel):
    """
    Object for specifying target local or hosted Neo4j database instance to upload to data to.

    Args:
        neo4j_uri (str): The URI of the Neo4j instance to upload to.
        neo4j_password (str): The password for the Neo4j instance to upload to.
        neo4j_user (str): The username for the Neo4j instance to upload to.
        neo4j_database (str): The name of the Neo4j database to upload to. Default 'neo4j'.
        max_batch_size (int): Maximum number of nodes to upload in a single batch. Default 500.
        overwrite (bool): Overwrite existing nodes. Default False.
    """
    neo4j_uri : str
    neo4j_password: str
    neo4j_user : str = Field(default="neo4j")
    neo4j_database : str = Field(default="neo4j")
    max_batch_size : int = Field(default=500)
    overwrite : bool = False

class Nodes(BaseModel):
    """Configuration object for uploading nodes to a Neo4j database.

    Args:
        labels (list[str]): List of node labels to upload (ie Person, Place, etc).
        key (str): Unique key for each node.
        records (list[dict]): List of dictionary objects containing node data.
        exclude_keys (list[str]): List of keys to exclude from upload.
        dedupe (bool): Remove duplicate entries. Default True.
    """
    labels: list[str]
    key: str
    records: list[dict]
    exclude_keys: Optional[list[str]] = []
    dedupe : Optional[bool] = True

class TargetNode(BaseModel):
    """Node specification object for uploading relationships to a Neo4j database.

    Args:
        node_label (str): Optional Node label of the target node. Including a label is more performant than without.
        node_key (str): Target key or property name that identifies a unique node.
        record_key (str): Key within the relationship record, whose value will be mapped to the node_key.
    """
    node_label: Optional[str] = None
    node_key: str
    record_key: str

class Relationships(BaseModel):
    """Configuration object for uploading relationships to a Neo4j database.

    Args:
        type (str): Relationship type (ie FOLLOWS, WORKS_AT, etc).
        from_node (TargetNode): TargetNode object for the source node.
        to_node (TargetNode): TargetNode object for the target node.
        records (list[dict]): List of dictionary objects containing relationship data.
        exclude_keys (list[str]): List of keys to exclude from upload.
        auto_exclude_keys (bool): Automatically exclude keys used to reference nodes used in the from_node and to_node arguments. Default True.
        dedupe (bool): Remove duplicate entries. Default True.
    """
    type: str
    from_node: TargetNode
    to_node: TargetNode
    records : list[dict]
    exclude_keys: Optional[list[str]] = []
    auto_exclude_keys: Optional[bool] = True
    dedupe: Optional[bool] = True

class GraphData(BaseModel):
    """Object representation of nodes and relationships specifications and records to upload to a specified Neo4j database.

    Args:
        nodes (list[Nodes]): List of Nodes configuration objects for specifying nodes to upload.
        relationships (list[Relationships]): Optional List of Relationships configuration objects for specifying relationships to upload.
    """
    nodes: list[Nodes] = []
    relationships: Optional[list[Relationships]] = []

class UploadResult(BaseModel):
    """Result object for uploading nodes to a Neo4j database.

    Args:
        was_successful (bool): True if upload was successful.
        seconds_to_complete (float): Time taken to upload nodes in seconds.
        nodes_created (int): Number of nodes created.
        relationships_created (int): Number of relationships created.
        properties_set (int): Number of properties set.
        error_message (str): Error message if upload failed.
    """
    was_successful : bool
    seconds_to_complete: Optional[float] = None
    nodes_created: Optional[int] = None
    relationships_created: Optional[int] = None
    properties_set: Optional[int] = None
    error_message: Optional[str] = None