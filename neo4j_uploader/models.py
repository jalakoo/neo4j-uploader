from pydantic import BaseModel, Field
from typing import Optional

class Neo4jConfig(BaseModel):
    neo4j_uri : str
    neo4j_password: str
    neo4j_user : str = Field(default="neo4j")
    neo4j_database : str = Field(default="neo4j")
    max_batch_size : int = Field(default=500)
    overwrite : bool = False

class UploadResult(BaseModel):
    was_successful : bool
    time_to_complete: Optional[float] = None
    nodes_created: Optional[int] = None
    relationships_created: Optional[int] = None
    properties_set: Optional[int] = None
    nodes_skipped: list[dict] = Field(default=[])
    relationships_skipped: list[dict] = Field(default=[]),
    error_message: Optional[str] = None

class Nodes(BaseModel):
    labels: list[str]
    records: list[dict]
    constraints: Optional[list[str]] = []
    dedupe : Optional[bool] = True

class TargetNode(BaseModel):
    label: Optional[str]
    record_key: str
    node_prop: str
    keep: Optional[bool] = False

class Relationships(BaseModel):
    type: str
    from_node: TargetNode
    to_node: TargetNode
    records : list[dict]
    dedupe: Optional[bool] = True

class GraphData(BaseModel):
    config: Optional[Neo4jConfig] = None
    nodes: Optional[list[Nodes]] = []
    relationships: Optional[list[Relationships]] = []