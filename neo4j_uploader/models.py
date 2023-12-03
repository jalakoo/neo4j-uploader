from pydantic import BaseModel, Field
from typing import Optional

class Neo4jConfig(BaseModel):
    neo4j_uri : str
    neo4j_password: str
    neo4j_user : str = Field(default="neo4j")
    neo4j_database : str = Field(default="neo4j")
    max_batch_size : int = Field(default=500)

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
    key: str
    records: list[dict]
    dedupe : bool = Field(default=True)

class TargetNode(BaseModel):
    label: Optional[str]
    properties: dict

class Relationship(BaseModel):
    type: str
    from_node: TargetNode
    to_node: TargetNode
    dedupe: bool = Field(default=True)
    properties : Optional[dict]

class GraphData(BaseModel):
    config: Optional[Neo4jConfig]
    nodes: list[Nodes]
    relationships: list[Relationship] = Field(default=[])