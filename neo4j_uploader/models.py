from datetime import datetime, timedelta
from pydantic import BaseModel, Field, field_validator
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

    neo4j_uri: str
    neo4j_password: str
    neo4j_user: str = Field(default="neo4j")
    neo4j_database: str = Field(default="neo4j")
    max_batch_size: int = Field(default=500)
    overwrite: bool = False

    def creds(self) -> tuple[str, str, str]:
        """Convenience for providing tuple of Neo4j credentials as (uri, user, password).

        Returns:
            tuple(str, str, str): Neo4j credentials uri, user, password
        """


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
    dedupe: Optional[bool] = True


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
    records: list[dict]
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
        started_at (datetime): Start time of the upload.

        was_successful (bool): True if upload was successful. See error_message for failure details.

        records_total (int): Total number of records to upload. Does not need to be actual number of Nodes and Relationships to upload. Can be arbitrary (ie total number of batches).

        records_completed (int): Number of records uploaded. This is an arbitrary number and not indicative of actual number of nodes or relationships uploaded.

        seconds_to_complete (float): Time taken to upload Nodes and Relationships. Value in seconds.

        nodes_created (int): Number of nodes created.

        relationships_created (int): Number of relationships created.

        properties_set (int): Number of properties set.

        error_message (str): Error message if upload failed.
    """

    started_at: datetime
    was_successful: bool
    records_total: int
    records_completed: Optional[int] = 0
    finished_at: Optional[datetime] = None
    seconds_to_complete: Optional[float] = None
    nodes_created: Optional[int] = None
    relationships_created: Optional[int] = None
    properties_set: Optional[int] = None
    error_message: Optional[str] = None

    def __repr__(self):
        return (
            f"UploadResult(\n"
            f"    started_at={self.started_at!r},\n"
            f"    was_successful={self.was_successful!r},\n"
            f"    records_total={self.records_total!r},\n"
            f"    records_completed={self.records_completed!r},\n"
            f"    finished_at={self.finished_at!r},\n"
            f"    seconds_to_complete={self.seconds_to_complete!r},\n"
            f"    nodes_created={self.nodes_created!r},\n"
            f"    relationships_created={self.relationships_created!r},\n"
            f"    properties_set={self.properties_set!r},\n"
            f"    error_message={self.error_message!r}\n"
            f")"
        )

    def float_completed(self) -> float:
        """Returns percent complete as a float value between 0.0 (0%) and 1.0 (100%)

        Returns:
            float: Float equivalent of the percent complete.
        """
        return round(self.records_completed / self.records_total, 2)

    def seconds_remaining(self) -> float:
        """Calculate the number of seconds remaining to complete the upload.

        Returns:
            float: Number of seconds remaining to complete the upload.
        """
        time_elapsed = (datetime.now() - self.started_at).total_seconds()
        current_rate = time_elapsed / self.records_completed
        records_remaining = self.records_total - self.records_completed
        return current_rate * records_remaining

    def readable_time_remaining(self) -> str:
        """Returns a human readable string of the time remaining to complete the upload.

        Returns:
            str: Human readable string of the time remaining to complete the upload. Showing in hours, minutes, and seconds. (ie 1 hour, 2 minutes, 30 seconds)
        """
        seconds_remaining = self.seconds_remaining()
        hours, remainder = divmod(seconds_remaining, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds"

    def projected_time_to_complete(self) -> datetime:
        """Calculate the projected end time of the upload.

        Returns:
            datetime: Projected end time of the upload.
        """
        return self.started_at + timedelta(seconds=self.seconds_remaining())

    def time_since_start(self) -> datetime:
        """Calculate the number of seconds since the start of the upload.

        Returns:
            datetime: Datetime since the start of the upload.
        """
        return datetime.now() - self.started_at
