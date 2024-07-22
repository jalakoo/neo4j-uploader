from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from typing import Optional
from neo4j_uploader._logger import logger

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

        records_total (int): Total number of records to upload. Does not need to be actual number of Nodes and Relationships to upload. Can be arbitrary (ie total number of queries/batches).

        records_completed (int): Number of records uploaded. This is an arbitrary number (ie queries/batches) and not indicative of actual number of nodes or relationships uploaded.

        finished_at (datetime): End time of the upload.

        seconds_to_complete (float): Time taken to upload Nodes and Relationships. Value in seconds.

        was_successful (bool): True if upload was successful. See error_message for failure details if False. Default False until completed.

        nodes_created (int): Number of nodes created.

        relationships_created (int): Number of relationships created.

        properties_set (int): Number of properties set.

        error_message (str): Error message if upload failed.
    """

    started_at: datetime
    records_total: int
    records_completed: Optional[int] = 0
    finished_at: Optional[datetime] = None
    was_successful: bool = False
    seconds_to_complete: Optional[float] = None
    nodes_created: int = 0
    relationships_created: int = 0
    properties_set: int = 0
    error_message: Optional[str] = ""

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
        return float(f"{self.records_completed / self.records_total:.2f}")

    def projected_seconds_to_complete(self) -> int:
        """Returns projected seconds to complete based on current rate of completion.

        Returns:
            int: Projected seconds to complete. Returns -1 if no records have been completed or unable to calculate.
        """

        # Check if there have been any records completed
        if self.records_completed == 0:
            return -1

        # Ensure that the total number of records is positive and greater than completed records
        if self.records_total <= self.records_completed:
            return 0

        # Calculate the time elapsed since the start
        elapsed_time = (datetime.now() - self.started_at).total_seconds()
        # logger.debug(f"Elapsed time: {elapsed_time} seconds")

        # Avoid division by zero if the elapsed time is zero
        if elapsed_time == 0:
            return -1

        # Calculate the rate of processing records per second
        records_per_second = self.records_completed / elapsed_time
        # logger.debug(f"Records per second: {records_per_second}")

        # Calculate the number of remaining records
        remaining_records = self.records_total - self.records_completed
        # logger.debug(f"Remaining records: {remaining_records}")

        # Project the remaining time based on the current rate
        remaining_seconds = remaining_records * records_per_second
        # logger.debug(f"Projected remaining seconds: {remaining_seconds}")

        # Return the projected time as an integer
        return int(remaining_seconds)

    def projected_completion_time(self) -> datetime:
        """Returns the projected end time of the upload based on the current rate of completion.

        Returns:
            datetime: Projected completion time of the upload.
        """
        return self.started_at + timedelta(seconds=self.projected_seconds_to_complete())
