from timeit import default_timer as timer
from neo4j_uploader.n4j import execute_query, reset
from neo4j_uploader.upload_utils import upload_nodes, upload_relationships
from neo4j_uploader.logger import ModuleLogger
import json

def start_logging():
    """
    Enables logging from the graph-data-generator module. Log level matches the existing log level of the calling module.
    """
    logger = ModuleLogger()
    logger.is_enabled = True
    logger.info("Graph-Data-Generator logging enabled")

def stop_logging():
    """
    Surpresses logging from the graph-data-generator module.
    """
    ModuleLogger().info(f'Discontinuing logging')
    ModuleLogger().is_enabled = False

def upload(
    neo4j_creds:(str, str, str), 
    data: str | dict,
    node_key : str = "_uid",
    dedupe_nodes : bool = True,
    dedupe_relationships : bool = True,
    should_overwrite: bool = False,
    database_name: str = 'neo4j',
    max_batch_size: int = 500,
    )-> (float, int, int, int):
    """
    Uploads a dictionary of records to a target Neo4j instance.

    Args:
        neo4j_credits: Tuple containing the hostname, username, and password of the target Neo4j instance. The host name should contain only the database name and not the protocol. For example, if the host name is 'neo4j+s://<unique_db_id>.databases.neo4j.io', the host string to use is '<unique_db_id>.databases.neo4j.io'.

        data: A .json string or dictionary of records to upload. The dictionary keys must contain a 'nodes' and 'relationships' key. The value of which should be a list of dictionaries, each of these dictionaries contain the property keys and values for the nodes and relationships to be uploaded, respectively.

        node_key: The key in the dictionary that contains the unique identifier for the node. Relationship generation will also use this to find the from and to Nodes it connects to. Default is '_uid'.

        dedupe_nodes: Should nodes only be created once. False means a new node will always be created. True means if an existing node exists, only the properties will be updated. Default True.

        dedupe_relationships: Should relationships only create 1 of a given relationship between the same from and to node. False means a new relationship will always be created. True means if an existing relationship exists between the target nodes, only the properties will be updated. If no prior relationship, a new one will be created. Default True.

        should_overwrite: A boolean indicating whether the upload should overwrite existing data. If set to True, the upload will delete all existing nodes and relationships before uploading. Default is False.
    
    Returns:
        Tuple of result data: float of time to complete, int of nodes created, int of relationships created, int of total node and relationship properties set.
    
    Raises:
        Exceptions if data is not in the correct format or if the upload ungracefully fails.
    """
    # Convert to dictionary if data is string
    if isinstance(data, str) is True:
        try:
            data = json.loads(data)
        except Exception as e:
            raise Exception(f'Input data string not a valid JSON format: {e}')

    if node_key is None or node_key == "":
        raise Exception(f'node_key cannot be None or an empty string')
    
    # Start clock
    start = timer()

    # Upload nodes data first
    nodes = data.get('nodes', None)
    if nodes is None:
        raise Exception('No nodes data found in input data')
    
    if should_overwrite is True:
        reset(neo4j_creds)

    nodes_created, node_props_set = upload_nodes(
        neo4j_creds, 
        nodes, 
        node_key= node_key, 
        dedupe=dedupe_nodes, 
        database= database_name,
        max_batch_size= max_batch_size)
    
    all_props_set = node_props_set
    relationships_created = 0,

    # Upload relationship data next
    rels = data.get('relationships', None)
    if rels is not None and len(rels) > 0:
        ModuleLogger().info(f'Begin processing relationships: {rels}')
        relationships_created, relationship_props_set = upload_relationships(
            neo4j_creds, 
            rels, 
            node_key, 
            dedupe = dedupe_relationships, 
            database=database_name,
            max_batch_size=max_batch_size)
        
        all_props_set += relationship_props_set

    stop = timer()
    time_to_complete = round((stop - start), 4)

    return time_to_complete, nodes_created, relationships_created, all_props_set

def clear_db(creds: (str, str, str), database: str):
    return reset(creds, database)
