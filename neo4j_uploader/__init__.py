from neo4j_uploader._logger import ModuleLogger
from neo4j_uploader._queries import specification_queries
from neo4j_uploader._n4j import reset, upload_query
from neo4j_uploader._upload_utils import upload_nodes, upload_relationships
from neo4j_uploader.models import UploadResult, Neo4jConfig, GraphData
from timeit import default_timer as timer
from warnings import warn
import json

# Specify Google doctstring type for pdoc auto doc generation
__docformat__ = "google"

def start_logging():
    """
    Enables logging from this module. Log level matches the existing log level of the calling module.
    """
    logger = ModuleLogger()
    logger.is_enabled = True
    logger.info("Neo4j Uploader logging enabled")

def stop_logging():
    """
    Surpresses logging from this module.
    """
    ModuleLogger().info(f'Discontinuing logging')
    ModuleLogger().is_enabled = False


def batch_upload(
        config: dict | Neo4jConfig,
        data : dict | GraphData,
    ) -> UploadResult:
    """Uploads a dictionary containing nodes, relationships, and target Neo4j database information. The schema for nodes and relationships is more flexible and comprehensive than the schema for the earlier upload function.

    Args:
        config (dict or Neo4jConfig): A Neo4jConfig object or dict that can be converted to a Neo4jConfig object for defining target Neo4j database and credentials for upload.
        data (dict or GraphData): A GraphData object or a dict that can be converted to a GraphData object with specifications for nodes and relationships to upload

    Returns:
        UploadResult: Result object containing information regarding a successful or unsuccessful upload.
    """
     
    try:
        cdata = Neo4jConfig.model_validate(config)
    except Exception as e:
        return UploadResult(
            was_successful = False,
            error_message = f'{e}'
        )
    
    try:
        gdata = GraphData.model_validate(data)
    except Exception as e:
        return UploadResult(
            was_successful = False,
            error_message = f'{e}'
        )


    # Start clock for tracking processing time
    start = timer()
    total_nodes_created = 0
    total_relationships_created = 0
    total_properties_set = 0

    # Get list of tuples containing queries and accompanying params for driver execution
    query_params = specification_queries(gdata.nodes, cdata)
    query_params.extend(specification_queries(gdata.relationships, cdata))

    for qp in query_params:
        # Run queries and retrieve summary of upload
        summary = upload_query(
            creds = (cdata.neo4j_uri, cdata.neo4j_user, cdata.neo4j_password),
            query = qp[0],
            params = qp[1],
            database = cdata.neo4j_database
        )
        
        # Sample summary result
        # {'metadata': {'query': '<query>', 'parameters': {}, 'query_type': 'w', 'plan': None, 'profile': None, 'notifications': None, 'counters': {'_contains_updates': True, 'labels_added': 17, 'nodes_created': 17, 'properties_set': 78}, 'result_available_after': 73, 'result_consumed_after': 0}

        # {'metadata': {'query': "<rel_upload_query>", 'parameters': {}, 'query_type': 'w', 'plan': None, 'profile': None, 'notifications': None, 'counters': {'_contains_updates': True, 'relationships_created': 1, 'properties_set': 2}, 'result_available_after': 209, 'result_consumed_after': 0}

        # Sum up total nodes, relationshipts and props set
        try:
            props = summary.counters.properties_set
            total_properties_set += props
        except Exception as _:
            ModuleLogger().debug('No properties set in summary: {summary}')

        try:
            nodes = summary.counters.nodes_created
            total_nodes_created += nodes
        except Exception as _:
            pass

        try:
            relationships = summary.counters.relationships_created
            total_relationships_created += relationships
        except Exception as _:
            pass

    stop = timer()
    time_to_complete = round((stop - start), 4)

    return UploadResult(
        was_successful = True,
        error_message = None,
        seconds_to_complete = time_to_complete,
        nodes_created = total_nodes_created,
        relationships_created = total_relationships_created,
        properties_set = total_properties_set
    )


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
    Uploads a dictionary of simple node and relationship records to a target Neo4j instance specified in the arguments.

    Args:
        neo4j_creds: Tuple containing the hostname, username, password, and optionally a database name of the target Neo4j instance. The host name should contain only the database name and not the protocol. For example, if the host name is 'neo4j+s://<unique_db_id>.databases.neo4j.io', the host string to use is '<unique_db_id>.databases.neo4j.io'. The default database name is 'neo4j'.

        data: A .json string or dictionary of records to upload. The dictionary keys must contain a 'nodes' and 'relationships' key. The value of which should be a list of dictionaries, each of these dictionaries contain the property keys and values for the nodes and relationships to be uploaded, respectively.

        node_key: The key in the dictionary that contains the unique identifier for the node. Relationship generation will also use this to find the from and to Nodes it connects to. Default is '_uid'.

        dedupe_nodes: Should nodes only be created once. False means a new node will always be created. True means if an existing node exists, only the properties will be updated. Default True.

        dedupe_relationships: Should relationships only create 1 of a given relationship between the same from and to node. False means a new relationship will always be created. True means if an existing relationship exists between the target nodes, only the properties will be updated. If no prior relationship, a new one will be created. Default True.

        should_overwrite: A boolean indicating whether the upload should overwrite existing data. If set to True, the upload will delete all existing nodes and relationships before uploading. Default is False.

        database_name: String name of target Neo4j database.

        max_batch_size: Integer maximum number of nodes or relationships to upload in a single Cypher batch. Default 500.
    
    Returns:
        Tuple of result data: float of time to complete, int of nodes created, int of relationships created, int of total node and relationship properties set.
    
    Raises:
        Exceptions if data is not in the correct format or if the upload ungracefully fails.
    """
    warn("Upload is being deprecated, use batch_upload() instead; version=0.5.0", DeprecationWarning, stacklevel=2)

    # Convert to dictionary if data is string
    if isinstance(data, str) is True:
        try:
            data = json.loads(data)
        except Exception as e:
            raise Exception(f'Input data string not a valid JSON format: {e}')

    # if node_key is None or node_key == "":
    #     raise Exception(f'node_key cannot be None or an empty string')
    
    if data is None or len(data) == 0:
        raise Exception(f'data payload is empty or an invalid format')


    # Start clock
    start = timer()

    # TODO: Better check for missing data key
    
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
    """Deletes all existing nodes and relationships in a target Neo4j database.

    Args:
        creds (str, str, str): Neo4j URI, username, and password.
        database (str): Target Neo4j database.

    Returns:
        summary (neo4j.ResultSummary): Result summary of the operation. See https://neo4j.com/docs/api/python-driver/current/api.html#resultsummary for more info.
    """
    return reset(creds, database)