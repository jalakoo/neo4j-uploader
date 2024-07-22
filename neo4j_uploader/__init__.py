# from neo4j_uploader._logger import ModuleLogger
from neo4j_uploader._logger import logger, stream_handler, logging
from neo4j_uploader._queries import specification_queries
from neo4j_uploader._n4j import reset, upload_query, validate_credentials
from neo4j_uploader._upload_utils import upload_nodes, upload_relationships
from neo4j_uploader.models import (
    UploadResult,
    Neo4jConfig,
    GraphData,
)
from neo4j_uploader.errors import InvalidCredentialsError, InvalidPayloadError
from neo4j_uploader._conversions import (
    convert_legacy_node_records,
    convert_legacy_relationship_records,
)
from timeit import default_timer as timer
from typing import Callable, Optional, Dict, Iterator, Union, Tuple
from collections.abc import Generator
import warnings
import json
import time
from datetime import datetime

logger.setLevel(logging.INFO)

# Specify Google doctstring type for pdoc auto doc generation
__docformat__ = "google"


def start_logging():
    """
    Enables logging from this module. Log level matches the existing log level of the calling module.
    """
    warnings.warn(
        "The 'start_logging' function is deprecated and will be removed in a future version. Use standard logger.getLogger('neo4j_uploader')  instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    logging.warning(
        "DEPRECATED: The 'start_logging' function is deprecated and will be removed in a future version. Use standard logger.getLogger('neo4j_uploader') instead."
    )
    logger.info("Neo4j Uploader logging enabled")


def stop_logging():
    """
    Surpresses logging from this module.

    Deprecated:
        This function will be removed in the next version. Use standard logger.getLogger("neo4j_uploader") instead.
    """
    warnings.warn(
        "The 'stop_logging' function is deprecated and will be removed in a future version. See the _logger.py file for disabling the stream_handler or set the logger.getLogger('neo4j_uploader') to a higher level.",
        DeprecationWarning,
        stacklevel=2,
    )
    logging.warning(
        "DEPRECATED: The'stop_logging' function is deprecated and will be removed in a future version. See the _logger.py file for disabling the stream_handler or set the logger.getLogger('neo4j_uploader') to a higher level."
    )
    logger.info("Disabling Neo4j Uploader logging")
    logger.removeHandler(stream_handler)


# Using single UploadResult

# def batch_upload_generator(
#     config: dict | Neo4jConfig,
#     data: dict | GraphData,
#     query_params: list[(str, dict)],
#     progress_callback: Optional[Callable[[UploadResult], None]] = None,
# ):
#     """
#     Uploads a dictionary containing nodes, relationships, and target Neo4j database information as a generator.

#     Args:
#         config (dict or Neo4jConfig): A Neo4jConfig object or dict that can be converted to a Neo4jConfig object.
#         data (dict or GraphData): A GraphData object or a dict that can be converted to a GraphData object.
#         query_params (list): A list of tuples containing query strings and parameters.
#         progress_callback (Callable[[UploadResult], None], optional): A callback function to track upload progress.

#     Yields:
#         UploadResult: The overall result of the upload process.

#     Raises:
#         neo4j.exceptions: A Neo4j exception if credentials are invalid or database can not be accessed.
#         InvalidCredentialsError: If credentials are missing or malformed.
#         InvalidPayloadError: If payload schema is missing or unsupported.
#     """
#     try:
#         cdata = Neo4jConfig.model_validate(config)
#     except Exception as e:
#         raise InvalidCredentialsError(e)

#     validate_credentials((cdata.neo4j_uri, cdata.neo4j_user, cdata.neo4j_password))

#     try:
#         gdata = GraphData.model_validate(data)
#     except Exception as e:
#         raise InvalidPayloadError(e)

#     neo4j_creds = (cdata.neo4j_uri, cdata.neo4j_user, cdata.neo4j_password)
#     neo4j_database = cdata.neo4j_database

#     if cdata.overwrite:
#         reset(neo4j_creds, neo4j_database)

#     overall_result = UploadResult(
#         started_at=datetime.now(),
#         records_total=len(query_params),
#         was_successful=True,
#         nodes_created=0,
#         relationships_created=0,
#         properties_set=0,
#     )

#     composite_error_messages = []

#     for index, qp in enumerate(query_params):
#         try:
#             summary = upload_query(
#                 creds=neo4j_creds,
#                 query=qp[0],
#                 params=qp[1],
#                 database=neo4j_database,
#             )

#             props = getattr(summary.counters, "properties_set", 0)
#             nodes = getattr(summary.counters, "nodes_created", 0)
#             relationships = getattr(summary.counters, "relationships_created", 0)

#             overall_result.properties_set += props
#             overall_result.nodes_created += nodes
#             overall_result.relationships_created += relationships
#             overall_result.records_completed += 1

#             if progress_callback:
#                 progress_callback(
#                     UploadResult(
#                         started_at=overall_result.started_at,
#                         records_total=len(query_params),
#                         records_completed=index + 1,
#                         was_successful=True,
#                         seconds_to_complete=(
#                             datetime.now() - overall_result.started_at
#                         ).total_seconds(),
#                         nodes_created=overall_result.nodes_created,
#                         relationships_created=overall_result.relationships_created,
#                         properties_set=overall_result.properties_set,
#                     )
#                 )

#         except Exception as e:
#             error_message = (
#                 f"Error processing batch {index + 1} of {len(query_params)}: {str(e)}"
#             )
#             composite_error_messages.append(error_message)
#             overall_result.was_successful = False

#             if progress_callback:
#                 progress_callback(
#                     UploadResult(
#                         started_at=overall_result.started_at,
#                         records_total=len(query_params),
#                         records_completed=index + 1,
#                         was_successful=False,
#                         error_message=error_message,
#                         seconds_to_complete=(
#                             datetime.now() - overall_result.started_at
#                         ).total_seconds(),
#                         nodes_created=overall_result.nodes_created,
#                         relationships_created=overall_result.relationships_created,
#                         properties_set=overall_result.properties_set,
#                     )
#                 )

#     overall_result.finished_at = datetime.now()
#     overall_result.seconds_to_complete = (
#         overall_result.finished_at - overall_result.started_at
#     ).total_seconds()

#     if composite_error_messages:
#         overall_result.error_message = "\n".join(composite_error_messages)

#     yield overall_result


def batch_upload_generator(
    config: dict | Neo4jConfig,
    data: dict | GraphData,
    query_params: list[(str, dict)],
    progress_callback: Optional[Callable[[None], UploadResult]] = None,
):
    """
    Uploads a dictionary containing nodes, relationships, and target Neo4j database information as a generator.

    Args:
        config (dict or Neo4jConfig): A Neo4jConfig object or dict that can be converted to a Neo4jConfig object.
        data (dict or GraphData): A GraphData object or a dict that can be converted to a GraphData object.
        progress_callback (Callable[[None], UploadProgress], optional): A callback function to track upload progress.

    Returns:
        A generator of UploadResult objects

    Raises:
        neo4j.exceptions: A Neo4j exception if credentials are invalid or database can not be accessed.
        InvalidCredentialsError: If credentials are missing or malformed.
        InvalidPayloadError: If payload schema is missing or unsupported.
    """
    try:
        cdata = Neo4jConfig.model_validate(config)
    except Exception as e:
        raise InvalidCredentialsError(e)

    validate_credentials((cdata.neo4j_uri, cdata.neo4j_user, cdata.neo4j_password))

    try:
        _ = GraphData.model_validate(data)
    except Exception as e:
        raise InvalidPayloadError(e)

    neo4j_creds = (cdata.neo4j_uri, cdata.neo4j_user, cdata.neo4j_password)
    neo4j_database = cdata.neo4j_database

    if cdata.overwrite:
        reset(neo4j_creds, neo4j_database)

    overall_result = UploadResult(
        started_at=datetime.now(),
        records_total=len(query_params),
        was_successful=True,
        nodes_created=0,
        relationships_created=0,
        properties_set=0,
    )

    composite_error_messages = None

    for index, qp in enumerate(query_params):
        summary = upload_query(
            creds=neo4j_creds,
            query=qp[0],
            params=qp[1],
            database=neo4j_database,
        )

        props = getattr(summary.counters, "properties_set", 0)
        nodes = getattr(summary.counters, "nodes_created", 0)
        relationships = getattr(summary.counters, "relationships_created", 0)

        overall_result.properties_set += props
        overall_result.nodes_created += nodes
        overall_result.relationships_created += relationships
        overall_result.records_completed += 1

        # Return iterative result / status
        now = datetime.now()

        try:
            batch_result = UploadResult(
                started_at=now,
                records_total=len(query_params),
                records_completed=index + 1,
                was_successful=True,
                finished_at=now,
                seconds_to_complete=(now - overall_result.started_at).total_seconds(),
                nodes_created=nodes,
                relationships_created=relationships,
                properties_set=props,
            )

            if progress_callback:
                progress_callback(batch_result)

        except Exception as e:
            error_message = (
                f"Error processing batch {index} of {len(query_params)}: {e}."
            )
            batch_result = UploadResult(
                started_at=now,
                records_total=len(query_params),
                was_successful=False,
                error_message=error_message,
            )
            composite_error_messages += error_message
        yield batch_result

    # Return overall/final result
    overall_result.finished_at = datetime.now()
    overall_result.seconds_to_complete = (
        overall_result.finished_at - overall_result.started_at
    ).total_seconds()
    yield overall_result


def batch_upload(
    config: dict | Neo4jConfig,
    data: dict | GraphData,
    progress_callback: Optional[Callable[[None], UploadResult]] = None,
) -> Union[Generator[UploadResult, None, UploadResult], UploadResult]:
    """Uploads a dictionary containing nodes, relationships, and target Neo4j database information.
    Automatically detects whether it's being used as an iterator or a normal function.

    Args:
        config (dict or Neo4jConfig): A Neo4jConfig object or dict that can be converted to a Neo4jConfig object.

        data (dict or GraphData): A GraphData object or a dict that can be converted to a GraphData object.

        progress_callback (Callable[[None], UploadProgress], optional): A callback function to track upload progress. Not needed if using function as a generator. Default is None.

    Returns:
        Union[Generator[UploadResult, None, UploadResult], UploadResult]: A generator of UploadResult objects or a single UploadResult object.

    Raises:
        neo4j.exceptions: A Neo4j exception if credentials are invalid or database can not be accessed.
        InvalidCredentialsError: If credentials are missing or malformed.
        InvalidPayloadError: If payload schema is missing or unsupported.
    """

    now = datetime.now()

    try:
        cdata = Neo4jConfig.model_validate(config)
    except Exception as e:
        raise InvalidCredentialsError(e)

    validate_credentials((cdata.neo4j_uri, cdata.neo4j_user, cdata.neo4j_password))

    try:
        gdata = GraphData.model_validate(data)
    except Exception as e:
        raise InvalidPayloadError(e)

    query_params = specification_queries(gdata.nodes, cdata)
    query_params.extend(specification_queries(gdata.relationships, cdata))

    gen = batch_upload_generator(
        config=config,
        data=data,
        query_params=query_params,
        progress_callback=progress_callback,
    )

    try:
        first = next(gen)
    except StopIteration:
        return UploadResult(
            started_at=now,
            records_total=0,
            was_successful=False,
            error_message="No data to process",
        )

    # If we're here, we have at least one item
    def new_gen():
        yield first
        yield from gen

    try:
        # Check if the caller is iterating by attempting to fetch the second item
        next(new_gen())
        # If we get here, the caller is iterating
        return new_gen()
    except StopIteration:
        # This shouldn't happen as we've already yielded 'first'
        return first
    except Exception:
        # Handle any other exception and return the final UploadResult object
        pass

    # Consume the generator to get the final result
    final_result = first
    for result in new_gen():
        final_result = result

    return final_result


def upload(
    neo4j_creds: Tuple[str, str, str],
    data: str | dict,
    node_key: str = "_uid",
    dedupe_nodes: bool = True,
    dedupe_relationships: bool = True,
    should_overwrite: bool = False,
    database_name: str = "neo4j",
    max_batch_size: int = 500,
    progress_callback: Optional[Callable[[None], UploadResult]] = None,
) -> Union[Generator[UploadResult, None, UploadResult], UploadResult]:
    """
    Uploads a dictionary of simple node and relationship records to a target Neo4j instance specified in the arguments.

    Args:
        neo4j_creds: Tuple containing the hostname, username, password of the target Neo4j instance.

        data: A .json string or dictionary of records to upload.

        node_key: The key in the dictionary that contains the unique identifier for the node. Default is '_uid'.

        dedupe_nodes: Should nodes only be created once. Default True.

        dedupe_relationships: Should relationships only create 1 of a given relationship between the same from and to node. Default True.

        should_overwrite: A boolean indicating whether the upload should overwrite existing data. Default is False.

        database_name: String name of target Neo4j database. Default is 'neo4j'.

        max_batch_size: Integer maximum number of nodes or relationships to upload in a single Cypher batch. Default 500.

        progress_callback: Optional callback function to track upload progress. Unneeded if using function as a generator. Default is None.

    Returns:
        Union[Generator[UploadResult, None, UploadResult], UploadResult]: Either a generator of UploadResult objects or a single UploadResult object.

    Raises:
        Exception: If data is not in the correct format or if the upload ungracefully fails.
    """
    # Convert to dictionary if data is string
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception as e:
            raise Exception(f"Input data string not a valid JSON format: {e}")

    if data is None or len(data) == 0:
        raise Exception("data payload is empty or an invalid format")

    simple_nodes = data.get("nodes", None)
    simple_rels = data.get("relationships", None)

    nodes = convert_legacy_node_records(simple_nodes, dedupe_nodes, node_key)
    rels = convert_legacy_relationship_records(
        simple_rels, dedupe_relationships, node_key
    )

    uri, user, password = neo4j_creds
    config = Neo4jConfig(
        neo4j_uri=uri,
        neo4j_user=user,
        neo4j_password=password,
        neo4j_database=database_name,
        max_batch_size=max_batch_size,
        overwrite=should_overwrite,
    )

    return batch_upload(
        config=config,
        data={"nodes": nodes, "relationships": rels},
        progress_callback=progress_callback,
    )


def clear_db(creds: tuple[str, str, str], database: str):
    """Deletes all existing nodes and relationships in a target Neo4j database.

    Args:
        creds (str, str, str): Neo4j URI, username, and password.
        database (str): Target Neo4j database.

    Returns:
        summary (neo4j.ResultSummary): Result summary of the operation. See https://neo4j.com/docs/api/python-driver/current/api.html#resultsummary for more info.
    """
    return reset(creds, database)
