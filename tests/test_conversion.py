import pytest
from pydantic import ValidationError
from neo4j_uploader.conversion import elements, nodes_query, chunked_nodes_query, all_node_queries
from neo4j_uploader.models import Neo4jConfig, Nodes, Relationships, TargetNode
import logging
from neo4j_uploader.logger import ModuleLogger

class TestElements():

    @pytest.mark.usefixtures("caplog")
    def test_elements_dedupe(self, caplog):
        caplog.set_level(logging.DEBUG, logger='__temp_convertor__')

        records = [
            {"name": "John", "age": 30},
            {"name": "John", "age": 30}
        ]
        _, params = elements("test", records)
        assert len(params) == 2

    def test_elements_no_dedupe(self):
        records = [
            {"name": "John", "age": 30},
            {"name": "John", "age": 30}
        ]
        _, params = elements("test", records, dedupe=False)
        assert len(params) == 4  

    def test_elements_exclude_keys(self):
        records = [
            {"name": "John", "age": 30, "id": 1}
        ]
        _, params = elements("test", records, exclude_keys=["id"])
        assert "id" not in params
        assert "id_batch_0" not in params

    def test_elements_sorts_keys(self):
        records = [
            {"name": "John", "age": 30}
        ]
        string, params = elements("test", records)
        assert string == " {`age`:$age_test_0, `name`:$name_test_0}"
        assert "age_test_0" in params.keys()
        assert "name_test_0" in params.keys()

    def test_elements_handles_empty_records(self):
        records = []
        string, params = elements("test", records)
        assert string == None
        assert params == {}

    def test_elements_multiple(self):
        records = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 21}
        ]
        string, params = elements("test", records)
        assert string == " {`age`:$age_test_0, `name`:$name_test_0}, {`age`:$age_test_1, `name`:$name_test_1}"
        assert len(params) == 4  

class TestNodesQuery():
    def test_nodes_query_single_label(self):
        records = [{"name": "John"}] 
        labels = ["Person"]
        query, params = nodes_query("test", records, labels)

        assert query.startswith("WITH")
        assert "MERGE" in query
        assert "SET n += node" in query
        assert "n:`Person`" in query

    def test_nodes_query_multiple_labels(self):
        records = [{"name": "John"}]
        labels = ["Person", "User"]
        query, params = nodes_query("test", records, labels)

        assert "n:`Person`" in query 
        assert "n:`User`" in query

    def test_nodes_query_no_dedupe(self):
        records = [{"name": "John"}]
        labels = ["Person"]
        query, params = nodes_query("test", records, labels, dedupe=False)

        assert "CREATE" in query
        assert "MERGE" not in query

    def test_nodes_query_exclude_keys(self):
        records = [{"name": "John", "id": 1}]
        labels = ["Person"]
        query, params = nodes_query("test", records, labels, exclude_keys=["id"])

        assert "id" not in query

    def test_nodes_query_no_records(self):
        records = []
        labels = ["Person"]
        query, params = nodes_query("test", records, labels)

        assert query == None
        assert params == {}

class TestChunkedNodesQuery():
    def test_chunked_nodes_query_splits_batches(self):
        nodes = Nodes(
            records=[{'name': 'Node 1'}, {'name': 'Node 2'}],
            labels=['Label']
        )
        config = Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1)
        result = chunked_nodes_query(nodes, config)
        assert len(result) == 2

    def test_chunked_nodes_query_returns_queries(self):
        nodes = Nodes(
            records=[{'name': 'Node 1'}], 
            labels=['Label']
        )
        config = Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1
        )
        result = chunked_nodes_query(nodes, config)
        assert result[0][0].startswith('WITH')

    def test_all_node_queries_combines_results(self):
        nodes1 = Nodes(records=[{'name': 'Node 1'}], labels=['Label'])
        nodes2 = Nodes(records=[{'name': 'Node 2'}], labels=['Label'])
        config = Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1
        )
        result = all_node_queries([nodes1, nodes2], config)
        assert len(result) == 2

    def test_all_node_queries_no_nodes(self):
        result = all_node_queries([], Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
        ))
        assert result == []

class TestAllNodeQueries():
    def test_all_node_queries_with_nodes(self):
        nodes1 = Nodes(records=[{'name': 'Node 1'}], labels=['Label'])
        nodes2 = Nodes(records=[{'name': 'Node 2'}], labels=['Label'])
        config = Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1
        )
        result = all_node_queries([nodes1, nodes2], config)
        assert len(result) == 2

    def test_all_node_queries_empty_list(self):
        result = all_node_queries([], Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1
        ))
        assert result == []

    def test_all_node_queries_combines_batches(self):
        nodes1 = Nodes(records=[{'name': 'Node 1'}], labels=['Label'])
        nodes2 = Nodes(records=[{'name': 'Node 2'}], labels=['Label'])
        config = Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1
        )
        result = all_node_queries([nodes1, nodes2], config)
        assert len(result) == 2
        assert result[0][0].startswith('WITH')
        assert result[1][0].startswith('WITH')

    def test_all_node_queries_applies_config(self):
        nodes = Nodes(records=[{'name': 'Node 1'}, {'name': 'Node 2'}], 
                    labels=['Label'])
        config = Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1
        )
        result = all_node_queries([nodes], config)
        assert len(result) == 2