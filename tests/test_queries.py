import pytest
from pydantic import ValidationError
from neo4j_uploader.queries import node_elements, nodes_query, chunked_query, specification_queries, relationship_elements, relationships_query
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
        query, params = node_elements("test", records)
        assert len(params) == 2
        assert query == " {`age`:$age_test0, `name`:$name_test0}"
        assert params == {
            "age_test0": 30,
            "name_test0": "John"
        }

    def test_elements_no_dedupe(self):
        records = [
            {"name": "John", "age": 30},
            {"name": "John", "age": 30}
        ]
        _, params = node_elements("test", records, dedupe=False)
        assert len(params) == 4  

    def test_elements_exclude_keys(self):
        records = [
            {"name": "John", "age": 30, "id": 1}
        ]
        _, params = node_elements("test", records, exclude_keys=["id"])
        assert "id" not in params
        assert "id_batch_0" not in params

    def test_elements_sorts_keys(self):
        records = [
            {"name": "John", "age": 30}
        ]
        string, params = node_elements("test", records)
        assert string == " {`age`:$age_test0, `name`:$name_test0}"
        assert "age_test0" in params.keys()
        assert "name_test0" in params.keys()

    def test_elements_handles_empty_records(self):
        records = []
        string, params = node_elements("test", records)
        assert string == None
        assert params == {}

    def test_elements_multiple(self):
        records = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 21}
        ]
        string, params = node_elements("test", records)
        assert string == " {`age`:$age_test0, `name`:$name_test0}, {`age`:$age_test1, `name`:$name_test1}"
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
        assert params == {
            "name_test0": "John"
        }

    def test_nodes_query_multiple_labels(self):
        records = [{"name": "John"}]
        labels = ["Person", "User"]
        query, params = nodes_query("test", records, labels)

        assert "n:`Person`" in query 
        assert "n:`User`" in query

    def test_nodes_query_no_dedupe(self):
        records = [{"name": "John"},{"name": "John"}]
        labels = ["Person"]
        query, params = nodes_query("test", records, labels, dedupe=False)

        assert "CREATE" in query
        assert "MERGE" not in query
        assert len(params) == 2
        assert query == "WITH [ {`name`:$name_test0}, {`name`:$name_test1}] AS node_data\nUNWIND node_data AS node\nCREATE (n:`Person`)"

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

class TestRelationshipElements():
    def test_relationship_elements_basic(self):
        records = [{'from':'abc', 'to':'cde', 'since': 2022}]
        from_node = TargetNode(record_key='from', node_key='uid')
        to_node = TargetNode(record_key='to', node_key='uid')
        
        result_str, result_params = relationship_elements('test', records, from_node, to_node, exclude_keys=["from", "to"])
        
        assert result_str == "[$from_test0, $to_test0, {`since`:$since_test0}]"
        assert result_params == {
            'from_test0': 'abc',
            'to_test0': 'cde',
            'since_test0': 2022
        }

    def test_relationship_elements_no_props(self):
        records = [{'from':'abc', 'to':'cde'}]
        from_node = TargetNode(record_key='from', node_key='uid')
        to_node = TargetNode(record_key='to', node_key='uid')
        
        result_str, result_params = relationship_elements('test', records, from_node, to_node, exclude_keys=["from", "to"])
        
        assert result_str == "[$from_test0, $to_test0, {}]"
        assert result_params == {
            'from_test0': 'abc',
            'to_test0': 'cde'
        }

    def test_relationship_elements_multiple(self):
        records = [
            {'from':'abc','to':'cde','since': 2022},
            {'from':'abc','to':'cde','since': 2023},
        ]
        from_node = TargetNode(record_key='from', node_key='gid')
        to_node = TargetNode(record_key='to', node_key='gid')

        result_str, result_params = relationship_elements('test', records, from_node, to_node, exclude_keys=["from","to"])

        assert result_str == "[$from_test0, $to_test0, {`since`:$since_test0}], [$from_test1, $to_test1, {`since`:$since_test1}]"
        assert len(result_params) == 6

    def test_relationship_elements_no_exclusion(self):
        records = [
            {'from':'abc','to':'cde','since': 2022}
        ]
        from_node = TargetNode(record_key='from', node_key='gid')
        to_node = TargetNode(record_key='to', node_key='gid')

        result_str, result_params = relationship_elements('test', records, from_node, to_node)

        assert result_str == "[$from_test0, $to_test0, {`from`:$from_test0, `since`:$since_test0, `to`:$to_test0}]" 
        assert len(result_params) == 3

    def test_relationship_elements_dedupe(self):
        records = [
            {'from':'abc','to':'cde','since': 2022},
            {'from':'abc','to':'cde','since': 2022},
        ]
        from_node = TargetNode(record_key='from', node_key='gid')
        to_node = TargetNode(record_key='to', node_key='gid')

        result_str, result_params = relationship_elements('test', records, from_node, to_node, dedupe=True)

        assert result_str == "[$from_test0, $to_test0, {`from`:$from_test0, `since`:$since_test0, `to`:$to_test0}]" 
        assert len(result_params) == 3

    def test_relationship_elements_empty(self):
        records = []
        from_node = TargetNode(record_key='from', node_key='gid')
        to_node = TargetNode(record_key='to', node_key='gid')

        result_str, result_params = relationship_elements('test', records, from_node, to_node)

        assert result_str is None
        assert result_params == {}

class TestRelationshipsQuery():
    def test_relationships_query_basic(self):
        records = [{'from': 'a', 'to': 'b'}]
        from_node = TargetNode(record_key='from', node_key='gid')
        to_node = TargetNode(record_key='to', node_key='gid')
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS')
        
        assert 'MERGE' in query

        assert params['from_test0'] == 'a'
        assert params['to_test0'] == 'b'
        assert query == "WITH [[$from_test0, $to_test0, {`from`:$from_test0, `to`:$to_test0}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {`gid`:tuple[0]})\nMATCH (toNode {`gid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"

    def test_relationships_query_multiple(self):
        records = [{'from': 'a', 'to': 'b'}, {'from': 'c', 'to': 'd'}]
        from_node = TargetNode(record_key='from', node_key='gid')
        to_node = TargetNode(record_key='to', node_key='gid')
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS')
        
        assert len(params) == 4
        assert 'UNWIND' in query

    def test_relationships_query_empty(self):
        records = []
        from_node = TargetNode(record_key='from', node_key='gid')
        to_node = TargetNode(record_key='to', node_key='gid')
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS')
        
        assert query is None
        assert params == {}
        
    def test_relationships_query_no_dedupe(self):
        records = [{'from': 'a', 'to': 'b'}, {'from': 'a', 'to': 'b'}]
        from_node = TargetNode(record_key='from', node_key='gid')
        to_node = TargetNode(record_key='to', node_key='gid')
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS', dedupe=False)

        assert 'CREATE' in query
        assert len(params) == 4
        assert query == "WITH [[$from_test0, $to_test0, {`from`:$from_test0, `to`:$to_test0}], [$from_test1, $to_test1, {`from`:$from_test1, `to`:$to_test1}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {`gid`:tuple[0]})\nMATCH (toNode {`gid`:tuple[1]})\nCREATE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"

    def test_relationships_query_exclude(self):
        records = [{'from': 'a', 'to': 'b'}, {'from': 'a', 'to': 'b'}]
        from_node = TargetNode(record_key='from', node_key='gid')
        to_node = TargetNode(record_key='to', node_key='gid')
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS', dedupe=True, exclude_keys=["from","to"])

        assert 'MERGE' in query
        assert len(params) == 2
        assert query == "WITH [[$from_test0, $to_test0, {}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {`gid`:tuple[0]})\nMATCH (toNode {`gid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"

class TestChunkedQuery():
    def test_chunked_nodes_query_splits_batches(self):
        nodes = Nodes(
            records=[{'name': 'Node 1'}, {'name': 'Node 2'}],
            labels=['Label']
        )
        config = Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1)
        result = chunked_query(nodes, config)
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
        result = chunked_query(nodes, config)
        assert result[0][0].startswith('WITH')


class TestSpecificationQueries():
    def test_specification_queries_with_nodes(self):
        nodes1 = Nodes(records=[{'name': 'Node 1'}], labels=['Label'])
        nodes2 = Nodes(records=[{'name': 'Node 2'}], labels=['Label'])
        config = Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1
        )
        result = specification_queries([nodes1, nodes2], config)
        assert len(result) == 2

    def test_specification_queries_empty_list(self):
        result = specification_queries([], Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1
        ))
        assert result == []

    def test_specification_queries_combines_batches(self):
        nodes1 = Nodes(records=[{'name': 'Node 1'}], labels=['Label'])
        nodes2 = Nodes(records=[{'name': 'Node 2'}], labels=['Label'])
        config = Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1
        )
        result = specification_queries([nodes1, nodes2], config)
        assert len(result) == 2
        assert result[0][0].startswith('WITH')
        assert result[1][0].startswith('WITH')

    def test_specification_queries_applies_config(self):
        nodes = Nodes(
            records=[{'name': 'Node 1'}, {'name': 'Node 2'}], 
            labels=['Label']
            )
        config = Neo4jConfig(
            neo4j_uri = "",
            neo4j_password = "",
            max_batch_size=1
        )
        result = specification_queries([nodes], config)
        assert len(result) == 2