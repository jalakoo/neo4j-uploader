
from neo4j_uploader.models import Neo4jConfig, Nodes, Relationships, TargetNode

from neo4j_uploader._queries import relationship_elements, relationships_query

from neo4j_uploader._queries_relationships import relationship_from_to_dict, new_relationships_from_relationships_with_lists


class TestNewRelationshipsFromRelationshipLists():
    # Test case for when the from_node and to_node values are not lists
    def test_no_list_values(self):

        from_node = TargetNode(node_key='from_key', node_label='From', record_key='from_record')
        to_node = TargetNode(node_key='to_key', node_label='To', record_key='to_record')
        records = [{'from_record': 1, 'to_record': 2}]

        source_relationships = Relationships(
            type = "Test",
            from_node=from_node, 
            to_node=to_node, 
            records=records)

        result = new_relationships_from_relationships_with_lists(source_relationships)

        assert len(result) == 0

    # Test case for when the to_node value is a list of dictionaries
    def test_to_node_list_dicts(self):
        from_node = TargetNode(node_key='from_key', node_label='From', record_key='from_record')
        to_node = TargetNode(node_key='to_key', node_label='To', record_key='to_record.id')
        records = [{'from_record': 4, 'to_record': [{'id': 5}, {'id': 6}]}]
        source_relationships = Relationships(
            type="TEST",
            from_node=from_node, 
            to_node=to_node, 
            records=records)

        result = new_relationships_from_relationships_with_lists(source_relationships)

        assert len(result) == 1
        first_result = result[0]
        result_records = first_result.records
        assert len(result_records) == 2
        assert first_result.from_node == from_node
        assert first_result.to_node.record_key == "id"


class TestRelationshipsQuery():
    def test_relationships_query_basic(self):
        records = [{'from': 'a', 'to': 'b'}]
        from_node = TargetNode(record_key='from', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS')
        
        assert 'MERGE' in query

        assert params['from_test0'] == 'a'
        assert params['to_test0'] == 'b'
        assert query == "WITH [[$from_test0, $to_test0, {`from`:$from_test0, `to`:$to_test0}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode:`testLabel` {`gid`:tuple[0]})\nMATCH (toNode:`testLabel` {`gid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"

    def test_relationships_query_multiple(self):
        records = [{'from': 'a', 'to': 'b'}, {'from': 'c', 'to': 'd'}]
        from_node = TargetNode(record_key='from', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS')
        
        assert len(params) == 4
        assert 'UNWIND' in query

    def test_relationships_query_empty(self):
        records = []
        from_node = TargetNode(record_key='from', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS')
        
        assert query is None
        assert params == {}
        
    def test_relationships_query_no_dedupe(self):
        records = [{'from': 'a', 'to': 'b'}, {'from': 'a', 'to': 'b'}]
        from_node = TargetNode(record_key='from', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS', dedupe=False)

        assert 'CREATE' in query
        assert len(params) == 4
        assert query == "WITH [[$from_test0, $to_test0, {`from`:$from_test0, `to`:$to_test0}], [$from_test1, $to_test1, {`from`:$from_test1, `to`:$to_test1}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode:`testLabel` {`gid`:tuple[0]})\nMATCH (toNode:`testLabel` {`gid`:tuple[1]})\nCREATE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"

    def test_relationships_query_exclude(self):
        records = [{'from': 'a', 'to': 'b'}, {'from': 'a', 'to': 'b'}]
        from_node = TargetNode(record_key='from', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS', dedupe=True, exclude_keys=["from","to"])

        assert 'MERGE' in query
        assert len(params) == 2
        assert query == "WITH [[$from_test0, $to_test0, {}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode:`testLabel` {`gid`:tuple[0]})\nMATCH (toNode:`testLabel` {`gid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"

    # TODO: Add check for optional node labels in Target Nodes
    def test_relationship_no_from_target_node_label(self):
        records = [{'from': 'a', 'to': 'b'}]
        from_node = TargetNode(record_key='from', node_key='gid')
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS')
        
        assert 'MERGE' in query

        assert params['from_test0'] == 'a'
        assert params['to_test0'] == 'b'
        assert query == "WITH [[$from_test0, $to_test0, {`from`:$from_test0, `to`:$to_test0}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {`gid`:tuple[0]})\nMATCH (toNode:`testLabel` {`gid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"

    def test_relationship_no_to_target_node_label(self):
        records = [{'from': 'a', 'to': 'b'}]
        from_node = TargetNode(record_key='from', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid')
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS')
        
        assert 'MERGE' in query

        assert params['from_test0'] == 'a'
        assert params['to_test0'] == 'b'
        assert query == "WITH [[$from_test0, $to_test0, {`from`:$from_test0, `to`:$to_test0}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode:`testLabel` {`gid`:tuple[0]})\nMATCH (toNode {`gid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"

class TestRelationshipElements():
    def test_relationship_elements_basic(self):
        records = [{'from':'abc', 'to':'cde', 'since': 2022}]
        from_node = TargetNode(record_key='from', node_key='uid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='uid', node_label="testLabel")
        
        result_str, result_params = relationship_elements('test', records, from_node, to_node, exclude_keys=["from", "to"])
        
        assert result_str == "[$from_test0, $to_test0, {`since`:$since_test0}]"
        assert result_params == {
            'from_test0': 'abc',
            'to_test0': 'cde',
            'since_test0': 2022
        }

    def test_relationship_elements_no_props(self):
        records = [{'from':'abc', 'to':'cde'}]
        from_node = TargetNode(record_key='from', node_key='uid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='uid', node_label="testLabel")
        
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
        from_node = TargetNode(record_key='from', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")

        result_str, result_params = relationship_elements('test', records, from_node, to_node, exclude_keys=["from","to"])

        assert result_str == "[$from_test0, $to_test0, {`since`:$since_test0}], [$from_test1, $to_test1, {`since`:$since_test1}]"
        assert len(result_params) == 6

    def test_relationship_elements_no_exclusion(self):
        records = [
            {'from':'abc','to':'cde','since': 2022}
        ]
        from_node = TargetNode(record_key='from', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")

        result_str, result_params = relationship_elements('test', records, from_node, to_node)

        assert result_str == "[$from_test0, $to_test0, {`from`:$from_test0, `since`:$since_test0, `to`:$to_test0}]" 
        assert len(result_params) == 3

    def test_relationship_elements_dedupe(self):
        records = [
            {'from':'abc','to':'cde','since': 2022},
            {'from':'abc','to':'cde','since': 2022},
        ]
        from_node = TargetNode(record_key='from', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")

        result_str, result_params = relationship_elements('test', records, from_node, to_node, dedupe=True)

        assert result_str == "[$from_test0, $to_test0, {`from`:$from_test0, `since`:$since_test0, `to`:$to_test0}]" 
        assert len(result_params) == 3

    def test_relationship_elements_empty(self):
        records = []
        from_node = TargetNode(record_key='from', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")

        result_str, result_params = relationship_elements('test', records, from_node, to_node)

        assert result_str is None
        assert result_params == {}

class TestNestedRelationshipsQuery():
    def test_nested_relationships_query_basic(self):
        records = [{'from': {'nested_key':'a'}, 'to': 'b'}]
        from_node = TargetNode(record_key='from.nested_key', node_key='gid', node_label="testLabel")
        to_node = TargetNode(record_key='to', node_key='gid', node_label="testLabel")
        
        query, params = relationships_query('test', records, from_node, to_node, 'KNOWS')
        
        assert 'MERGE' in query

        print(f'params: {params}')
        assert params['from.nested_key_test0'] == 'a'
        assert params['to_test0'] == 'b'
        assert query == "WITH [[$from.nested_key_test0, $to_test0, {`from`:$from_test0, `to`:$to_test0}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode:`testLabel` {`gid`:tuple[0]})\nMATCH (toNode:`testLabel` {`gid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"

class TestRelationshipDynamicFromDict():
    def test_flat_node_keys(self):
        record = {
            "from_uid": "abc",
            "to_gid": "def",
            "name": "John Doe"
        }
        result = relationship_from_to_dict(
            from_param_key="from_uid",
            to_param_key="to_gid",
            from_node_key="from_uid",
            to_node_key="to_gid",
            record=record
        )
        assert result == {
            "from_uid": "abc",
            "to_gid": "def"
        }

    def test_nested_node_keys(self):
        record = {
            "person": {
                "uid": "abc",
                "name": "John Doe"
            },
            "dog": {
                "gid": "def",
                "name": "Buddy"
            }
        }
        result = relationship_from_to_dict(
            from_param_key="from_uid",
            to_param_key="to_gid",
            from_node_key="person.uid",
            to_node_key="dog.gid",
            record=record
        )
        assert result == {
            "from_uid": "abc",
            "to_gid": "def"
        }
