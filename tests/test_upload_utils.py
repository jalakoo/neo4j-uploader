import pytest
from neo4j_uploader._upload_utils import upload_node_records_query, upload_relationship_records_query, prop_subquery, with_relationship_elements

from neo4j_uploader._upload_utils import from_key, to_key

class TestFromKey:
    def test_from_key_explicit(self):
        record = {"_from_uid": "123"}
        node_key = "uid"
        
        result = from_key(record, node_key)
        
        assert result == "_from_uid"

    def test_from_key_implicit(self):
        record = {"_from_aid": "123"} 
        node_key = "uid"
        
        result = from_key(record, node_key)
         
        assert result == "_from_aid"

    def test_from_key_no_match(self):
        record = {"name": "John"}
        node_key = "uid"
        
        result = from_key(record, node_key)
        
        assert result is None

    def test_from_key_multiple_matches(self):
        record = {"_from_uid": "123", "_from_aid": "456"}
        node_key = "uid"
        
        result = from_key(record, node_key)

        assert result == "_from_uid"

    def test_from_key_multiple_matches_no_node_key(self):
        record = {"_from_xyz": "123", "_from_aid": "456"}
        node_key = "uid"
        
        result = from_key(record, node_key)

        assert result == "_from_aid"


class TestToKey:
    def test_to_key_explicit(self):
        record = {"_to_uid": "123"}
        node_key = "uid"
        
        result = to_key(record, node_key)
        
        assert result == "_to_uid"

    def test_to_key_implicit(self):
        record = {"_to_aid": "123"} 
        node_key = "uid"
        
        result = to_key(record, node_key)
         
        assert result == "_to_aid"

    def test_to_key_no_match(self):
        record = {"name": "John"}
        node_key = "uid"
        
        result = to_key(record, node_key)
        
        assert result is None

    def test_to_key_multiple_matches(self):
        record = {"_to_uid": "123", "_to_aid": "456"}
        node_key = "uid"
        
        result = to_key(record, node_key)

        assert result == "_to_uid"

    def test_to_key_multiple_matches_no_node_key(self):
        record = {"_to_xyz": "123", "_to_aid": "456"}
        node_key = "uid"
        
        result = to_key(record, node_key)

        assert result == "_to_aid"

class TestPropSubquery:

    def test_prop_subquery_none(self):
        record = None
        query, params = prop_subquery(record) 
        
        expected_query = ""
        expected_params = {}

        assert query == expected_query
        assert params == expected_params

    def test_prop_subquery_empty(self):
        record = {}
        query, params = prop_subquery(record) 
        
        expected_query = ""
        expected_params = {}

        assert query == expected_query
        assert params == expected_params

    def test_prop_subquery_single_value(self):
        record = {"name": "John"}
        query, params = prop_subquery(record) 

        expected_query = """ {`name`:$name_}"""
        expected_params = {'name_':'John'}
        assert query == expected_query
        assert params == expected_params

    def test_prop_subquery_multiple_values(self):
        record = {"name": "John", "age": 30}
        query, params = prop_subquery(record) 

        expected_query = """ {`age`:$age_, `name`:$name_}"""
        expected_params = {'name_':'John','age_':30}
        assert query == expected_query
        assert params == expected_params

    def test_prop_subquery_ignores_none_values(self):
        record = {"name": None, "age": 30, "null":"null", "empty":"eMpty"}
        query, params = prop_subquery(record) 

        expected_query = """ {`age`:$age_}"""
        expected_params = {'age_':30}
        assert query == expected_query
        assert params == expected_params

class TestUploadNodeRecordsQuery:

    def test_upload_node_records_query_with_no_nodes(self):
        label = "Person"
        nodes = []

        query, params = upload_node_records_query(label, nodes) 

        expected_query = """"""
        expected_params = {}
        assert query == expected_query
        assert params == expected_params

    def test_upload_node_records_query_with_one_node(self):
        label = "Person"
        nodes = [
            {"first_name": "John", "_uid": "123"}
        ]
        query, params = upload_node_records_query(label, nodes) 

        expected_query = """WITH [ {`_uid`:$_uid_n0, `first_name`:$first_name_n0}] AS node_data\nUNWIND node_data AS node\nMERGE (n:`Person` {`_uid`:node.`_uid`})\nSET n += node"""

        expected_params = {"_uid_n0":"123", "first_name_n0": "John"}

        assert query == expected_query
        assert params == expected_params

    def test_upload_node_records_query_with_multiple_nodes(self):
        label = "Person"
        nodes = [
            {"first_name": "John", "_uid": "123"},
            {"first_name": "Jane", "_uid": "456"}
        ]

        query, params = upload_node_records_query(label, nodes) 

        expected_query = """WITH [ {`_uid`:$_uid_n0, `first_name`:$first_name_n0}, {`_uid`:$_uid_n1, `first_name`:$first_name_n1}] AS node_data\nUNWIND node_data AS node\nMERGE (n:`Person` {`_uid`:node.`_uid`})\nSET n += node"""


        expected_params = {"_uid_n0":"123", "first_name_n0": "John", "_uid_n1":"456", "first_name_n1":"Jane"}

        assert query == expected_query
        assert params == expected_params

    def test_upload_node_records_query_with_different_key(self):
        label = "Person"
        key = "aid"
        nodes = [
            {"first_name": "John", "aid": "123"},
            {"first_name": "Jane", "aid": "456"}  
        ]

        query, params = upload_node_records_query(label, nodes, key) 

        expected_query = """WITH [ {`aid`:$aid_n0, `first_name`:$first_name_n0}, {`aid`:$aid_n1, `first_name`:$first_name_n1}] AS node_data\nUNWIND node_data AS node\nMERGE (n:`Person` {`aid`:node.`aid`})\nSET n += node"""

        expected_params = {"aid_n0":"123", "first_name_n0": "John", "aid_n1":"456", "first_name_n1":"Jane"}

        assert query == expected_query
        assert params == expected_params

    def test_upload_node_records_query_with_whitespace_in_label(self):
        label = "Funny Person"
        key = "aid"
        nodes = [
            {"first_name": "John", "aid": "123"},
            {"first_name": "Jane", "aid": "456"}  
        ]

        query, params = upload_node_records_query(label, nodes, key) 

        expected_query = """WITH [ {`aid`:$aid_n0, `first_name`:$first_name_n0}, {`aid`:$aid_n1, `first_name`:$first_name_n1}] AS node_data\nUNWIND node_data AS node\nMERGE (n:`Funny Person` {`aid`:node.`aid`})\nSET n += node"""

        expected_params = {"aid_n0":"123", "first_name_n0": "John", "aid_n1":"456", "first_name_n1":"Jane"}

        assert query == expected_query
        assert params == expected_params


    def test_upload_node_records_query_dedupe_true(self):
        label = "Funny Person"
        key = "aid"
        dedupe = True
        nodes = [
            {"first_name": "John", "aid": "123"},
            {"first_name": "John", "aid": "123"}  
        ]

        query, params = upload_node_records_query(label, nodes, key, dedupe=dedupe) 

        expected_query = """WITH [ {`aid`:$aid_n0, `first_name`:$first_name_n0}] AS node_data\nUNWIND node_data AS node\nMERGE (n:`Funny Person` {`aid`:node.`aid`})\nSET n += node"""

        expected_params = {"aid_n0":"123", "first_name_n0": "John"}

        assert query == expected_query
        assert params == expected_params

    def test_upload_node_records_query_dedupe_false(self):
        label = "Funny Person"
        key = "id"
        dedupe = False
        nodes = [
            {"first_name": "John", "id": "123"},
            {"first_name": "John", "id": "123"}  
        ]

        query, params = upload_node_records_query(label, nodes, key, dedupe=dedupe) 

        expected_query = """WITH [ {`first_name`:$first_name_n0, `id`:$id_n0}, {`first_name`:$first_name_n1, `id`:$id_n1}] AS node_data\nUNWIND node_data AS node\nCREATE (n:`Funny Person`)\nSET n += node"""

        expected_params = {"first_name_n0": "John", "id_n0":"123", "first_name_n1":"John", "id_n1":"123"}

        assert query == expected_query
        assert params == expected_params

class TestWithRelationshipElements:
    def test_with_relationship_elements_empty(self):
        relationships = []
        type = "TEST"
        node_key = "_uid"
        
        expected_query = []
        expected_params = {}
        
        query, params = with_relationship_elements(type, relationships, node_key)

        assert query == expected_query
        assert params == expected_params

    def test_with_relationship_elements_single(self):
        relationships = [
            {"_from__uid": "123", "_to__uid": "456", "likes": True}
        ]
        type = "TEST"
        nodes_key = "_uid"
        
        expected_list = ["[$_from__uid_r0,$_to__uid_r0, {`likes`:$likes_r0}]"]

        expected_params = {"_from__uid_r0":"123","_to__uid_r0":"456","likes_r0": True}
        
        list, params = with_relationship_elements(type, relationships, nodes_key)

        assert list == expected_list 
        assert params == expected_params

    def test_with_relationship_elements_multiple(self):
        relationships = [
            {"_from__uid": "123", "_to__uid": "456", "likes": True},
            {"_from__uid": "789", "_to__uid": "101", "likes": False}
        ]
        type = "TEST"
        nodes_key = "_uid"

        expected_list = ["[$_from__uid_r0,$_to__uid_r0, {`likes`:$likes_r0}]","[$_from__uid_r1,$_to__uid_r1, {`likes`:$likes_r1}]"]

        expected_params = {"_from__uid_r0":"123","_to__uid_r0":"456","likes_r0": True,"_from__uid_r1":"789","_to__uid_r1":"101","likes_r1": False}
        
        list, params = with_relationship_elements(type, relationships, nodes_key)

        assert list == expected_list
        assert params == expected_params

    def test_with_relationship_elements_multiple_alt_nodes_key(self):
        relationships = [
            {"_from_cid": "123", "_to_cid": "456", "likes": True},
            {"_from_cid": "789", "_to_cid": "101", "likes": False}
        ]
        type = "TEST"
        nodes_key = "cid"

        expected_list = ["[$_from_cid_r0,$_to_cid_r0, {`likes`:$likes_r0}]","[$_from_cid_r1,$_to_cid_r1, {`likes`:$likes_r1}]"]

        expected_params = {"_from_cid_r0":"123","_to_cid_r0":"456","likes_r0": True,"_from_cid_r1":"789","_to_cid_r1":"101","likes_r1": False}
        
        list, params = with_relationship_elements(type, relationships, nodes_key)

        assert list == expected_list
        assert params == expected_params

class TestUploadRelationshipRecordsQuery:

    def test_empty_relationships(self):
        type = "KNOWS"
        nodes_key = ""
        relationships = []
        
        expected_query = ""
        expected_params = {}

        query, params = upload_relationship_records_query(type, relationships, nodes_key)

        assert query == expected_query
        assert params == expected_params

    def test_single_relationship(self):
        type = "KNOWS"
        nodes_key = "_uid"
        relationships = [
            {"since": 2022, "_from__uid": "123", "_to__uid": "456"}
        ]

        expected_query = """WITH [[$_from__uid_r0,$_to__uid_r0, {`since`:$since_r0}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {`_uid`:tuple[0]})\nMATCH (toNode {`_uid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"""

        expected_params = {"_from__uid_r0":"123","_to__uid_r0":"456",
        "since_r0":2022}

        query, params = upload_relationship_records_query(type, relationships, nodes_key)

        assert query == expected_query
        assert params == expected_params

    def test_single_relationship_dedupe_false(self):
        type = "KNOWS"
        nodes_key = "_uid"
        dedupe = False
        relationships = [
            {"since": 2022, "_from__uid": "123", "_to__uid": "456"}
        ]

        expected_query = """WITH [[$_from__uid_r0,$_to__uid_r0, {`since`:$since_r0}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {`_uid`:tuple[0]})\nMATCH (toNode {`_uid`:tuple[1]})\nCREATE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"""

        expected_params = {"_from__uid_r0":"123","_to__uid_r0":"456",
        "since_r0":2022}

        query, params = upload_relationship_records_query(type, relationships, nodes_key, dedupe=dedupe)

        assert query == expected_query
        assert params == expected_params

    def test_single_relationship_no_props(self):
        type = "KNOWS"
        nodes_key = "_uid"
        relationships = [
            {"_from__uid": "123", "_to__uid": "456"}
        ]

        expected_query = """WITH [[$_from__uid_r0,$_to__uid_r0, {}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {`_uid`:tuple[0]})\nMATCH (toNode {`_uid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"""

        expected_params = {"_from__uid_r0":"123","_to__uid_r0":"456"}

        query, params = upload_relationship_records_query(type, relationships, nodes_key)

        assert query == expected_query
        assert params == expected_params

    def test_multiple_relationship_no_props(self):
        type = "KNOWS"
        nodes_key = "_uid"
        relationships = [
            {"_from__uid": "123", "_to__uid": "456"},
            {"_from__uid": "1234", "_to__uid": "4567"}
        ]

        expected_query = """WITH [[$_from__uid_r0,$_to__uid_r0, {}],[$_from__uid_r1,$_to__uid_r1, {}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {`_uid`:tuple[0]})\nMATCH (toNode {`_uid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"""

        expected_params = {
            "_from__uid_r0":"123",
            "_to__uid_r0":"456",
            "_from__uid_r1":"1234",
            "_to__uid_r1":"4567"}

        query, params = upload_relationship_records_query(type, relationships, nodes_key)

        assert query == expected_query
        assert params == expected_params

    def test_multiple_relationship_no_props_dedupe(self):
        type = "KNOWS"
        nodes_key = "_uid"
        relationships = [
            {"_from__uid": "123", "_to__uid": "456"},
            {"_from__uid": "123", "_to__uid": "456"}
        ]

        expected_query = """WITH [[$_from__uid_r0,$_to__uid_r0, {}]] AS from_to_data\nUNWIND from_to_data AS tuple\nMATCH (fromNode {`_uid`:tuple[0]})\nMATCH (toNode {`_uid`:tuple[1]})\nMERGE (fromNode)-[r:`KNOWS`]->(toNode)\nSET r += tuple[2]"""

        expected_params = {"_from__uid_r0":"123","_to__uid_r0":"456"}

        query, params = upload_relationship_records_query(
            type, 
            relationships, 
            nodes_key,
            dedupe=True)

        assert query == expected_query
        assert params == expected_params