import pytest
from neo4j_uploader import upload_node_records_query, upload_relationship_records_query, prop_subquery


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

        expected_query = """ {`name`:{name_}}"""
        expected_params = {'name_':'John'}
        assert query == expected_query
        assert params == expected_params

    def test_prop_subquery_multiple_values(self):
        record = {"name": "John", "age": 30}
        query, params = prop_subquery(record) 

        expected_query = """ {`age`:{age_}, `name`:{name_}}"""
        expected_params = {'name_':'John','age_':30}
        assert query == expected_query
        assert params == expected_params

    def test_prop_subquery_ignores_none_values(self):
        record = {"name": None, "age": 30, "null":"null", "empty":"eMpty"}
        query, params = prop_subquery(record) 

        expected_query = """ {`age`:{age_}}"""
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

        expected_query = """MERGE (`Person0`:`Person` {`_uid`:{_uid_n0}, `first_name`:{first_name_n0}})"""
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

        expected_query = """MERGE (`Person0`:`Person` {`_uid`:{_uid_n0}, `first_name`:{first_name_n0}})\nMERGE (`Person1`:`Person` {`_uid`:{_uid_n1}, `first_name`:{first_name_n1}})"""

        expected_params = {"_uid_n0":"123", "first_name_n0": "John", "_uid_n1":"456", "first_name_n1":"Jane"}

        assert query == expected_query
        assert params == expected_params

    def test_upload_node_records_query_with_different_key(self):
        label = "Person"
        key = "id"
        nodes = [
            {"first_name": "John", "id": "123"},
            {"first_name": "Jane", "id": "456"}  
        ]

        query, params = upload_node_records_query(label, nodes, key) 

        expected_query = """MERGE (`Person0`:`Person` {`first_name`:{first_name_n0}, `id`:{id_n0}})\nMERGE (`Person1`:`Person` {`first_name`:{first_name_n1}, `id`:{id_n1}})"""

        expected_params = {"id_n0":"123", "first_name_n0": "John", "id_n1":"456", "first_name_n1":"Jane"}

        assert query == expected_query
        assert params == expected_params

#     def test_upload_node_records_query_with_whitespace_label(self):
#         label = "My Label"
#         nodes = [
#             {"name": "Node 1", "_uid": "123"}
#         ]
#         expected = """MERGE (`My Label1`:`My Label` {`_uid`:"123"})\nSET `My Label1` += {`_uid`:"123", `name`:"Node 1"}"""
#         query = upload_node_records_query(label, nodes)

#         assert query == expected


#     def test_upload_node_records_query_with_none_null_empty_value(self):
#         label = "My Label"
#         nodes = [
#             {"name": None, "_uid": "123", "name_null": "Null", "name_empty": "eMpty"}
#         ]
#         expected = """MERGE (`My Label1`:`My Label` {`_uid`:"123"})\nSET `My Label1` += {`_uid`:"123"}"""
#         query = upload_node_records_query(label, nodes)

#         assert query == expected


# class TestUploadRelationshipRecordsQuery:

#     def test_empty_relationships(self):
#         type = "KNOWS"
#         relationships = []
        
#         expected_match = None
#         expected_create = None

#         match, create = upload_relationship_records_query(type, relationships)

#         assert match == expected_match
#         assert create == expected_create

#     def test_single_relationship(self):
#         type = "KNOWS"
#         relationships = [
#             {"since": 2022, "_from__uid": "123", "_to__uid": "456"}
#         ]

#         expected_match = """MATCH (`fnKNOWS1` {`_uid`:'123'})\nOPTIONAL MATCH (`tnKNOWS1` {`_uid`:'456'})"""

#         expected_create = """CREATE (`fnKNOWS1`)-[`rKNOWS1`:`KNOWS` {`since`:2022}]->(`tnKNOWS1`)"""

#         match, create = upload_relationship_records_query(type, relationships)

#         assert match == expected_match
#         assert create == expected_create

#     def test_single_relationship_no_props(self):
#         type = "KNOWS"
#         relationships = [
#             {"_from__uid": "123", "_to__uid": "456"}
#         ]

#         expected_match = """MATCH (`fnKNOWS1` {`_uid`:'123'})\nOPTIONAL MATCH (`tnKNOWS1` {`_uid`:'456'})"""

#         expected_create = """CREATE (`fnKNOWS1`)-[`rKNOWS1`:`KNOWS`]->(`tnKNOWS1`)"""

#         match, create = upload_relationship_records_query(type, relationships)

#         assert match == expected_match
#         assert create == expected_create

#     def test_single_relationship_using_node_key(self):
#         type = "KNOWS"
#         relationships = [
#             {"since": 2022, "_from_custom_key": "123", "_to_custom_key": "456"}
#         ]

#         expected_match = """MATCH (`fnKNOWS1` {`custom_key`:'123'})\nOPTIONAL MATCH (`tnKNOWS1` {`custom_key`:'456'})"""

#         expected_create = """CREATE (`fnKNOWS1`)-[`rKNOWS1`:`KNOWS` {`since`:2022}]->(`tnKNOWS1`)"""

#         match, create = upload_relationship_records_query(type, relationships, "custom_key")

#         assert match == expected_match
#         assert create == expected_create

#     def test_single_relationship_using_missing_custom_node_key(self):
#         type = "KNOWS"
#         relationships = [
#             {"since": 2022, "_from__uid": "123", "_to__uid": "456"}
#         ]

#         expected_match = None
#         expected_create = None

#         match, create = upload_relationship_records_query(type, relationships, "custom_key")

#         assert match == expected_match
#         assert create == expected_create

#     def test_multiple_relationships(self):
#         relationships = [
#             {
#                 "_from__uid": "123", 
#                 "_to__uid": "456",
#                 "_uid": "rel1",
#                 "since": 2022
#             },
#             {
#                 "_from__uid": "456",
#                 "_to__uid": "789",
#                 "_uid": "rel2",
#                 "since": 2020  
#             }
#         ]
        
#         expected_match = """MATCH (`fnKNOWS1` {`_uid`:'123'})\nOPTIONAL MATCH (`tnKNOWS1` {`_uid`:'456'})\nMATCH (`fnKNOWS2` {`_uid`:'456'})\nOPTIONAL MATCH (`tnKNOWS2` {`_uid`:'789'})"""
        
#         expected_create = """CREATE (`fnKNOWS1`)-[`rKNOWS1`:`KNOWS` {`_uid`:"rel1", `since`:2022}]->(`tnKNOWS1`)\nCREATE (`fnKNOWS2`)-[`rKNOWS2`:`KNOWS` {`_uid`:"rel2", `since`:2020}]->(`tnKNOWS2`)"""
        
#         match, create = upload_relationship_records_query("KNOWS", relationships)
        
#         assert match == expected_match
#         assert create == expected_create