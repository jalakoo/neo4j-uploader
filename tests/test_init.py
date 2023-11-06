import pytest
from neo4j_uploader import upload_node_records_query, upload_relationship_records_query

class TestUploadNodeRecordsQuery:
    def test_upload_node_records_query_with_no_nodes(self):
        label = "Person"
        nodes = []
        query = upload_node_records_query(label, nodes)
        assert query == ""

    def test_upload_node_records_query_with_one_node(self):
        label = "Person"
        nodes = [
            {"first_name": "John", "_uid": "123"}
        ]
        expected = """MERGE (`Person1`:`Person` {`_uid`:"123"})\nSET `Person1` += {`_uid`:"123", `first_name`:"John"}"""
        query = upload_node_records_query(label, nodes)

        assert query == expected

    def test_upload_node_records_query_with_multiple_nodes(self):
        label = "Person"
        nodes = [
            {"first_name": "John", "_uid": "123"},
            {"first_name": "Jane", "_uid": "456"}
        ]
        expected = """MERGE (`Person1`:`Person` {`_uid`:"123"})\nSET `Person1` += {`_uid`:"123", `first_name`:"John"}\nMERGE (`Person2`:`Person` {`_uid`:"456"})\nSET `Person2` += {`_uid`:"456", `first_name`:"Jane"}"""

        query = upload_node_records_query(label, nodes)

        assert query == expected

    def test_upload_node_records_query_with_different_key(self):
        label = "Person"
        key = "id"
        nodes = [
            {"first_name": "John", "id": "123"},
            {"first_name": "Jane", "id": "456"}  
        ]
        expected = """MERGE (`Person1`:`Person` {`id`:"123"})\nSET `Person1` += {`first_name`:"John", `id`:"123"}\nMERGE (`Person2`:`Person` {`id`:"456"})\nSET `Person2` += {`first_name`:"Jane", `id`:"456"}"""

        query = upload_node_records_query(label, nodes, key)

        assert query == expected

    def test_upload_node_records_query_with_whitespace_label(self):
        label = "My Label"
        nodes = [
            {"name": "Node 1", "_uid": "123"}
        ]
        expected = """MERGE (`My Label1`:`My Label` {`_uid`:"123"})\nSET `My Label1` += {`_uid`:"123", `name`:"Node 1"}"""
        query = upload_node_records_query(label, nodes)

        assert query == expected


class TestUploadRelationshipRecordsQuery:

    def test_empty_relationships(self):
        type = "KNOWS"
        relationships = []
        
        expected_match = ""
        expected_create = ""

        match, create = upload_relationship_records_query(type, relationships)

        assert match == expected_match
        assert create == expected_create

    def test_single_relationship(self):
        type = "KNOWS"
        relationships = [
            {"since": 2022, "_from__uid": "123", "_to__uid": "456"}
        ]

        expected_match = """MATCH (`fnKNOWS1` {`_uid`:'123'}),(`tnKNOWS1` {`_uid`:'456'})"""

        expected_create = """CREATE (`fnKNOWS1`)-[`rKNOWS1`:`KNOWS` {`since`:2022}]->(`tnKNOWS1`)"""

        match, create = upload_relationship_records_query(type, relationships)

        assert match == expected_match
        assert create == expected_create

    def test_single_relationship_no_props(self):
        type = "KNOWS"
        relationships = [
            {"_from__uid": "123", "_to__uid": "456"}
        ]

        expected_match = """MATCH (`fnKNOWS1` {`_uid`:'123'}),(`tnKNOWS1` {`_uid`:'456'})"""

        expected_create = """CREATE (`fnKNOWS1`)-[`rKNOWS1`:`KNOWS`]->(`tnKNOWS1`)"""

        match, create = upload_relationship_records_query(type, relationships)

        assert match == expected_match
        assert create == expected_create

    def test_single_relationship_using_node_key(self):
        type = "KNOWS"
        relationships = [
            {"since": 2022, "_from__uid": "123", "_to__uid": "456"}
        ]

        expected_match = """MATCH (`fnKNOWS1` {`custom_key`:'123'}),(`tnKNOWS1` {`custom_key`:'456'})"""

        expected_create = """CREATE (`fnKNOWS1`)-[`rKNOWS1`:`KNOWS` {`since`:2022}]->(`tnKNOWS1`)"""

        match, create = upload_relationship_records_query(type, relationships, "custom_key")

        assert match == expected_match
        assert create == expected_create


    def test_multiple_relationships(self):
        relationships = [
            {
                "_from__uid": "123", 
                "_to__uid": "456",
                "_uid": "rel1",
                "since": 2022
            },
            {
                "_from__uid": "456",
                "_to__uid": "789",
                "_uid": "rel2",
                "since": 2020  
            }
        ]
        
        expected_match = """MATCH (`fnKNOWS1` {`_uid`:'123'}),(`tnKNOWS1` {`_uid`:'456'})\nMATCH (`fnKNOWS2` {`_uid`:'456'}),(`tnKNOWS2` {`_uid`:'789'})"""
        
        expected_create = """CREATE (`fnKNOWS1`)-[`rKNOWS1`:`KNOWS` {`_uid`:"rel1", `since`:2022}]->(`tnKNOWS1`)\nCREATE (`fnKNOWS2`)-[`rKNOWS2`:`KNOWS` {`_uid`:"rel2", `since`:2020}]->(`tnKNOWS2`)"""
        
        match, create = upload_relationship_records_query("KNOWS", relationships)
        
        assert match == expected_match
        assert create == expected_create