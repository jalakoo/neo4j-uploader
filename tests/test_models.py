import pytest
from neo4j_uploader.models import Nodes, Relationships, TargetNode, GraphData, Neo4jConfig

class TestNeo4jConfig():
    def test_invalid_config(self):
        # Missing neo4j_uri
        instance = {
            "neo4j_user" : "test_user",
            "neo4j_password" : "test_password"
        }

        with pytest.raises(Exception):
            _ = Neo4jConfig.model_validate(instance)

        # Missing neo4j_password
        instance = {
            "neo4j_user" : "test_user",
            "neo4j_uri" : "test_uri"
        }

        with pytest.raises(Exception):
            _ = Neo4jConfig.model_validate(instance)

        # NOTE: neo4j_user has a default

    def test_default_config_user(self):
        instance = {
            "neo4j_uri": "test_uri",
            "neo4j_password" : "test_password"
        }

        config = Neo4jConfig.model_validate(instance)

        assert config.model_dump() == {
            "neo4j_uri":"test_uri",
            "neo4j_user":"neo4j",
            "neo4j_password":"test_password",
            "neo4j_database":"neo4j",
            "max_batch_size":500,
            "overwrite":False,
        }

class TestGraphData():

    def test_invalid_single_node(self):
        instance = {
            "nodes":[
                {
                    "invalid":"test"
                }
            ]
        }

        with pytest.raises(Exception):
            _ = GraphData.model_validate(instance)

    def test_valid_single_node(self):
        instance = {
            "nodes":[
                {
                    "labels":["testNode"],
                    "key":"uid",
                    "records":[
                        {
                            "uid":"test"
                        }
                    ]
                }
            ]
        }

        gd = GraphData.model_validate(instance)

        assert gd.nodes[0].model_dump() == {
            "labels":["testNode"],
            "dedupe" : True,
            "key":"uid",
            "exclude_keys": [],
            "records":[
                {
                    "uid":"test"
                }
            ]
        }

    def test_valid_multiple_nodes(self):
        instance = {
            "nodes":[
                {
                    "labels":["testNode"],
                    "key":"uid",
                    "records":[
                        {
                            "uid":"test"
                        }
                    ]
                },
                {
                    "labels":["testNodeB"],
                    "key":"abc",
                    "records":[
                        {
                            "abc":"test1"
                        },
                        {
                            "abc":"test2"
                        }
                    ]
                }
            ]
        }

        gd = GraphData.model_validate(instance)

        assert len(gd.nodes) == 2
        assert len(gd.nodes[0].records) == 1
        assert len(gd.nodes[1].records) == 2


    def test_valid_single_relationship(self):
        instance = {
            "nodes":[
                {
                    "labels":["testNode"],
                    "key":"abc",
                    "records":[
                        {
                            "abc":"test1"
                        },
                        {
                            "abc":"test2"
                        }
                    ]
                }
            ],
            "relationships":[
                {
                    "type":"testRelationship",
                    "from_node":{
                        "node_label":"testNode",
                        "node_key":"abc",
                        "record_key":"_from"
                    },
                    "to_node":{
                        "node_label":"testNode",
                        "node_key":"abc",
                        "record_key":"_to"
                    },
                    "records":[
                        {
                            "_from":"test1",
                            "_to":"test2",
                            "test_key":"test_value"
                        }
                    ]
                }
            ]
        }

        gd = GraphData.model_validate(instance)

        assert len(gd.nodes) == 1
        assert len(gd.nodes[0].records) == 2
        assert gd.relationships[0].type == "testRelationship"
        assert gd.relationships[0].from_node.model_dump() == {
            "node_label":"testNode",
            "node_key":"abc",
            "record_key":"_from",
        }
        assert gd.relationships[0].to_node.model_dump() == {
            "node_label":"testNode",
            "node_key":"abc",
            "record_key":"_to",
        }
        assert len(gd.relationships) == 1
        assert gd.relationships[0].model_dump() == {
            "type":"testRelationship",
            "auto_exclude_keys": True,
            "dedupe":True,
            "exclude_keys": [],
            "from_node":{
                "node_label":"testNode",
                "node_key":"abc",
                "record_key":"_from"
            },
            "to_node":{
                "node_label":"testNode",
                "node_key":"abc",
                "record_key":"_to"
            },
            "records":[
                {
                    "_from":"test1",
                    "_to":"test2",
                    "test_key":"test_value"
                }
            ]
        }