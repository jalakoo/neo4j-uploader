import pytest
from neo4j_uploader.models import Nodes, Relationships, TargetNode, GraphData

class TestGraphData():
    def test_invalid_config_graphdata(self):
        # Missing neo4j_uri
        instance = {
            "config" : {
                "neo4j_password" : "test_password"
            },
            "nodes":[
            ]
        }

        with pytest.raises(Exception):
            _ = GraphData.model_validate(instance)

        # Missing neo4j_password
        instance = {
            "config" : {
                "neo4j_uri" : "test_uri"
            },
            "nodes":[
            ]
        }

        with pytest.raises(Exception):
            _ = GraphData.model_validate(instance)

        instance = {
            "config" : {
            },
            "nodes":[
            ]
        }

        with pytest.raises(Exception):
            _ = GraphData.model_validate(instance)

    def test_default_config_graphdata(self):
        instance = {
            "config" : {
                "neo4j_uri": "test_uri",
                "neo4j_password" : "test_password"
            },
            "nodes":[
            ]
        }

        gd = GraphData.model_validate(instance)

        assert gd.config.model_dump() == {
            "neo4j_uri":"test_uri",
            "neo4j_user":"neo4j",
            "neo4j_password":"test_password",
            "neo4j_database":"neo4j",
            "max_batch_size":500,
            "overwrite":False,
        }

    def test_invalid_single_node(self):
        instance = {
            "config" : {
                "neo4j_uri": "test_uri",
                "neo4j_password" : "test_password"
            },
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
            "config" : {
                "neo4j_uri": "test_uri",
                "neo4j_password" : "test_password"
            },
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
            "constraints":[],
            "dedupe" : True,
            "records":[
                {
                    "uid":"test"
                }
            ]
        }

    def test_valid_multiple_nodes(self):
        instance = {
            "config" : {
                "neo4j_uri": "test_uri",
                "neo4j_password" : "test_password"
            },
            "nodes":[
                {
                    "labels":["testNode"],
                    "records":[
                        {
                            "uid":"test"
                        }
                    ]
                },
                {
                    "labels":["testNodeB"],
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


    # def test_valid_single_relationship(self):
    #     instance = {
    #         "config" : {
    #             "neo4j_uri": "test_uri",
    #             "neo4j_password" : "test_password"
    #         },
    #         "nodes":[
    #             {
    #                 "labels":["testNode"],
    #                 "key":"abc",
    #                 "records":[
    #                     {
    #                         "abc":"test1"
    #                     },
    #                     {
    #                         "abc":"test2"
    #                     }
    #                 ]
    #             }
    #         ],
    #         "relationships":[
    #             {
    #                 "type":"testRelationship",
    #                 "from_node":{
    #                     "label":"testNode",
    #                     "properties":{
    #                         "abc":"test1"
    #                     }
    #                 },
    #                 "to_node":{
    #                     "label":"testNode",
    #                     "properties":{
    #                         "abc":"test2"
    #                     }
    #                 },
    #                 "properties":{
    #                     "key":"value"
    #                 }
    #             }
    #         ]
    #     }

    #     gd = GraphData.model_validate(instance)

    #     assert len(gd.nodes) == 1
    #     assert len(gd.nodes[0].records) == 2
    #     assert gd.relationships[0].type == "testRelationship"
    #     assert gd.relationships[0].from_node.model_dump() == {
    #         "label":"testNode",
    #         "properties":{"abc":"test1"}
    #     }
    #     assert gd.relationships[0].to_node.model_dump() == {
    #         "label":"testNode",
    #         "properties":{"abc":"test2"}
    #     }
    #     assert len(gd.relationships) == 1
    #     assert gd.relationships[0].model_dump() == {
    #         "dedupe":True,
    #         "key":"value"
    #     }