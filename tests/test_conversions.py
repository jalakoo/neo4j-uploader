import pytest
from pydantic import ValidationError
from neo4j_uploader.models import UploadResult, Neo4jConfig, GraphData, Nodes, Relationships, TargetNode
from neo4j_uploader import batch_upload
from neo4j_uploader._conversions import convert_legacy_node_records, convert_legacy_relationship_records  


class TestConvertLegacyNodeRecords:
    def test_convert_basic(self):
        input_data = {
            "Label1": [
                {   
                    "_uid": "1",
                    "prop1": "val1"
                }, 
                {
                    "_uid":"2",
                    "prop2": "val2"
                }
            ]
        }
        expected = [
            Nodes(
                labels=["Label1"],
                key="_uid",
                records=[
                    {
                        "_uid": "1",
                        "prop1": "val1"
                    },
                    {
                        "_uid": "2",
                        "prop2": "val2"
                    }
                ],
                dedupe = True,
                exclude_keys = []
            )
        ]

        result = convert_legacy_node_records(input_data, True, "_uid")

        assert result == expected

    # TODO: Check for dedupe
    # TODO: Check for alternate node_keys

class TestConvertLegacyRelationshipRecords:
    def test_convert_basic(self):
        input_data = {
            "testRel":[
                {
                    "_from__uid": "1",
                    "_to__uid": "2",
                    "prop1": "val1"
                }
            ]
            
        }
        expected = [
            Relationships(
                type="testRel",
                from_node=TargetNode(
                    node_key="_uid",
                    record_key="_from__uid",
                    node_label = None
                ),
                to_node=TargetNode(
                    node_key="_uid",
                    record_key="_to__uid",
                    node_label = None
                ),
                exclude_keys = [],
                auto_exclude_keys = True,
                dedupe=True,
                records=[
                    {
                        "_from__uid": "1",
                        "_to__uid": "2",
                        "prop1": "val1"
                    },
                ]
            )
        ]

        result = convert_legacy_relationship_records(input_data, True, "_uid")

        assert result == expected

        # TODO: Check for dedupe
        # TODO: Check for alternate node_keys