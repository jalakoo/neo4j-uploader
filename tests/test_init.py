import pytest
from pydantic import ValidationError
from neo4j_uploader.errors import InvalidCredentialsError
from neo4j_uploader.models import UploadResult, Neo4jConfig, GraphData
from neo4j_uploader import batch_upload
import pytest
import os
import importlib.resources as resources

@pytest.fixture
def sample_graphdata():
    data = {
        "nodes": [
            {
                "labels":["Person"],
                "key":"uid",
                "records":[
                    {
                        "uid":"abc",
                        "name": "John Wick"
                    },
                    {
                        "uid":"bcd",
                        "name":"Cane"
                    }
                ]
            },
            {
                "labels":["Dog"],
                "key": "gid",
                "records":[
                    {
                        "gid":"abc",
                        "name": "Daisy"
                    }
                ]
            }
        ],
        "relationships": [
            {
                "type":"loves",
                "from_node": {
                    "record_key":"_from_uid",
                    "node_key":"uid",
                    "node_label":"Person"
                },
                "to_node": {
                    "record_key":"_to_gid",
                    "node_key":"gid",
                    "node_label": "Dog"
                },
                "exclude_keys":["_from_uid", "_to_gid"],
                "records":[
                    {
                        "_from_uid":"abc",
                        "_to_gid":"abc"
                    }
                ]
            }
        ]
    }
    
    return GraphData(**data)

class TestBatchUpload():
    def test_missing_config_arg(self, sample_graphdata):
        with pytest.raises(InvalidCredentialsError):
            _ = batch_upload(
            data=sample_graphdata,
            config=None)

    def test_invalid_config_arg(self, sample_graphdata):
        with pytest.raises(ValidationError):
            config = Neo4jConfig.model_validate({})
            _ = batch_upload(sample_graphdata, config=config)

