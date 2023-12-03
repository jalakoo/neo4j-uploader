import pytest
from pydantic import ValidationError
from neo4j_uploader.models import UploadResult, Neo4jConfig, GraphData
from neo4j_uploader import batch_upload, final_graph_data

class TestFinalGraphData():
    def test_final_graph_data_no_config(self):
        data = GraphData.model_validate({})
        result = final_graph_data(data)
        assert result is None

    def test_final_graph_data_external_config(self):
        data = GraphData.model_validate({})
        config = Neo4jConfig.model_validate({
            "neo4j_uri": "bolt://localhost:7687",
            "neo4j_password":"test"
            }) 
        result = final_graph_data(data, config)
        assert result.config.model_dump() == {
            "neo4j_uri": "bolt://localhost:7687",
            "neo4j_user":"neo4j",
            "neo4j_password":"test",
            "neo4j_database":"neo4j",
            "overwrite": False,
            "max_batch_size": 500
            }

    def test_final_graph_data_embedded_config(self):
        data = GraphData.model_validate({
            "config": {
                "neo4j_uri": "bolt://localhost:7687",
                "neo4j_password":"test"
                }
            })
        result = final_graph_data(data)
        assert result.config.model_dump() == {
            "neo4j_uri": "bolt://localhost:7687",
            "neo4j_user":"neo4j",
            "neo4j_password":"test",
            "neo4j_database":"neo4j",
            "overwrite": False,
            "max_batch_size": 500
            }

    def test_final_graph_data_external_override(self):
        data = GraphData.model_validate({"config": {
            "neo4j_uri": "bolt://localhost:1234",
            "neo4j_password":"test"
            }})
        config = Neo4jConfig.model_validate({
            "neo4j_uri": "bolt://remote:7687",
            "neo4j_password":"testOverride"
            })
        result = final_graph_data(data, config)
        assert result.config.model_dump() == {
            "neo4j_uri": "bolt://remote:7687",
            "neo4j_user":"neo4j",
            "neo4j_password":"testOverride",
            "neo4j_database":"neo4j",
            "overwrite": False,
            "max_batch_size": 500
            }

class TestBatchUpload():
    def test_missing_config_arg(self):
        data = GraphData.model_validate({})
        result = batch_upload(data)
        assert result.was_successful == False
        assert result.error_message is not None

    def test_invalid_config_arg(self):
        data = GraphData.model_validate({})

        with pytest.raises(ValidationError):
            config = Neo4jConfig.model_validate({})
            _ = batch_upload(data, config=config)
    
    def test_invalid_config_embedded(self):

        with pytest.raises(ValidationError):
            data = GraphData.model_validate({
                "config":{
                }
            })
            _ = batch_upload(data)