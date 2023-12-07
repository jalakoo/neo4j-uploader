import pytest
from pydantic import ValidationError
from neo4j_uploader.models import UploadResult, Neo4jConfig, GraphData
from neo4j_uploader import batch_upload


class TestBatchUpload():
    def test_missing_config_arg(self):
        data = GraphData.model_validate({})
        result = batch_upload(
            data = data,
            config = None)
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