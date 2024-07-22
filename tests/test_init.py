import pytest
from pydantic import ValidationError
from neo4j_uploader.models import UploadResult, Neo4jConfig, GraphData
from neo4j_uploader import batch_upload


class TestBatchUpload:

    def test_invalid_config_arg(self):
        data = GraphData.model_validate({})

        with pytest.raises(ValidationError):
            config = Neo4jConfig.model_validate({})
            _ = batch_upload(data, config=config)


class TestBatchUploadGenerator:

    # Raises InvalidCredentialsError if config conversion fails
    def test_raises_invalid_credentials_error_on_config_conversion_failure(
        self, mocker
    ):
        from neo4j_uploader.errors import InvalidCredentialsError
        from neo4j_uploader import batch_upload_generator

        invalid_config_dict = {
            "neo4j_uri": "bolt://localhost:7687",
            # Missing password field
            "neo4j_user": "neo4j",
            "neo4j_database": "neo4j",
            "max_batch_size": 500,
            "overwrite": False,
        }
        data_dict = {"nodes": [], "relationships": []}

        with pytest.raises(InvalidCredentialsError):
            generator = batch_upload_generator(invalid_config_dict, data_dict)
            next(generator)
