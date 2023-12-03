import pytest
from neo4j_uploader.models import UploadResult
from neo4j_uploader import batch_upload

class TestBatchUpload():
    def test_invalid_config_arg(self):
        data = {
        }
        result = batch_upload(data)
        assert result.was_successful == False
        assert result.error_message is not None