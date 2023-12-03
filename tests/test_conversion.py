import pytest
from pydantic import ValidationError
from neo4j_uploader.conversion import elements, nodes_query, chunked_nodes_query, all_node_queries
import logging
from neo4j_uploader.logger import ModuleLogger

class TestElements():

    @pytest.mark.usefixtures("caplog")
    def test_elements_dedupe(self, caplog):
        caplog.set_level(logging.DEBUG, logger='__temp_convertor__')

        records = [
            {"name": "John", "age": 30},
            {"name": "John", "age": 30}
        ]
        _, params = elements("test", records)
        assert len(params) == 2

    def test_elements_no_dedupe(self):
        records = [
            {"name": "John", "age": 30},
            {"name": "John", "age": 30}
        ]
        _, params = elements("test", records, dedupe=False)
        assert len(params) == 4  

    def test_elements_exclude_keys(self):
        records = [
            {"name": "John", "age": 30, "id": 1}
        ]
        _, params = elements("test", records, exclude_keys=["id"])
        assert "id" not in params
        assert "id_batch_0" not in params

    def test_elements_sorts_keys(self):
        records = [
            {"name": "John", "age": 30}
        ]
        string, params = elements("test", records)
        assert string == " {`age`:$age_test_0, `name`:$name_test_0}"
        assert "age_test_0" in params.keys()
        assert "name_test_0" in params.keys()

    def test_elements_handles_empty_records(self):
        records = []
        string, params = elements("test", records)
        assert string == None
        assert params == {}

    def test_elements_multiple(self):
        records = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 21}
        ]
        string, params = elements("test", records)
        assert string == " {`age`:$age_test_0, `name`:$name_test_0}, {`age`:$age_test_1, `name`:$name_test_1}"
        assert len(params) == 4  

class TestNodesQuery():
    def test_nodes_query_single_label(self):
        records = [{"name": "John"}] 
        labels = ["Person"]
        query, params = nodes_query("test", records, labels)

        assert query.startswith("WITH")
        assert "MERGE" in query
        assert "SET n += node" in query
        assert "n:`Person`" in query

    def test_nodes_query_multiple_labels(self):
        records = [{"name": "John"}]
        labels = ["Person", "User"]
        query, params = nodes_query("test", records, labels)

        assert "n:`Person`" in query 
        assert "n:`User`" in query

    def test_nodes_query_no_dedupe(self):
        records = [{"name": "John"}]
        labels = ["Person"]
        query, params = nodes_query("test", records, labels, dedupe=False)

        assert "CREATE" in query
        assert "MERGE" not in query

    def test_nodes_query_exclude_keys(self):
        records = [{"name": "John", "id": 1}]
        labels = ["Person"]
        query, params = nodes_query("test", records, labels, exclude_keys=["id"])

        assert "id" not in query

    def test_nodes_query_no_records(self):
        records = []
        labels = ["Person"]
        query, params = nodes_query("test", records, labels)

        assert query == None
        assert params == {}