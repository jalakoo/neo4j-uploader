import pytest
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from neo4j_uploader.schemas import schema_a, SchemaType, upload_schema

class TestUploadSchema:
    def test_invalid_schema(self):
        instance = {
        } 
        schema = upload_schema(instance)
        assert schema == SchemaType.UNKNOWN     

    def test_schema_a(self):
        instance = {
            "nodes":[],
            "relationships":{}
        } 
        schema = upload_schema(instance)
        assert schema == SchemaType.A      

    # def test_schema_b(self):
    #     instance = {
    #         "nodes":[],
    #         "relationships":[]
    #     } 
    #     schema = upload_schema(instance)
    #     assert schema == SchemaType.B 

    #     instance = {
    #         "nodes":[
    #             {
    #                 "key":"_uid",
    #                 "labels":["Person"],
    #                 "records":[
    #                     {
    #                         "_uid":"test"
    #                     }
    #                 ]
    #             }
    #         ],
    #         "relationships":[
    #             {
    #                 "from":{
    #                     "label":"Person",
    #                     "properties":{}
    #                 },
    #                 "to":{
    #                     "label":"Person",
    #                     "properties":{}
    #                 },
    #                 "records":[
    #                     {

    #                     }
    #                 ]
    #             }
    #         ]
    #     } 
    #     schema = upload_schema(instance)
    #     assert schema == SchemaType.B 

    #     instance = {
    #         "max_batch_size":10,
    #         "nodes":[],
    #         "relationships":[]
    #     } 
    #     schema = upload_schema(instance)
    #     assert schema == SchemaType.B

class TestSchemaA:

    def test_schema_a_empty(self):
        instance = {
        }

        with pytest.raises(ValidationError):
            validate(instance, schema_a)

    def test_schema_a_missing_keys(self):
        # Only nodes required
        instance = {
            "relationships":[]
        }

        with pytest.raises(ValidationError):
            validate(instance, schema_a)

    def test_schema_a_invalid_key_values(self):
        instance = {
            "nodes":{},
            "relationships":[]
        }

        with pytest.raises(ValidationError):
            validate(instance, schema_a)

        instance = {
            "nodes":{},
            "relationships":{}
        }

        with pytest.raises(ValidationError):
            validate(instance, schema_a)

        instance = {
            "nodes":[],
            "relationships":[]
        }

        with pytest.raises(ValidationError):
            validate(instance, schema_a)

    def test_schema_a_valid_empty_keys(self):
        instance = {
            "nodes":[],
            "relationships":{}
        }

        # JSONSchema returns None if valid
        assert validate(instance, schema_a) == None

    def test_invalid_node_records(self):
        instance = {
            "nodes":[
                "invalid_item"
            ],
        }

        with pytest.raises(ValidationError):
            validate(instance, schema_a)  

        instance = {
            "nodes":[
                False
            ],
        }

        with pytest.raises(ValidationError):
            validate(instance, schema_a)    

        instance = {
            "nodes":[
                1.3
            ],
        }

        with pytest.raises(ValidationError):
            validate(instance, schema_a)  

    def test_valid_node_records(self):
        instance = {
            "nodes":[
                {
                    "id": "1",
                    "type": "Person"
                }
            ]
        }
        assert validate(instance, schema_a) == None