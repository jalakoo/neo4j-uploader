schema_a = {
    "type": "object",
    "properties":{
        "nodes": {
            "type": "array",
            "items":{
                "type": "object",
            }
            },
        "relationships":{
            "type": "object",
        }
    },
    "required":["nodes"]
}

schema_b = {
    "type": "object",
    "properties":{
        "max_batch_size" : {"type":"integer"},
        "nodes": {
            "type": "array",
            "items":{
                "type": "object",
                "properties":{
                    "labels": {
                        "type":"array",
                        "items":{
                            "type":"string"
                        }
                    },
                    "key": "string",
                    "records" : {
                        "type" : "array",
                        "items":{
                            "type": "object",
                        }
                    }
                }
            }
        },
        "relationships":{
            "type": "array",
            "items":{
                "type": "object",
                "properties":{
                    "from_node":{
                        "type":"object",
                        "properties":{
                            "label": "string",
                            "properties":"object"
                        }
                    },
                    "to_node":{
                        "type":"object",
                        "properties":{
                            "label": "string",
                            "properties":"object"
                        }
                    },
                    "records":{
                        "type":"array",
                        "items":{
                            "type":"object",
                        }
                    }
                }
            }
        }
    },
    "required":["nodes"]
}

from jsonschema import validate
from jsonschema.exceptions import ValidationError, SchemaError
from enum import Enum

class SchemaType(Enum):
    UNKNOWN = 0
    A = 1
    B = 2

def upload_schema(data: dict) -> SchemaType:
    try:
        if validate(data, schema_a) is None:
            return SchemaType.A
    except ValidationError as _:
        pass
    except SchemaError as _:
        pass

    try:
        if validate(data, schema_b) is None:
            return SchemaType.B
    except ValidationError as _:
        pass
    except SchemaError as _:
        pass

    return SchemaType.UNKNOWN