# Neo4j-uploader
For uploading specially formatted dictionary data to a Neo4j database instance.

## Installation
`pip install neo4j-uploader`

## Usage
```
from neo4j_uploader import batch_upload

config = {
    "neo4j_uri": "<NEO4J_URI>",
    "neo4j_user": "<NEO4J_USER>",
    "neo4j_password": "<NEO4J_PASSWORD>",
    "overwrite": true
}

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

result = batch_upload(config, data)
```

The original `upload` function using the simpler schema also still works:

```
from neo4j_uploader import upload

credentials = ("database_uri", "user", "password")
data = {
  "nodes":
  {
    "Person": [
      {"name": "Bob", "age": 30},
      {"name": "Jane", "age": 25}
    ]
  },
  "relationships":
  {
    "KNOWS": [
      {"_from_name": "Bob", "_to_name": "Jane", "since": 2015}
    ]
  }
}
upload(credentials, data, node_key="name)
```

## Documentation
[Documentation](https://jalakoo.github.io/neo4j-uploader/neo4j_uploader.html) for the current version.
