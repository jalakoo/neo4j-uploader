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
  "nodes":[
    {
      "labels": list[str],
      "key": str,
      "records": [
        {
          "<a_key>": any,
        }
      ]
    }
  ],
  "relationships":[
    {
        "type": str,
        "from_node": {
            "record_key": str,
            "node_key": str,
            "node_label": str
        },
        "to_node": {
            "record_key": str,
            "node_key": str,
            "node_label": str,
        },
        "exclude_keys": list[str],
        "records":[
            {
                "<record_key>": str,
                "<a_key>: any
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
[Documentation](docs/index.html) for the current version.