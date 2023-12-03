# Neo4j-uploader
For uploading specially formatted dictionary data to a Neo4j database instance.

## Installation
`pip install neo4j-uploader`


## Usage
In your application
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

## Arguments
* `credentials` - A tuple of (database_uri, user, password)
* `data` - A dictionary of nodes and relationships to upload. See the Data Argument Format section for more details.
* `node_key` - The name of the key in the nodes dictionary that contains the node name. Defaults to "name"
* `dedupe_nodes` - If True, will not upload duplicate Nodes. Only updates existing with specified properties. Defaults to True
* `dedupe_relationships` - If True, will not created duplicate Relationships. Only updates existing with specified properties. Defaults to True
* `should_overwrite` - If True, will clear existing constraints, and will overwrite any existing Nodes and Relationships. Defaults to False.


## Data Argument Format
Either a .json string or dictionary can be passed as an arg into the upload() function.
This dictionary must have a key named `nodes` and optionally one named `relationships`

The value for the `nodes` key must be a dictionary, where each key is the primary node label for the node records to upload. The value of each is a list of dictionaries that represent node properties to upload.

The value for the `relationships` key must be a dictionary, where each key is relationship type. The value of each is a list of dictionaries that are the relationship properties to upload.

Every node record dictionary must have a unique key specified in the `node_key` argument. The default is `_uid` if none is provided. 

Relationships by default will use `_from_<node_key>` and `_to_<node_key>` key-values to determine which Nodes a Relationship should connect. However, from version 0.5.0 on, a custom property key may be assigned. For example, if a relationship has a key `_from_gid`, the uploader will attempt to connect this relationship to a node with a property named `gid`. Make sure this property is unique across all nodes. If this property value is not unique, the uploader will not guarantee which node it will select among matching values.

Example .json using a custom node_key:
```
{
    "node_key": "nid",
    "nodes": {
      "Person": [
        {
          "first_name": "Jaclyn",
          "last_name": "Stacey",
          "nid": "b267e10d-998b-4804-b5a5-fe84b9ee982a"
        },
        {
          "first_name": "Ryan",
          "last_name": "Adam",
          "nid": "0053fd25-71e4-40c1-80ff-f6b7e4211c77"
        }
      ]
    },
    "relationships": {
      "LIVES_WITH": [
        {
          "_from_nid": "b267e10d-998b-4804-b5a5-fe84b9ee982a",
          "_to_nid": "0053fd25-71e4-40c1-80ff-f6b7e4211c77",
          "since" : "2018-02-01"
        }
      ]
    }
  }

Example .json using a custom node_key:
```
{
    "node_key": "nid",
    "nodes": {
      "Person": [
        {
          "first_name": "Jaclyn",
          "last_name": "Stacey",
          "nid": "b267e10d-998b-4804-b5a5-fe84b9ee982a"
        },
        {
          "first_name": "Ryan",
          "last_name": "Adam",
          "nid": "0053fd25-71e4-40c1-80ff-f6b7e4211c77"
        }
      ]
    },
    "relationships": {
      "LIVES_WITH": [
        {
          "_from_nid": "b267e10d-998b-4804-b5a5-fe84b9ee982a",
          "_to_nid": "0053fd25-71e4-40c1-80ff-f6b7e4211c77",
          "since" : "2018-02-01"
        }
      ]
    }
  }
```