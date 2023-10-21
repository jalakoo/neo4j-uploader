# Neo4j-uploader
For uploading specially formatted dictionary data to a specific Neo4j database instance

## Dictionary Format
Either a .json or dictionary can be passed as an arg into the upload() function.
This dictionary must have a key named `nodes` and optionally one named `relationships`

The value for the `nodes` key must be a dictionary, where each key is the primary node label for the node records to upload. The value of each is a list of dictionaries that represent node properties to upload.

The value for the `relationships` key must be a dictionary, where each key is relationship type. The value of each is a list of dictionaries that are the relationship properties to upload.

Every record dictionary must have `_uid` key and the relationships must have a `_from__uid` and `_to__uid` key.

Example .json:
```
{
    "nodes": {
      "Person": [
        {
          "first_name": "Jaclyn",
          "last_name": "Stacey",
          "email": "{'reference': ['first_name']}.{'reference': ['last_name']}@email.com",
          "_uid": "b267e10d-998b-4804-b5a5-fe84b9ee982a"
        },
        {
          "first_name": "Ryan",
          "last_name": "Adam",
          "email": "{'reference': ['first_name']}.{'reference': ['last_name']}@email.com",
          "_uid": "0053fd25-71e4-40c1-80ff-f6b7e4211c77"
        }
      ]
    },
    "relationships": {
      "LIVES_WITH": [
        {
          "_from__uid": "b267e10d-998b-4804-b5a5-fe84b9ee982a",
          "_to__uid": "0053fd25-71e4-40c1-80ff-f6b7e4211c77",
          "_uid": "9db12425-0eba-47cf-8654-84ad235f98c1"
        }
      ]
    }
  }
```

## Usage
`pip install neo4j-uploader`

In your application
```
from neo4j_uploader import upload

credentials = ("database_uri", "user", "password")
data = ...<dictionary_or_json_payload>
upload(credentials, data)
```

## Limitations
This package currently requires the .json data payload to be strictly formatted to the above requirements.