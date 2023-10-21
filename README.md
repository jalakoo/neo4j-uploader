# Neo4j-uploader
For uploading specially formatted dictionary data to a Neo4j database instance

## Dictionary Format
Either a .json or dictionary can be passed as an arg into the upload() function.
This dictionary must have a key named `nodes` and optionally one named `relationships`

The value for the `nodes` key must be a dictionary, where each key is the primary node label for the node records to upload. The value of each is a list of dictionaries that represent node properties to upload.

The value for the `relationships` key must be a dictionary, where each key is relationship type. The value of each is a list of dictionaries that are the relationship properties to upload.

See the `samples/data.json` file for an example.
