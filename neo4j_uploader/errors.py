class InvalidCredentialsError(Exception):
    """Error if Neo4j credentials are invalid"""
    pass

class InvalidPayloadError(Exception):
    """Error if json payload schema is unrecognized"""
    pass