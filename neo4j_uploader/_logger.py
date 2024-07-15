import logging

module_name = "neo4j_uploader"
logger = logging.getLogger(module_name)
if not len(logger.handlers):
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"%(levelname)s:{module_name}:%(asctime)s:%(message)s"
    )
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

# To remove/clear the module stream handler
# logger.removeHandler(stream_handler)
