import logging

class ModuleLogger(object):

    is_enabled : bool = False
    _logger = None

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ModuleLogger, cls).__new__(cls)
        return cls.instance

    def logger(self):
        if self.is_enabled is False:
            return EmptyLogger()
        
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
            # For custom formatting - This will cause duplicate logs for each message
            # FORMAT = "[%(asctime)s: %(filename)s: %(lineno)s - %(funcName)20s()] %(message)s"
            # formatter = logging.Formatter(FORMAT)
            # mh = logging.StreamHandler()
            # mh.setFormatter(formatter)
            # self._logger.addHandler(mh)
            self._logger.addHandler(logging.NullHandler())
        
        return self._logger

    def notset(self, arg: str):
        if self.is_enabled:
            return self.logger().notset(arg)
        pass

    def debug(self, arg: str):
        if self.is_enabled:
            return self.logger().debug(arg)
        pass
  
    def info(self, arg: str):
        if self.is_enabled:
            return self.logger().info(arg)
        pass

    def warning(self, arg: str):
        if self.is_enabled:
            return self.logger().warning(arg)
        pass

    def error(self, arg: str):
        if self.is_enabled:
            return self.logger().error(arg)
        pass

    def critical(self, arg: str):
        if self.is_enabled:
            return self.logger().critical(arg)
        pass

class EmptyLogger:
    
    def notset(self, arg:str):
        pass
    def debug(self, arg: str):
        pass
    def info(self, arg: str):
        pass
    def warning(self, arg: str):
        pass
    def error(self, arg: str):
        pass
    def critical(self, arg: str):
        pass