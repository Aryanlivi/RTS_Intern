import logging

LOG_NAME=os.getenv('log_name')
LOG_PATH=os.getenv('log_path')
class Logger:
    def __init__(self, logger_name=LOG_NAME, log_file=LOG_PATH):
        self.log_level=logging.DEBUG
        self.logger = logging.getLogger(logger_name) 
        self.logger.setLevel(self.log_level)
        
        # Create handlers
        self.file_handler = logging.FileHandler(log_file)
        self.console_handler = logging.StreamHandler()
        
        # Set the logging level for handlers
        self.file_handler.setLevel(logging.DEBUG)
        self.console_handler.setLevel(logging.INFO)
        
        # Create a formatter
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        self.formatter = logging.Formatter(log_format)
        
        # Attach formatter to handlers
        self.file_handler.setFormatter(self.formatter)
        self.console_handler.setFormatter(self.formatter)
        
        # Add handlers to the logger
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.console_handler)

    def get_logger(self):
        return self.logger
