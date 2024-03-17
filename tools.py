import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from time import time 
  
  
def timer(func): 
    # This function shows the execution time of  
    # the function object passed 
    def wrap_func(*args, **kwargs): 
        t1 = time() 
        result = func(*args, **kwargs) 
        t2 = time() 
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s') 
        return result 
    return wrap_func 

def get_logger(file_name: str = 'app.log'):
    logger_name = 'my_app_logger'  # A fixed name for the shared logger
    logger = logging.getLogger(logger_name)

    # Check if the logger already has handlers to avoid adding them again
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter(fmt="[%(asctime)s %(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        
        # Console handler
        console_handler = StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Rotating file handler
        rotating_handler = RotatingFileHandler(file_name, maxBytes=100000, backupCount=10)
        rotating_handler.setFormatter(formatter)
        logger.addHandler(rotating_handler)

    return logger


def human_size(bytes, units=[' bytes','KB','MB','GB','TB', 'PB', 'EB']):
    """ Returns a human readable string representation of bytes """
    return str(bytes) + units[0] if bytes < 1024 else human_size(bytes>>10, units[1:])