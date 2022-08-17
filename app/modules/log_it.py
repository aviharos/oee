'''
A logger decorator. Source:
https://github.com/CoreyMSchafer/code_snippets/blob/master/Decorators/decorators.py
'''
from functools import wraps
import sys

def log_it(orig_func):
    import logging
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    @wraps(orig_func)
    def wrapper(*args, **kwargs):
        logging.info(
            'Ran {} with args: {}, and kwargs: {}'.format(orig_func.__name__, args, kwargs))
        return orig_func(*args, **kwargs)

    return wrapper

