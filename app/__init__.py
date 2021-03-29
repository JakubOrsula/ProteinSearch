import multiprocessing
from flask import Flask


application = Flask(__name__)

application.jinja_env.trim_blocks = True
application.jinja_env.lstrip_blocks = True


application.mp_manager = multiprocessing.Manager()
application.db_stats = application.mp_manager.dict()
application.computation_results = application.mp_manager.dict()

application.secret_key = 'protein search secret key'

if __name__ == '__main__':
    application.run()

from . import routes
