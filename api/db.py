'''
database connection
'''
from sqlalchemy import create_engine

from api.settings import DB_USER, DB_PASS

ENGINE = create_engine(
    "mysql+pymysql://{}:{}@localhost/himachal_pradesh_data".format(DB_USER, DB_PASS)
)
CONNECTION = ENGINE.connect()
