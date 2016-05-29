"""Access to the database. Use
    session = get_session()
to get a thread safe session for the database.
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

db_location = os.path.expanduser("~/iowa.sqlite")

def get_engine(echo=False):
    engine = create_engine('sqlite:///' + db_location, echo=echo)
    return engine

def get_session(echo=False, scoped=False):
    engine = get_engine(echo=echo)
    sm = sessionmaker(bind=engine)
    if scoped:
        session = scoped_session()
    else:
        session = sm()
    return session
