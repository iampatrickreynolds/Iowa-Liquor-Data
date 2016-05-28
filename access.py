from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session


db_location = os.path.expanduser("~/iowa.db")

def get_engine():
    engine = create_engine('sqlite:///' + db_location, echo=False)
    return engine

def get_session():
    engine = get_engine()
    session = scoped_session(sessionmaker(bind=engine))
    return session
