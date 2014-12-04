from sqlalchemy import create_engine

from smartsandbox.models import Base

def setup():
    engine = create_engine('postgresql://localhost/smart_sandbox')
 
    Base.metadata.create_all(engine)