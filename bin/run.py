#!/usr/bin/env python

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def cwd():
    return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

from smartsandbox.models import Base

#probably temporary
engine = create_engine('postgresql://localhost/smart_sandbox')

if __name__ == '__main__':
    Base.metadata.bind = engine
    
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
