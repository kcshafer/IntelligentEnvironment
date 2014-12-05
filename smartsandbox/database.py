from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from smartsandbox.api.client import SalesforceClient
from smartsandbox.models import Base

class SuperEngine(object):

    def __init__(self):

        config_engine = create_engine('postgresql://localhost/smart_sandbox')
        Base.metadata.bind = config_engine
        ConfigSession = sessionmaker(bind=config_engine)
        self.config_session = ConfigSession()

        data_engine = create_engine('postgresql://localhost/ss_data')
        DataSession = sessionmaker(bind=data_engine)
        self.data_session = DataSession()

        self.source_client = SalesforceClient('kc@analyticscloud.com', 'Rubygem14')
