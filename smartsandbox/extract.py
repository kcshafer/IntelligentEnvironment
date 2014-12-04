from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from smartsandbox.api.client import SalesforceClient
from smartsandbox.models import Base, SObject, Relationship, RecordType
from smartsandbox.refs import METADATA_OBJECTS

db = 'postgresql://localhost/smart_sandbox'

class Scanner(object):
    def __init__(self):
        self.client = SalesforceClient('kc@analyticscloud.com', 'Rubygem14')

    def retrieve_source_schema(self):
        engine = create_engine(db)
        Base.metadata.bind = engine
        DBSession = sessionmaker(bind=engine)
        session = DBSession()

        sobject_key = {}

        sobjects = self.client.get_sobjects()

        print 'starting sobjects'
        for sobj in sobjects:
            if sobj.get('queryable') and sobj.get('name') not in METADATA_OBJECTS:
                s = SObject(name=sobj.get('name'))
                session.add(s)
        
        session.commit()

        print 'starting relationships'
        for sobj in session.query(SObject).all():
            print sobj.name
            sobject_key[sobj.name] = sobj.id
            dsobj = self.client.sobject_describe(sobj.name)
            for cr in dsobj.get('childRelationships'):
                if cr.get('relationshipName'):
                    rel = Relationship(name=cr.get('relationshipName'), sobject_id=sobj.id, field=cr.get('field'))
                    session.add(rel)

        session.commit()

        print 'starting record types'
        for rt in self.client.query('SELECT Id, SobjectType, DeveloperName FROM RecordType'):
            record_type = RecordType(name=rt.get('DeveloperName'), sf_id=rt.get('Id'), sobject_id=sobject_key.get(rt.get('DeveloperName')))
            session.add(record_type)

        session.commit()



