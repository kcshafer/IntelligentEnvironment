from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from smartsandbox.api.client import SalesforceClient
from smartsandbox.models import Base, SObject, Relationship, RecordType, Owner, SObjectOwner
from smartsandbox.refs import METADATA_OBJECTS, TEMPORARILY_UNSUPPORTED, EXCLUDE_RECORD_TYPES, EXCLUDE_OWNER

db = 'postgresql://localhost/smart_sandbox'

class Scanner(object):
    def __init__(self):
        engine = create_engine(db)
        Base.metadata.bind = engine
        DBSession = sessionmaker(bind=engine)

        self.session = DBSession()
        self.client = SalesforceClient('kc@analyticscloud.com', 'Rubygem14')

    def retrieve_source_schema(self):
        sobjects = self.client.get_sobjects()

        print "**************Extracting SF Tables***************"

        for sobj in sobjects:
            if sobj.get('queryable') and sobj.get('name') not in METADATA_OBJECTS and sobj.get('name') not in TEMPORARILY_UNSUPPORTED:
                s = SObject(name=sobj.get('name'))
                self.session.add(s)
        
        self.session.commit()

        print "******************Extracting relationship and record type data *****************"
        for sobj in self.session.query(SObject).all():
            dsobj = self.client.sobject_describe(sobj.name)
            #TODO: find a better way to deal with record types, objects that don't support record types and objects with only one
            if sobj.name not in EXCLUDE_RECORD_TYPES:
                for rt in dsobj.get('recordTypeInfos'):
                    rt = RecordType(name=rt.get('name'), sf_id=rt.get('recordTypeId'), sobject_id=sobj.id)
                    self.session.add(rt)
            for cr in dsobj.get('childRelationships'):
                if cr.get('relationshipName'):
                    rel = Relationship(name=cr.get('relationshipName'), sobject_id=sobj.id, field=cr.get('field'))
                    self.session.add(rel)

        self.session.commit()

        print "******************Extracting user information***********************"
        for user in self.client.query('SELECT Id, isActive FROM User'):
            owner = Owner(is_active=(1 if user.get('IsActive') else 0), sf_id=user.get('Id'))
            self.session.add(owner)

        self.session.commit()

    def analyze_record_distribution(self):
        print "*****************Counting record totals********************"
        for sobj in self.session.query(SObject).all():
            sobj.amount = self.client.count(sobj.name)
            self.session.add(sobj)
            #TODO: find a better way to deal with record types, objects that don't support record types and objects with only one
            if sobj.name not in EXCLUDE_RECORD_TYPES:
                if len(sobj.record_types) == 1:
                    rt = sobj.record_types[0]
                    rt.amount = sobj.amount
                    self.session.add(rt)
                else:
                    rt_count = self.client.count_group(sobj.name, 'RecordTypeId')
                    for rt in sobj.record_types:
                        rt.amount = rt_count.get(rt.name)
                        self.session.add(rt)
            if sobj.name not in EXCLUDE_OWNER:
                owner_count = self.client.count_group(sobj.name, 'OwnerId')
                for id, amt in owner_count.iteritems():
                    owner = self.session.query(Owner).filter(Owner.sf_id==id).first()
                    print 'owner'
                    print sobj.name
                    print owner
                    print id
                    print amt
                    sobject_owner = SObjectOwner(sobject_id=sobj.id, owner_id=owner.id, amount=amt)
                    self.session.add(sobject_owner)

        self.session.commit()







