from smartsandbox.models import Base, SObject, Relationship, RecordType, Owner, SObjectOwner
from smartsandbox.refs import METADATA_OBJECTS, TEMPORARILY_UNSUPPORTED, EXCLUDE_RECORD_TYPES, EXCLUDE_OWNER

db = 'postgresql://localhost/smart_sandbox'


def retrieve_source_schema(engine):
    sobjects = engine.source_client.get_sobjects()

    print "**************Extracting SF Tables***************"

    for sobj in sobjects:
        if sobj.get('queryable') and sobj.get('name') not in METADATA_OBJECTS and sobj.get('name') not in TEMPORARILY_UNSUPPORTED:
            s = SObject(name=sobj.get('name'))
            engine.config_session.add(s)
    
    engine.config_session.commit()

    print "******************Extracting relationship and record type data *****************"
    for sobj in engine.config_session.query(SObject).all():
        dsobj = engine.source_client.sobject_describe(sobj.name)
        #TODO: find a better way to deal with record types, objects that don't support record types and objects with only one
        if sobj.name not in EXCLUDE_RECORD_TYPES:
            for rt in dsobj.get('recordTypeInfos'):
                rt = RecordType(name=rt.get('name'), sf_id=rt.get('recordTypeId'), sobject_id=sobj.id)
                engine.config_session.add(rt)
        for cr in dsobj.get('childRelationships'):
            if cr.get('relationshipName') and cr.get('childSObject') not in METADATA_OBJECTS and cr.get('childSObject') not in TEMPORARILY_UNSUPPORTED:
                child = engine.config_session.query(SObject).filter(SObject.name==cr.get('childSObject')).first()
                rel = Relationship(name=cr.get('relationshipName'), parent_id=sobj.id, child_id=child.id, field=cr.get('field'))
                engine.config_session.add(rel)

    engine.config_session.commit()

    print "******************Extracting user information***********************"
    for user in engine.source_client.query('SELECT Id, isActive FROM User'):
        owner = Owner(is_active=(1 if user.get('IsActive') else 0), sf_id=user.get('Id'))
        engine.config_session.add(owner)

    engine.config_session.commit()

def analyze_record_distribution(engine):
    print "*****************Counting record totals********************"
    for sobj in engine.config_session.query(SObject).all():
        sobj.amount = engine.source_client.count(sobj.name)
        engine.config_session.add(sobj)
        #TODO: find a better way to deal with record types, objects that don't support record types and objects with only one
        if sobj.name not in EXCLUDE_RECORD_TYPES:
            if len(sobj.record_types) == 1:
                rt = sobj.record_types[0]
                rt.amount = sobj.amount
                engine.config_session.add(rt)
            else:
                rt_count = engine.source_client.count_group(sobj.name, 'RecordTypeId')
                for rt in sobj.record_types:
                    rt.amount = rt_count.get(rt.name)
                    engine.config_session.add(rt)
        if sobj.name not in EXCLUDE_OWNER:
            owner_count = engine.source_client.count_group(sobj.name, 'OwnerId')
            for id, amt in owner_count.iteritems():
                owner = engine.config_session.query(Owner).filter(Owner.sf_id==id).first()
                print 'owner'
                print sobj.name
                print owner
                print id
                print amt
                sobject_owner = SObjectOwner(sobject_id=sobj.id, owner_id=owner.id, amount=amt)
                engine.config_session.add(sobject_owner)

    engine.config_session.commit()







