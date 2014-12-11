from smartsandbox.models import Base, SObject, Relationship, RecordType, Owner, SObjectOwner, ExtractOrder
from smartsandbox.refs import METADATA_OBJECTS, TEMPORARILY_UNSUPPORTED, EXCLUDE_RECORD_TYPES, EXCLUDE_OWNER
from smartsandbox.sql import bottom_relationship_join, next_level_join

from sqlalchemy import func
db = 'postgresql://localhost/smart_sandbox'


def retrieve_source_schema(engine):
    sobjects = engine.source_client.get_sobjects()

    print "**************Extracting SF Tables***************"

    for sobj in sobjects:
        if sobj.get('queryable') and sobj.get('name') not in METADATA_OBJECTS and sobj.get('name') not in TEMPORARILY_UNSUPPORTED:
            sobj_amount = engine.source_client.count(sobj.get('name'))
            #TODO: this would need to be retrieved or somehow set
            sobj.amount_requested = 
            if sobj_amount > 0:
                print 'Extracting %s' % sobj.get('name')
                s = SObject(name=sobj.get('name'), amount=sobj_amount)
                engine.config_session.add(s)
    
    engine.config_session.commit()

    print "******************Extracting relationship and record type data *****************"
    for sobj in engine.config_session.query(SObject).all():
        print "Extracting record types and relationships for %s" % sobj.name
        dsobj = engine.source_client.sobject_describe(sobj.name)
        #TODO: find a better way to deal with record types, objects that don't support record types and objects with only one
        if sobj.name not in EXCLUDE_RECORD_TYPES:
            for rt in dsobj.get('recordTypeInfos'):
                rt = RecordType(name=rt.get('name'), sf_id=rt.get('recordTypeId'), sobject_id=sobj.id)
                engine.config_session.add(rt)
        for cr in dsobj.get('childRelationships'):
            if cr.get('relationshipName') and cr.get('childSObject') not in METADATA_OBJECTS and cr.get('childSObject') not in TEMPORARILY_UNSUPPORTED and cr.get('childSObject') != 'CombinedAttachment':
                try:
                    child = engine.config_session.query(SObject).filter(SObject.name==cr.get('childSObject')).first()
                    rel = Relationship(name=cr.get('relationshipName'), parent_id=sobj.id, child_id=child.id, field=cr.get('field'))
                    engine.config_session.add(rel)
                except:
                    print "Relationships with %s are not supported" % cr.get('childSObject')
        fields = ''
        for field in dsobj.get('fields'):
            fields = fields + field.get('name') + ', '

        fields = fields[:-2]
        sobj.fields = fields
        engine.config_session.add(sobj)

    engine.config_session.commit()

    print "******************Extracting user information***********************"
    for user in engine.source_client.query('SELECT Id,Username, isActive FROM User'):
        print "Extracting %s" % user.get('Username')
        owner = Owner(is_active=(1 if user.get('IsActive') else 0), sf_id=user.get('Id'))
        engine.config_session.add(owner)

    engine.config_session.commit()

def analyze_record_distribution(engine):
    print "*****************Counting record totals********************"
    total_records = engine.config_session.query(func.sum(SObject.amount))
    #TODO: the total amount needed would need to make it's way here for the point of entry
    sample_ratio = 10000 / total_records
    for sobj in engine.config_session.query(SObject).all():
        print "Counting RecordTypes and Owner totals for %s" % sobj.name
        #TODO: find a better way to deal with record types, objects that don't support record types and objects with only one
        if sobj.name not in EXCLUDE_RECORD_TYPES:
            try:
                if len(sobj.record_types) == 1:
                    rt = sobj.record_types[0]
                    rt.amount = sobj.amount
                    engine.config_session.add(rt)
                else:
                    rt_count = engine.source_client.count_group(sobj.name, 'RecordTypeId')
                    for rt in sobj.record_types:
                        rt.amount = rt_count.get(rt.name)
                        rt.amount_requested = rt_count.get(rt.name)
                        engine.config_session.add(rt) * sample_ratio
            except:
                print "%s does not have a RecordTypeId field." % sobj.name
        if sobj.name not in EXCLUDE_OWNER:
            try:
                owner_count = engine.source_client.count_group(sobj.name, 'OwnerId')
                for id, amt in owner_count.iteritems():
                    #TODO: if possible, find better way of excluding queues
                    if id[:3] != '00G':
                        owner = engine.config_session.query(Owner).filter(Owner.sf_id==id).first()
                        sobject_owner = SObjectOwner(sobject_id=sobj.id, owner_id=owner.id, amount=amt, amount_requested=(amt * sample_ratio))
                        engine.config_session.add(sobject_owner)
            except:
                print "%s does not have an OwnerId field." % sobj.name

            sobj.amount_requested = sobj.amount * sample_ratio

    engine.config_session.commit()

#TODO: This doesn't handle self relationships yet becuase of issues with recursion, and this logic lives in the sql queries in sql.py
#TODO: this should implement a tree data structure and 
def plan_extraction_order(engine):
    objs = {'0': []}
    res = engine.config_session.execute(bottom_relationship_join).fetchall()
    relationships = []
    for r in res:
        relationships.append(dict(zip(r.keys(), r)))

    child_ids = []
    sobjects = []
    for sobj in engine.config_session.query(SObject).all():
        sobjects.append(sobj.name)

    eo = ExtractOrder(position=0)
    engine.config_session.add(eo)
    engine.config_session.commit()
    for rel in relationships:
        print "Assigning %s to %s" % (rel.get('name'), 0)
        sobjects.remove(rel.get('name'))
        objs.get('0').append(rel.get('name'))
        child_ids.append(rel.get('id'))
        sobj = engine.config_session.query(SObject).filter(SObject.id==rel.get('id')).first()
        sobj.extract_order_id = eo.id
        engine.config_session.add(sobj)
    engine.config_session.commit()

    counter = 1
    #TODO: this limits to 10 levels, not sure if it does anything anymore but it needs to go
    while len(sobjects) != 0 and counter != 10:
        eo = ExtractOrder(position=counter)
        engine.config_session.add(eo)
        engine.config_session.commit()
        child_ids_temp = []
        objs[counter] = []
        for cid in child_ids:
            query = next_level_join % cid
            res = engine.config_session.execute(query).fetchall()
            crelationships = []
            for r in res:
                crelationships.append(dict(zip(r.keys(), r)))
            for crel in crelationships:
                if crel.get('name') in sobjects:
                    objs.get(counter).append(crel.get('name'))
                    child_ids_temp.append(crel.get('id'))
                    print "Assigning %s to %s" % (crel.get('name'), counter)
                    sobj = engine.config_session.query(SObject).filter(SObject.id==crel.get('id')).first()
                    sobj.extract_order_id = eo.id
                    sobjects.remove(crel.get('name'))
        engine.config_session.commit()
        child_ids = child_ids_temp
        counter = counter + 1

    print objs
