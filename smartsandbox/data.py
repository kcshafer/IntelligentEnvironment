from sqlalchemy import func
from smartsandbox.models import ExtractOrder, SObject
from smartsandbox.refs import AMBIGOUS_RELATIONSHIPS
from smartsandbox.sql import insert, relationship_to_parent_join, fields_sobject_join
from smartsandbox.utils import fix_psql_names, array_to_soql_string, salesforce_datetime_format, convert_soap_datetime

def extract_levels(engine):
    total_records = engine.config_session.query(func.sum(SObject.amount))
    total_created = 0
    created_dates = {}
    null_fields = generate_nullable_dictionary(engine)
    print "*****************Beginning data extract*********************"
    #TODO: need logic here or somewhere to handle how many records to actually pull
    #TODO: how to handle if this gets stuck, to jump to a new set of bottom level records?
    while total_created < 50000:
        print "Total created is %s" % total_created
        print created_dates
        parent_sobjects = {}
        for eo in engine.config_session.query(ExtractOrder).all():
            print "Total created is %s" % total_created
            print "Starting level %s" % eo.position
            parent_sobjects, created_dates, tc = execute_levels(engine, eo, parent_sobjects, created_dates, null_fields)
            total_created = total_created + tc

def generate_nullable_dictionary(engine):
    res = engine.config_session.execute(fields_sobject_join )
    fields = []
    null_fields = {}
    #TODO: might not need to dict zip this
    for r in res:
        fields.append(dict(zip(r.keys(), r)))

    for field in fields:
        if field.get('sobject_name') not in null_fields.keys():
            null_fields[field.get('sobject_name')] = []
        null_fields.get(field.get('sobject_name')).append(field.get('field_name'))

    return null_fields

#TODO this just needs a lot of cleaning and optimizing, and maybe some light commenting
def execute_levels(engine, lvl, parent_sobjects, created_dates, null_fields):
    #extract first level 

    #query for all level one sobjects
    total_created = 0
    for sobj in engine.config_session.query(SObject).filter(SObject.extract_order_id==lvl.id).all():
        print total_created
        count = 0
        print "     Extracting " + sobj.name
        #TODO: this should be handled sooner
        tablename = fix_psql_names(sobj.name)
        parent_sobjects_temp = {}
        if sobj.name in parent_sobjects and parent_sobjects.get(sobj.name):
            records = engine.source_client.query(sobj.build_query() + " WHERE Id IN %s" % array_to_soql_string(parent_sobjects.get(sobj.name)))
        else:
            if sobj.name not in created_dates:
                #TODO: exclude sobjects that return less than 10, meaning they have no records for future queries
                records = engine.source_client.query(sobj.build_query() + " LIMIT 50")
            else:
                records = engine.source_client.query(sobj.build_query() + " WHERE CreatedDate < %s LIMIT 50 " % created_dates.get(sobj.name))

        keys = []
        values = ''
        store_keys = True

        #find all relationships
        res = engine.config_session.execute(relationship_to_parent_join % sobj.id).fetchall()
        relationships = {}
        for r in res:
            rel = dict(zip(r.keys(), r))
            parent_sobjects_temp[rel.get('name')] = []
            relationships[rel.get('field')] = rel.get('name')

        for r in records:
            #vals = []
            exists = engine.data_session.execute("SELECT count(id) FROM %s WHERE Id = '%s'" % (tablename, r.get('Id')))
            exists = exists.first()[0]
            if exists == 0:
                vals = '('
                for k, v in r.iteritems():
                    #TODO: fix the billing and shippingaddress
                    if k == 'CreatedDate':
                        created_date = convert_soap_datetime(v)
                        created_date = salesforce_datetime_format(created_date)
                        created_dates[sobj.name] = created_date
                    if k not in ('attributes', 'BillingAddress', 'ShippingAddress'):
                        if store_keys:
                            keys.append(k)
                        print k
                        print sobj.name
                        print null_fields.get(sobj.name)
                        if null_fields.get(sobj.name) and k in null_fields.get(sobj.name):
                            if v is None:
                                vals = vals + 'null, '
                            else:
                                vals = vals  + "'%s', " % v
                            print "value is"
                        else:
                            # if the curent field is a relationship field
                            if k in relationships:
                                parent_sobjects_temp.get(relationships.get(k)).append(v)
                            if not isinstance(v, basestring):
                                v = str(v)
                            try:
                                v = v.decode('utf-8')
                            except UnicodeError:
                                v = v.encode('utf-8')
                                v = v.decode('utf-8')
     
                            v = v.replace("'", "''") if v else None
                            v = v.replace(":", "\\:")
                            print "value is"
                            print v
                            vals = vals + ("'%s', " % v if v else "null, ")
                        #vals.append(v)
                vals = vals[:-2] + '), '
                total_created = total_created + 1
                count = count + 1
                #values = values + '(' + ', '.join([("'" + v + "'").replace('None', 'Null') for v in vals]) + '),'
                store_keys = False
                values = values + vals

        if count > 0:
            keys_statement = ', '.join([str(key) for key in keys])
            values = values[:-2]
            insert_statement = insert % (tablename, keys_statement, values)
            f = open('insert.txt', 'w+')
            f.write(insert_statement)
            f.close()
            engine.data_session.execute(insert_statement)

            engine.data_session.commit()
        
    return parent_sobjects_temp, created_dates, total_created