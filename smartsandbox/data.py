from sqlalchemy import func
from smartsandbox.models import ExtractOrder, SObject
from smartsandbox.refs import AMBIGOUS_RELATIONSHIPS
from smartsandbox.sql import insert, relationship_to_parent_join
from smartsandbox.utils import fix_psql_names, array_to_soql_string, salesforce_datetime_format, convert_soap_datetime

def extract_levels(engine):
    total_records = engine.config_session.query(func.sum(SObject.amount))
    total_created = 0
    created_dates = {}
    print "*****************Beginning data extract*********************"
    while total_created < 50000:
        print "Total created is %s" % total_created
        print created_dates
        parent_sobjects = {}
        for eo in engine.config_session.query(ExtractOrder).all():
            print "Total created is %s" % total_created
            print "Starting level %s" % eo.position
            parent_sobjects, created_dates, tc = execute_levels(engine, eo, parent_sobjects, created_dates)
            total_created = total_created + tc



def execute_levels(engine, lvl, parent_sobjects, created_dates):
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
            vals = []
            exists = engine.data_session.execute("SELECT count(id) FROM %s WHERE Id = '%s'" % (tablename, r.get('Id')))
            exists = exists.first()[0]
            if exists == 0:
                for k, v in r.iteritems():
                    #TODO: fix the billing and shippingaddress
                    if k == 'CreatedDate':
                        created_date = convert_soap_datetime(v)
                        created_date = salesforce_datetime_format(created_date)
                        created_dates[sobj.name] = created_date
                    if k not in ('attributes', 'BillingAddress', 'ShippingAddress'):
                        # if the curent field is a relationship field
                        if k in relationships:
                            parent_sobjects_temp.get(relationships.get(k)).append(v)
                        if store_keys:
                            keys.append(k)
                        if not isinstance(v, basestring):
                            v = str(v)
                        try:
                            v = v.decode('utf-8')
                        except UnicodeError:
                            v = v.encode('utf-8')
                            v = v.decode('utf-8')

                        v = v.replace("'", "''") if v else None
                        v = v.replace(":", "\\:")
                        vals.append(v)
                total_created = total_created + 1
                count = count + 1
                values = values + '(' + ', '.join(["'" + v + "'" for v in vals]) + '),'
                store_keys = False

        if count > 0:
            keys_statement = ', '.join([str(key) for key in keys])
            values = values[:-1]
            insert_statement = insert % (tablename, keys_statement, values)

            engine.data_session.execute(insert_statement)

            engine.data_session.commit()
        
    return parent_sobjects_temp, created_dates, total_created