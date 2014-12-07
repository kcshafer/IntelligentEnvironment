from smartsandbox.models import ExtractOrder, SObject
from smartsandbox.refs import AMBIGOUS_RELATIONSHIPS
from smartsandbox.sql import insert
from smartsandbox.utils import fix_psql_names

amount = 500
def extract_from_salesforce(engine):
    for eo in engine.config_session.query(ExtractOrder).all():
        for sobj in engine.config_session.query(SObject).all():
            tablename = fix_psql_names(sobj.name)
            records = engine.source_client.query(sobj.build_query())
            keys = []
            values = ''
            store_keys = True
            for r in records:
                vals = []
                for k,v in r.iteritems():
                    #TODO: figure out how to handle city in account and masterrecord id 
                    if k != 'attributes' and k != 'BillingAddress' and k != 'ShippingAddress':
                        k = tablename + '_' + k if k in AMBIGOUS_RELATIONSHIPS else k
                        if store_keys:
                            keys.append(k)
                        v = str(v)
                        v = v.replace("'", "''") if v else None
                        vals.append(v)
                values = values + '(' + ', '.join(["'" + str(v) + "'" for v in vals]) + '),'
                store_keys = False
            keys_statement = ', '.join([str(key) for key in keys])
            values = values[:-1]
            insert_statement = insert % (tablename, keys_statement, values)
            f = open('test.sql', 'w+')
            f.write(insert_statement)
            f.close()
            engine.data_session.execute(insert_statement)

        engine.data_session.commit()