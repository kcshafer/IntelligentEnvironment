from lxml import etree

from smartsandbox.api.client import NS
from smartsandbox.models import Relationship
from smartsandbox.sql import child_relationship_join, load_sobject_order
from smartsandbox.utils import fix_psql_names

#controller function
def load(engine):
    print "***************Beginning data load*******************"
    res = engine.config_session.execute(load_sobject_order)
    sobjects = []
    for r in res.fetchall():
        sobjects.append(dict(zip(res.keys(), r)))

    for sobj in sobjects:
        print "     ************LOADING %s****************" % sobj.get('name')
        created = 0
        offset = 0
        print sobj.get('amount')
        while offset < sobj.get('amount'):
            rows = engine.data_session.execute('SELECT *  FROM ' + fix_psql_names(sobj.get('name')) + ' LIMIT 200 OFFSET ' + str(offset))
            ids = load_table_slice(engine, rows, sobj.get('name'))
            update_foreign_keys(engine, sobj.get('name'), ids)
            offset = offset + 200        

        

#load the slices of each of the table in batches
def load_table_slice(engine, rows, table):
    keys = rows.keys()
    rows = rows.fetchall()
    result = engine.source_client.insert(table, rows, keys)
    old_ids = []
    new_ids = []
    for row in rows:
        old_ids.append(row.id)
    for r in result:
        new_ids.append(r.id)

    print old_ids
    print new_ids

    ids = dict(zip(old_ids, new_ids))

    return ids

#update the foreign keys for tables with the new id retrieved from salesforce
def update_foreign_keys(engine, table, ids):
    #get the relationships that we need to update
    crj = child_relationship_join % table.capitalize()
    res = engine.config_session.execute(crj)
    child_relationships = []
    table = fix_psql_names(table)
    for r in res.fetchall():
        child_relationships.append(dict(zip(res.keys(), r)))

    for cr in child_relationships:
        keys = [k.encode() for k in ids.keys()]
        child_table = fix_psql_names(cr.get('name'))
        print child_table
        for row in engine.data_session.execute('SELECT %s FROM %s WHERE %s IN %s' % (cr.get('field'), child_table, cr.get('field'), str(tuple(keys)))).fetchall():
            engine.data_session.execute("UPDATE %s set %s = '%s' WHERE %s = '%s'" % (child_table, cr.get('field'), ids.get(row[0]), cr.get('field'), row[0]))

        engine.data_session.commit()