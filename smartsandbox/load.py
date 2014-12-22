from lxml import etree

from smartsandbox.api.client import NS
from smartsandbox.models import Relationship
from smartsandbox.sql import child_relationship_join
from smartsandbox.utils import fix_psql_names

#controller function
def load(engine):
    pass

#load the slices of each of the table in batches
def load_table_slice(engine):
    accounts = engine.data_session.execute('select * from account limit 10')
    keys = accounts.keys()
    accounts = accounts.fetchall()
    result = engine.source_client.insert('Account', accounts, keys)
    old_ids = []
    new_ids = []
    for account in accounts:
        old_ids.append(account.id)
    for r in result:
        new_ids.append(r.id)

    print old_ids
    print new_ids

    print dict(zip(old_ids, new_ids))

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
        for row in engine.data_session.execute('SELECT %s FROM %s WHERE %s IN %s' % (cr.get('field'), child_table, cr.get('field'), str(tuple(keys)))).fetchall():
            engine.data_session.execute("UPDATE %s set %s = '%s' WHERE %s = '%s'" % (child_table, cr.get('field'), ids.get(row[0]), cr.get('field'), row[0]))

        engine.data_session.commit()