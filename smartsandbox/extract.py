from sqlalchemy import Table, Column, Integer, Unicode, MetaData, create_engine
from sqlalchemy.orm import mapper, create_session

from smartsandbox.models import SObject, Relationship
from smartsandbox.refs import PSQL_RESERVED_WORDS
from smartsandbox.sql import relationship_sobject_join, add_foreign_key

def extract_source_schema(engine):
    print "***************Extracting all tables*********************"
    for sobj in engine.config_session.query(SObject).all():
        #TODO: handle this earlier in the scan so that we store in the config database the translation
        tablename = sobj.name + '2' if sobj.name in PSQL_RESERVED_WORDS else sobj.name
        create_statement = 'CREATE TABLE %s (Id VARCHAR(18) PRIMARY KEY, ' % tablename
        dsobj = engine.source_client.sobject_describe(sobj.name)
        for field in dsobj.get('fields'):
            if field.get('name') == 'Id':
                pass
            elif field.get('type') == 'reference':
                pass
            elif field.get('type') == 'textarea':
                create_statement = create_statement + '%s TEXT, ' % field.get('name')
            else:
                create_statement = create_statement + '%s VARCHAR(255), ' % field.get('name')

        create_statement = create_statement[:-2] + ')'
        engine.data_session.execute(create_statement)

    engine.data_session.commit()

    print "*****************Building Table Relationships********************"
    res = engine.config_session.execute(relationship_sobject_join).fetchall()
    relationships = []
    for r in res:
        relationships.append(dict(zip(r.keys(), r)))

    #TODO: better ways to handle ambigous column names, probably at a higher level so it can be used throughout the program. Perhaps doing the modification earlier, in the program
    for rel in relationships:
        child_name = rel.get('child_name') + '2' if rel.get('child_name') in PSQL_RESERVED_WORDS else rel.get('child_name')
        parent_name = rel.get('parent_name') + '2' if rel.get('parent_name') in PSQL_RESERVED_WORDS else rel.get('parent_name')
        field = rel.get('field') + '_' + rel.get('parent_name') if rel.get('field') in ('WhatId', 'WhoId', 'ParentId', 'RelationId')  else rel.get('field')
        print field
        print parent_name
        print child_name
        alter_statement = add_foreign_key % (child_name, field, parent_name)
        engine.data_session.execute(alter_statement)

    engine.data_session.commit()