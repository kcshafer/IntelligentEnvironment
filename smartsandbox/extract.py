from sqlalchemy import Table, Column, Integer, Unicode, MetaData, create_engine
from sqlalchemy.orm import mapper, create_session

from smartsandbox.models import SObject, Relationship
from smartsandbox.refs import PSQL_RESERVED_WORDS, AMBIGOUS_RELATIONSHIPS
from smartsandbox.sql import relationship_sobject_join, add_foreign_key, has_owners_join
from smartsandbox.utils import fix_psql_names

def extract_source_schema(engine):
    print "***************Extracting all tables*********************"
    for sobj in engine.config_session.query(SObject).all():
        print "Creating table %s" % sobj.name 
        #TODO: handle this earlier in the scan so that we store in the config database the translation
        tablename = fix_psql_names(sobj.name)
        create_statement = 'CREATE TABLE %s (Id VARCHAR(18) PRIMARY KEY, old_id VARCHAR(18), insert_row BOOLEAN, inserted BOOLEAN, ' % tablename
        dsobj = engine.source_client.sobject_describe(sobj.name)
        for field in dsobj.get('fields'):
            if field.get('name') == 'Id':
                pass
            elif field.get('type') == 'reference':
                create_statement = create_statement + '%s VARCHAR(18), ' % field.get('name')
            elif field.get('type') == 'textarea':
                create_statement = create_statement + '%s TEXT, ' % field.get('name')
            else:
                create_statement = create_statement + '%s TEXT, ' % field.get('name')

        create_statement = create_statement[:-2] + ')'
        engine.data_session.execute(create_statement)

    engine.data_session.commit()