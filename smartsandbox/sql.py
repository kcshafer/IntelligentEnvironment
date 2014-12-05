relationship_sobject_join = '''
SELECT parent.name as parent_name, child.name as child_name, relationship.field 
FROM relationship 
INNER JOIN sobject AS parent
    ON relationship.parent_id=parent.id 
INNER JOIN sobject AS child 
    ON relationship.child_id=child.id;
'''

add_foreign_key = '''
    ALTER TABLE %s
    ADD COLUMN %s VARCHAR(18)
    REFERENCES  %s(id); 
'''