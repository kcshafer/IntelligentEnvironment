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

bottom_relationship_join = '''
SELECT sobject.id, sobject.name
FROM sobject
WHERE NOT EXISTS (
    SELECT 1 FROM relationship
    WHERE relationship.parent_id = sobject.id AND relationship.parent_id <> relationship.child_id
);
'''

next_level_join = '''
select sobject.name, sobject.id
from relationship
inner join sobject
    on relationship.parent_id = sobject.id
where relationship.child_id = %s and relationship.child_id <> relationship.parent_id; 
'''

has_owners_join = '''
SELECT sobject.name
FROM sobject
WHERE EXISTS (
    SELECT 1 FROM sobject_owner
);
'''

insert = '''
INSERT INTO %s
(%s)
VALUES %s
'''