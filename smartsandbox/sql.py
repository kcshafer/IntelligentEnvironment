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


relationship_to_parent_join = '''
SELECT relationship.field, sobject.name
FROM relationship
INNER JOIN sobject
ON relationship.parent_id = sobject.id
WHERE relationship.child_id = %s
'''

child_relationship_join = '''
SELECT sobject.id, child.name, relationship.field 
FROM sobject 
INNER JOIN relationship 
    ON relationship.parent_id = sobject.id 
INNER JOIN sobject 
    AS child 
    ON relationship.child_id = child.id
WHERE sobject.name = '%s'
'''

fields_sobject_join = '''
SELECT sobject.name AS sobject_name, field.name AS field_name
FROM field
INNER JOIN sobject
    ON field.sobject_id = sobject.id
WHERE field.type IN ('currency', 'double', 'integer', 'percent')
'''

load_sobject_order = '''
SELECT sobject.name,sobject.amount, extract_order.position
FROM sobject
INNER JOIN extract_order
    ON sobject.extract_order_id = extract_order.id
ORDER BY extract_order.position desc
'''