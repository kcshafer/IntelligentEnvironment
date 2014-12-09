from smartsandbox.refs import PSQL_RESERVED_WORDS

#TODO: fix this by naming the table stored in sobjects this way during the first extract    
def fix_psql_names(name):
    return name + '2' if name in PSQL_RESERVED_WORDS else name

def array_to_soql_string(arr):
    return '(\'' + '\', \''.join(arr) + '\')'
