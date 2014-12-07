from multiprocessing import Process 
import os
import sys
import time

from smartsandbox.database import SuperEngine
from smartsandbox import scan, extract, data

db = 'postgresql://localhost/smart_sandbox'

def execute():
    t0 = time.time()

    engine = SuperEngine()

    #scan 
    scan.retrieve_source_schema(engine)
    scan.analyze_record_distribution(engine)
    scan.clean_database(engine)
    scan.plan_extraction_order(engine)

    #extract schema
    extract.extract_source_schema(engine)
    data.extract_from_salesforce(engine)
    #extract.build_relationships(engine)

    t1 = time.time()

    print "Finished in %s" % (t1-t0)