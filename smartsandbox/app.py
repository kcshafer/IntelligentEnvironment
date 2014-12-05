import os
import sys
import time

from smartsandbox.database import SuperEngine
from smartsandbox import scan

db = 'postgresql://localhost/smart_sandbox'

def execute():
    t0 = time.time()

    engine = SuperEngine()

    scan.retrieve_source_schema(engine)
    scan.analyze_record_distribution(engine)
    t1 = time.time()

    print "Finished in %s" % (t1-t0)