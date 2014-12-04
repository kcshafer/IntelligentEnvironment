import os
import sys
import time

from smartsandbox.extract import Scanner


def execute():
    t0 = time.time()
    scanner = Scanner()
    scanner.retrieve_source_schema()
    scanner.analyze_record_distribution()
    t1 = time.time()

    print "Finished in %s" % (t1-t0)