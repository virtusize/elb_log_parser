#!/usr/bin/env python

"""
CopperEgg Agent.

Usage:
  elb_log_parser -h | --help
  elb_log_parser [--dry] [--verbose] --year <yy> --month <mm> --day <dd> | --path <path>

Options:
  -h, --help         Show this help.
  -y, --year <yy>    Year
  -m, --month <mm>   Month of the year
  -d, --day <dd>     Day of month
  -p, --path <path>  Local folder
  --dry              Dry run, do not change anything.
  -v, --verbose      Verbose mode.


Examples:

  elb_log_parser -y 2014 -m 08 -d 01
  elb_log_parser --path ./logs

"""

import numpy as np
from collections import defaultdict

from docopt import docopt
import re
import os
import sys
import tempfile
import csv

import boto.s3.connection

BUCKET_NAME = 'virtusize-production-elb-access-log'
BUCKET_PREFIX = 'AWSLogs/976148039105/elasticloadbalancing/eu-west-1/%s/%s/%s/'

VERBOSE = False
DRY_RUN = False

# http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html
REGEX = re.compile(r'''
    ^
    (?P<ts>\S+)  # timestamp 2014-08-29T23:00:00.081362Z
    \s+
    \S+  # elb production-load-balancer
    \s+
    \S+  # client 198.100.150.192:53731
    \s+
    (?P<backend>(?:[0-9]+(?:\.[0-9]+){3}(?::\d+)?|-)) # backend 10.0.12.11:80
    \s+
    \S+  # request_processing_time 0.000044
    \s+
    (?P<backend_time>\S+)  # backend_processing_time
    \s+
    \S+  # response_processing_time
    \s+
    \S+  # elb_status_code
    \s+
    \S+  # backend_status_code
    \s+
    \S+  # received_bytes
    \s+
    \S+  # sent_bytes
    \s+
    "(?P<request>[^"]+)\s+HTTP/\d.\d"
    $
''', re.VERBOSE)

  # \"(?P<cmd>\w+)\s+(?P<url>\S+)\s+HTTP\/(?P<http>\d.\d)\" # "request"


def process(line):
    m = REGEX.match(line)
    if m:
        return m.groupdict()
    else:
        print "Unexpected log format"
        return None


def read_lines_s3(yy, mm, dd):
    c = boto.connect_s3()
    bucket = c.get_bucket(BUCKET_NAME, validate=False)
    keys = [k for k in bucket.list(prefix=BUCKET_PREFIX % (yy, mm, dd))
            if k.name.find('<?xml version=') == -1]
    total = len(keys)
    for i, k in enumerate(keys):
        with tempfile.SpooledTemporaryFile(mode='rw') as tmp:
            print "[%i/%i] Reading key %s" % (i + 1, total, k.name)
            k.get_file(tmp)
            tmp.seek(0)
            for line in tmp:
                yield line


def read_lines_local(dir):
    for root, dirs, files in os.walk(dir):
        for name in files:
            if name.endswith('.log'):
                sys.stderr.write("Reading %s\n" % name)
                with open(os.path.join(root, name), 'r') as file_handler:
                    for line in file_handler:
                        yield line


def main():
    global VERBOSE
    global DRY_RUN

    arguments = docopt(__doc__, version='ELB parser')

    yy = arguments.get('--year')
    mm = arguments.get('--month')
    dd = arguments.get('--day')
    path = arguments.get('--path')

    if arguments.get('--dry'):
        print 'DRY mode is not implemented yet.'
        DRY_RUN = True

    if arguments.get('--verbose'):
        print 'VERBOSE mode.'
        VERBOSE = True

    if path:
        print "Local mode: reading %s directory" % path
        lines = read_lines_local(path)
    elif yy and mm and dd:
        print "Reading data from s3://%s" % BUCKET_NAME
        lines = read_lines_s3(yy, mm, dd)
    else:
        print "CLI parsing error"
        return 1

    requests = defaultdict(list)
    for line in lines:
        req = process(line)
        if req:
            requests[req['request']].append(float(req['backend_time']))
        elif VERBOSE:
            print line

    print "Processing data"

    fields = ['url', 'average', 'max', 'min', 'mean', 'median']

    report = defaultdict(list)
    for r, t in requests.iteritems():
        report[r].append(np.average(t))
        report[r].append(np.max(t))
        report[r].append(np.min(t))
        report[r].append(float(np.mean(t)))
        report[r].append(np.median(t))

    if path:
        csv_filename = "report.csv"
    else:
        csv_filename = "report_%s_%s_%s.csv" % (yy, mm, dd)

    print "Writing %s" % csv_filename

    with open(csv_filename, 'wb+') as csv_file:
        w = csv.writer(csv_file, delimiter=";", quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(fields)
        w.writerows([[key]+value for key, value in report.items()])

if __name__ == '__main__':
    main()
