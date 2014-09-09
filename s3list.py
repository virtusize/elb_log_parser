#!/usr/bin/env python

import boto
import boto.s3.connection

BUCKET_NAME = 'virtusize-production-elb-access-log'
BUCKET_PREFIX = 'AWSLogs/976148039105/elasticloadbalancing/eu-west-1/%s/%s/%s/'


def main():
    c = boto.connect_s3(access_key, secret_key)
    bucket = c.lookup(BUCKET_NAME)
    try:
        for k in bucket.list(prefix=BUCKET_PREFIX % ('2014', '09', '05')):
            lines = (line for line in k if k.name.find('<?xml version=') == -1)
            for line in lines:
                print line
    except boto.exception.S3ResponseError, e:
        print "Received exception: %s" % (e,)

if __name__ == '__main__':
    main()
