#!/usr/bin/env python 

import sys
import re
import string
import boto3
import traceback

def usage():
    print >>sys.stderr, "s3ls"
    print >>sys.stderr, "list the s3 buckets"
    print >>sys.stderr, "s3ls [--long] [--prefix=prefix] buckets..."
    print >>sys.stderr, "list the objects in an s3 bucket with a given prefix"
    return 1

def main():
    dolong = 0
    prefix= ''

    buckets = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-l" or arg == "--long":
            dolong = 1
            continue
        match = re.match("--prefix=(.*)", arg)
        if match:
            prefix = match.group(1)
            continue
        buckets.append(arg)

    try:
        s3 = boto3.resource('s3')
        if 0: print s3, dir(s3)
        if len(buckets) == 0:
            for bucket in s3.buckets.all():
                if dolong:
                    print bucket.creation_date, bucket.name
                else:
                    print bucket.name
        else:
            for bucketname in buckets:
                bucket = s3.Bucket(bucketname)
                ls_bucket(bucket, prefix, dolong)
    except:
        e = sys.exc_info()
        print >>sys.stderr, e
        traceback.print_tb(e[2])
        return 1

    return 0

def ls_bucket(bucket, prefix, dolong):
    for k in bucket.objects.filter(Prefix=prefix):
        if 0: print k, dir(k)
        if dolong:
            o = bucket.Object(k.key)
            if 0: print o, dir(o)
            md5 = string.strip(o.e_tag, '"')
            if o.metadata.has_key('user-md5'):
                user_md5 = o.metadata['user-md5']
                if md5 != user_md5:
                    print o.key.encode('utf-8'), 'e_tag=', md5, 'user_md5=', user_md5
                md5 = user_md5 + '*'
            print "%s %12d %s %s" % (o.last_modified, int(o.content_length), md5, o.key.encode('utf-8'))
        else:
            print k.key.encode('utf-8')

sys.exit(main())
