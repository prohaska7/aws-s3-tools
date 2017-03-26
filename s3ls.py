#!/usr/bin/env python 

import sys
import re
import string
import boto3
import traceback

def usage():
    print >>sys.stderr, "s3ls [--verbose]"
    print >>sys.stderr, "list the s3 buckets"
    print >>sys.stderr, "s3ls [--verbose] [--long] [--prefix=prefix] buckets..."
    print >>sys.stderr, "list the objects in an s3 bucket with a given prefix"
    return 1

def main():
    verbose = 0
    dolong = 0
    prefix= ''

    buckets = []
    myargs = sys.argv[1:]
    while myargs:
        arg = myargs.pop(0)
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-v" or arg == "--verbose":
            verbose = 1
            continue
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
        if verbose:
            print s3
        ls_bucket(s3, buckets, prefix, dolong, verbose)
    except:
        e = sys.exc_info()
        print >>sys.stderr, e
        traceback.print_tb(e[2])
        return 1

    return 0

def print_dict(x):
    print x
    print x.__dict__
    for k in x.__dict__.keys():
        print "\t", k, "=", x.__dict__[k]

def print_bucket(bucket, prefix, dolong, verbose):
    for k in bucket.objects.filter(Prefix=prefix):
        if verbose:
            print_dict(k)
            # print k.get_acl()
            print k.key
            print k.size
            print k.last_modified
            print k.e_tag
            # print k.content_md5
            # print k.metadata
        elif dolong:
            o = bucket.Object(k.key)
            md5 = string.strip(o.e_tag, '"')
            if o.metadata.has_key('user-md5'):
                user_md5 = o.metadata['user-md5']
                if md5 != user_md5:
                    print o.key, md5, user_md5
                md5 = user_md5 + '*'
            print "%s %12d %s %s" % (o.last_modified, int(o.content_length), md5, o.key.encode('utf-8'))
        else:
            print k.key.encode('utf-8')

def ls_bucket(s3, buckets, prefix, dolong, verbose):
    if len(buckets) == 0:
        for bucket in s3.buckets.all():
            if verbose: print_dict(bucket)
            if dolong:
                print bucket.creation_date, bucket.name
            else:
                print bucket.name
    else:
        for bucketname in buckets:
            bucket = s3.Bucket(bucketname)
            if verbose: print_dict(bucket)
            print_bucket(bucket, prefix, dolong, verbose)


sys.exit(main())
