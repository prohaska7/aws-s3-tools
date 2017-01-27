#!/usr/bin/env python 

import sys
import re
import string
import boto.s3.connection

def usage():
    print >>sys.stderr, "s3ls [--verbose]"
    print >>sys.stderr, "list the s3 buckets"
    print >>sys.stderr, "s3ls [--verbose] [--long] [--prefix=prefix] [--select=field,field...] buckets..."
    print >>sys.stderr, "list the objects in an s3 bucket with a given prefix"
    return 1

def main():
    verbose = 0
    long = 0
    prefix= ''

    select = []
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
            long = 1
            continue
        match = re.match("--select=(.*)", arg)
        if match:
            fields = match.group(1);
            for f in fields.split(','):
                select.append(f)
            continue
        match = re.match("--prefix=(.*)", arg)
        if match:
            prefix = match.group(1)
            continue
        buckets.append(arg)

    try:
        s3 = boto.s3.connection.S3Connection()
        if verbose:
            print s3
        ls_bucket(s3, buckets, prefix, select, long, verbose)
    except:
        print >>sys.stderr, sys.exc_info()
        return 1

    return 0

def print_dict(x):
    print x
    for k in x.__dict__.keys():
        print "\t", k, "=", x.__dict__[k]

def print_key(k, select):
    d = k.__dict__
    for f in select:
        if d.has_key(f):
            print d[f].encode('utf-8'),
    print

def print_bucket(bucket, prefix, select, long, verbose):
    if verbose:
        print_dict(bucket)
    for k in bucket.list(prefix=prefix):
        # k.open()
        if verbose:
            print_dict(k)
            print k.get_acl()
            print 'user-md5=', k.get_metadata('user-md5')
        elif long:
            md5 = string.strip(k.etag, '"')
            user_md5 = k.get_metadata('user-md5')
            print md5, user_md5
            if user_md5 is not None:
                assert md5 == user_md5
                md5 = user_md5
            print "%s %12d %s %s" % (k.last_modified, int(k.size), md5, k.name)
        else:
            print_key(k, select)
        # k.close()

def ls_bucket(s3, buckets, prefix, select, long, verbose):
    if len(buckets) == 0:
        buckets = s3.get_all_buckets()
        if verbose:
            print_dict(buckets)
        if len(select) == 0:
            if long:
                select = ['creation_date','name']
            else:
                select = ['name']
        for bucket in buckets:
            if verbose:
                print_dict(bucket)
            d = bucket.__dict__
            for f in select:
                if d.has_key(f):
                    print d[f],
            print
    else:
        if len(select) == 0:
            select = ['name']
        for bucketname in buckets:
            bucket = s3.get_bucket(bucketname)
            print_bucket(bucket, prefix, select, long, verbose)


sys.exit(main())
