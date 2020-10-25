#!/usr/bin/env python3

import sys
import re
import string
import boto3
import traceback

def usage():
    print("s3ls", file=sys.stderr)
    print("list the s3 buckets", file=sys.stderr)
    print("s3ls [--long] [--prefix=prefix] buckets...", file=sys.stderr)
    print("list the objects in an s3 bucket with a given prefix", file=sys.stderr)
    return 1

def main():
    dolong = False
    prefix= ''

    buckets = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-l" or arg == "--long":
            dolong = True
            continue
        match = re.match("--prefix=(.*)", arg)
        if match:
            prefix = match.group(1)
            continue
        buckets.append(arg)

    try:
        s3 = boto3.resource('s3')
        if len(buckets) == 0:
            for bucket in s3.buckets.all():
                if dolong:
                    print(bucket.creation_date, bucket.name)
                else:
                    print(bucket.name)
        else:
            for bucketname in buckets:
                bucket = s3.Bucket(bucketname)
                ls_bucket(bucket, prefix, dolong)
    except:
        e = sys.exc_info()
        print(e, file=sys.stderr)
        traceback.print_tb(e[2])
        return 1

    return 0

def ls_bucket(bucket, prefix, dolong):
    for k in bucket.objects.filter(Prefix=prefix):
        if dolong:
            o = bucket.Object(k.key)
            md5 = o.e_tag.strip('"')
            if 'user-md5' in o.metadata:
                user_md5 = o.metadata['user-md5']
                if md5 != user_md5:
                    print(o.key.encode('utf-8'), 'e_tag=', md5, 'user_md5=', user_md5)
                md5 = user_md5 + '*'
            print("%s %12d %s %s" % (o.last_modified, int(o.content_length), md5, key_str(o.key)))
        else:
            print(key_str(k.key))

def key_str(k):
    try:
        return str(k, encoding='utf-8')
    except:
        return k

if __name__ == '__main__':
    sys.exit(main())
