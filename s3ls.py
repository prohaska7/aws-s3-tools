#!/usr/bin/env python3

import sys
import string
import boto3
import traceback

def usage():
    print("s3ls", file=sys.stderr)
    print("list the s3 buckets", file=sys.stderr)
    print("s3ls [--long] buckets...", file=sys.stderr)
    print("list the objects in an s3 bucket with a given prefix", file=sys.stderr)
    return 1

dolong = False
verbose = False

def main():
    global dolong, verbose
    args = []
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "-?" or arg == "--help":
            return usage()
        if arg == "-l" or arg == "--long":
            dolong = True
            continue
        if arg == '--verbose':
            verbose = True
            continue
        args.append(arg)

    try:
        s3 = boto3.resource('s3')
        if len(args) == 0:
            for bucket in s3.buckets.all():
                if dolong:
                    print(bucket.creation_date, bucket.name)
                else:
                    print(bucket.name)
        else:
            for path in args:
                bucketname, keyprefix = s3uri(path)
                if bucketname == '':
                    print('unknown bucketname in', path, file=sys.stderr)
                    return 1
                bucket = s3.Bucket(bucketname)
                ls_bucket(bucket, keyprefix)
    except:
        e = sys.exc_info()
        print(e, file=sys.stderr)
        traceback.print_tb(e[2])
        return 1

    return 0

def s3uri(uri):
    ''' returns (bucketname, keyprefix) '''
    if uri.startswith('s3://'):
        uri = uri[5:]
        f = uri.split('/')
        return (f.pop(0), '/'.join(f))
    else:
        return (uri, '')

def ls_bucket(bucket, prefix):
    global dolong
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
