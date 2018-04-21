#!/usr/bin/env python

# the aws cli cp utility sometimes does not copy the metadata from the source
# object to the destination object. since the metadata includes the user-md5
# checksum, the destination objects may not include the checksum.  in addition,
# if the source object did not have a user-md5 checksum, the e_tag is used for
# data validation.  unfortunately, the aws cli cp utility sometimes changes the
# e_tag in the destination object to be different than the source e_tag.  this
# change is not an md5 checksum, so there is no way to validate the data in
# the destination object.

# s3fixup finds all of the objects in the destination bucket that are missing
# the user-md5 metadata due to the multipart copy operation.  it retrieves the user-md5
# metadata from the source object and copies it the the destination object.  if
# the source object does not have the user-md5 metadata, this utility downloads
# both the source and destination objects, compares them, generates the md5 checksum
# and adds the user-md5 metadata to the destination object.

import sys
import re
import string
import boto3
import traceback
import os
import hashlib
import tempfile

def usage():
    print '[--help] [--verbose] [--prefix=the_prefix] [--size=the_size_limit] src_bucket dest_bucket'

def main():
    prefix = None
    size_limit = None
    buckets = []
    for arg in sys.argv[1:]:
        if arg.startswith('-'):
            if arg == "-h" or arg == "-?" or arg == "--help":
                usage()
                return 1
            match = re.match('^--prefix=(.*)', arg)
            if match:
                prefix = match.group(1)
                continue
            match = re.match('^--size=(.*)', arg)
            if match:
                size_limit = int(match.group(1))
            continue # eat unknown args
        match = re.match('^s3://(.*)', arg)
        if match:
            arg = match.group(1)
        buckets.append(arg)

    if len(buckets) != 2:
        usage()
        return 1

    try:
        s3 = boto3.resource('s3')
        s3client = boto3.client('s3')
        fixup_bucket(s3, s3client, buckets[0], buckets[1], prefix, size_limit)
    except:
        e = sys.exc_info()
        print >>sys.stderr, e
        traceback.print_tb(e[2])
        return 1

    return 0

def fixup_bucket(s3, s3client, src_bucketname, dest_bucketname, prefix, size_limit):
    src_bucket = s3.Bucket(src_bucketname)
    dest_bucket = s3.Bucket(dest_bucketname)
    for k in dest_bucket.objects.filter(Prefix=prefix):
        fixup_object(s3client, k, src_bucketname, src_bucket, dest_bucketname, dest_bucket, size_limit)

def fixup_object(s3client, k, src_bucketname, src_bucket, dest_bucketname, dest_bucket, size_limit):
    o = dest_bucket.Object(k.key)
    if not 'user-md5' in o.metadata:
        if 0: print 'user-md5 missing in', dest_bucketname, k.key
        srco = src_bucket.Object(k.key)
        if not 'user-md5' in srco.metadata:
            if 0: print '+ user-md5 missing in', src_bucketname, k.key
            print 'skipping', k.key, o.content_length

            if size_limit is not None and o.content_length > size_limit:
                return
            
            # diff src and dest objects
            srcf, srcfile = tempfile.mkstemp()
            os.close(srcf)
            srco.download_file(srcfile)

            destf, destfile = tempfile.mkstemp()
            os.close(destf)
            o.download_file(destfile)

            if not file_cmp(srcfile, destfile):
                return

            # compute md5 checksums
            src_md5 = compute_md5(srcfile)
            dest_md5 = compute_md5(destfile)
            if src_md5 != dest_md5:
                print k.key, 'src md5=', src_md5, 'dest md5=', dest_md5
                return

            os.unlink(srcfile)
            os.unlink(destfile)
            
            # compare with e_tag
            src_tag = string.strip(srco.e_tag, '"')
            dest_tag = string.strip(o.e_tag, '"')
            if src_md5 != src_tag:
                print k.key, 'src etag=', src_tag, 'dest etag=', dest_tag
                return
            
            # update dest object metadata with checksum
            print 'fixing', k.key, 'user-md5=', dest_md5
            new_metadata = srco.metadata.copy()
            new_metadata.update(o.metadata)
            new_metadata['user-md5'] = dest_md5
            s3client.copy_object(Bucket=dest_bucketname, Key=k.key, CopySource=dest_bucketname + '/' + k.key, Metadata=new_metadata, MetadataDirective='REPLACE')
        else:
            print 'fixing', k.key, 'src-md5=', srco.metadata['user-md5']
            new_metadata = srco.metadata.copy()
            new_metadata.update(o.metadata)
            s3client.copy_object(Bucket=dest_bucketname, Key=k.key, CopySource=dest_bucketname + '/' + k.key, Metadata=new_metadata, MetadataDirective='REPLACE')

def file_cmp(afile, bfile):
    if 1: print 'real_file_cmp', afile, bfile
    r = 1
    with open(afile, 'rb') as af, open(bfile, 'rb') as bf:
        n = 1024*1024
        while 1:
            ab = af.read(n)
            bb = bf.read(n)
            if ab != bb:
                r = 0
                break
            if not ab:
                break
    return r

def compute_md5(file):
    md5 = hashlib.md5()
    with open(file, 'rb') as f:
        n = 1024*1024
        b = f.read(n)
        while b:
            md5.update(b)
            b = f.read(n)
    return md5.hexdigest()

sys.exit(main())
