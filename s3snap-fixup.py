#!/usr/bin/env python3

# the aws cli cp utility sometimes does not copy the metadata from the source
# object to the destination object. since the metadata includes the user-md5
# checksum, the destination objects may not include the checksum.  in addition,
# if the source object did not have a user-md5 checksum, the e_tag is used for
# data validation.  unfortunately, the aws cli cp utility sometimes changes the
# e_tag in the destination object to be different than the source e_tag.  this
# change is not an md5 checksum, so there is no way to validate the data in
# the destination object.

# s3snap-fixup finds all of the objects in the destination bucket that are missing
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

verbose = 0

def usage():
    print('[--help] [--verbose] [--size=THE_SIZE_LIMIT] SRC DEST', file=sys.stderr)

class LocalRepo:
    def __init__(self, dname):
        self.dname = dname
    def __str__(self):
        if self.dname.startswith('/'):
            return self.dname
        else:
            return os.getcwd() + '/' + self.dname
    def get_type(self):
        return 'F'
    def get_size(self, key):
        fkey = self.get_full_key(key)
        return os.stat(fkey).st_size
    def get_md5(self, key):
        fkey = self.get_full_key(key)
        return compute_md5(fkey)
    def get_full_key(self, key):
        return self.dname + '/' + key
    def get_full_name(self, key):
        fkey = self.get_full_key(key)
        if fkey.startswith('/'):
            return fkey
        else:
            return os.getcwd() + '/' + fkey

class S3Repo:
    def __init__(self, bucket_name, prefix):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket_name)
        self.tempname = None
    def __del__(self):
        if self.tempname is not None:
            os.unlink(self.tempname)
            self.tempname = None
    def __str__(self):
        s3name = 's3://' + self.bucket_name
        if self.prefix != '':
            s3name += '/' + self.prefix
        return s3name
    def get_type(self):
        return 'S'
    def get_keys(self):
        keys = []
        filter_prefix = self.prefix
        if filter_prefix !=  '':
            filter_prefix += '/'
        for k in self.bucket.objects.filter(Prefix=filter_prefix):
            key = k.key
            if self.prefix != '' and key.startswith(self.prefix + '/'):
                key = key[len(self.prefix)+1:]
            keys.append(key)
        return keys
    def get_full_key(self, key):
        if self.prefix != '':
            return self.prefix + '/' + key
        else:
            return key
    def get_full_name(self, key):
        return 's3://' + self.bucket_name + '/' + self.get_full_key(key)
    def get_size(self, key):
        obj = self.bucket.Object(self.get_full_key(key))
        return obj.content_length
    def get_md5(self, key):
        obj = self.bucket.Object(self.get_full_key(key))
        md5 = None
        if 'user-md5' in obj.metadata:
            md5 = obj.metadata['user-md5']
        else:
            md5 = None
        return md5
    def set_md5(self, key, md5):
        full_key = self.get_full_key(key)
        global verbose
        if verbose: print('set_md5', full_key, md5)
        obj = self.bucket.Object(full_key)
        obj.metadata.update({'user-md5': md5})
        obj.copy_from(CopySource={'Bucket': self.bucket_name, 'Key': full_key}, Metadata=obj.metadata, MetadataDirective='REPLACE')

def main():
    size_limit = None
    buckets = []
    for arg in sys.argv[1:]:
        if arg.startswith('-'):
            if arg == "-h" or arg == "-?" or arg == "--help":
                usage()
                return 1
            if arg == '-v' or arg == '--verbose':
                global verbose
                verbose = 1
                continue
            match = re.match('^--size=(.*)', arg)
            if match:
                size_limit = int(match.group(1))
            continue # eat unknown args
        buckets.append(get_repo(arg))

    if len(buckets) != 2:
        print(len(buckets))
        usage()
        return 1

    try:
        fixup_bucket(buckets[0], buckets[1], size_limit)
    except:
        e = sys.exc_info()
        print(e, file=sys.stderr)
        traceback.print_tb(e[2])
        return 1

    return 0

def get_repo(name):
    if name.startswith('s3://'):
        name = name[5:]
        f = name.split('/')
        bucket_name = f.pop(0)
        prefix = '/'.join(f)
        return S3Repo(bucket_name, prefix)
    else:
        return LocalRepo(name)

def fixup_bucket(src, dest, size_limit):
    if dest.get_type() != 'S':
        return
    for key in dest.get_keys():
        md5 = dest.get_md5(key)
        if md5 is None:
            fixup_object(key, src, dest, size_limit)

def fixup_object(key, src, dest, size_limit):
    global verbose
    if verbose: print('fixup_object', dest.get_full_name(key))

    # verify src_size == dest_size
    try:
        src_size = src.get_size(key)
    except:
        print('ERROR cant get size', src.get_full_name(key), file=sys.stderr)
        return
    try:
        dest_size = dest.get_size(key)
    except:
        print('ERROR cant get size', dest.get_full_name(key), file=sys.stderr)
        return
    if src_size != dest_size:
        print('ERROR size diff', src.get_full_name(key), src_size, dest.get_full_name(key), dest_size, file=sys.stderr)
        return

    # filter large files
    if size_limit is not None and src_size > size_limit:
        print('ERROR size', src.get_full_name(key), src_size, file=sys.stderr)
        return

    # verify src md5 exists
    try:
        src_md5 = src.get_md5(key)
    except:
        src_md5 = None
    if src_md5 is None:
        # TODO download and compare
        print('ERROR src md5 missing', src.get_full_name(key), file=sys.stderr)
        return

    # set dest md5 == src md5
    dest.set_md5(key, src_md5)

def file_cmp(afile, bfile):
    if verbose: print('real_file_cmp', afile, bfile)
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

if __name__ == '__main__':
    sys.exit(main())
