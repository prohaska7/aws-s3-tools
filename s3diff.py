#!/usr/bin/env python
import sys
import os
import boto3
import traceback
import hashlib
import string
import tempfile
class MD(object):
    def __init__(self, name, size, digest):
        self.name = name
        self.size = size
        self.digest = digest
        self.obj = None
verbose = 0
def main():
    global verbose
    metaonly = 0
    ignore_md5 = 0
    args = []
    for a in sys.argv[1:]:
        if a == '-v' or a == '--verbose':
            verbose = 1
            continue
        if a == '-m' or a == '--meta':
            metaonly = 1
            continue
        if a == '--ignore-md5':
            ignore_md5 = 1
            continue
        args.append(a)
    
    assert len(args) == 2
    bucket_name = args[0]
    prefix = args[1]

    try:
        # get all local file names in a directory
        if verbose: print 'get local'
        local_objs = get_local_files(prefix, ignore_md5)

        # get all key names from s3
        if verbose: print 'get s3'
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        s3_objs = get_s3_keys(s3, bucket, prefix)

        # diff
        diff('local', local_objs, 's3', s3_objs, metaonly, ignore_md5)
    except:
        e = sys.exc_info()
        print >>sys.stderr, 'main', e
        traceback.print_tb(e[2])
        pass
    return 0
def diff(astr, a_objs, bstr, b_objs, metaonly, ignore_md5):
    if verbose: print 'sort', astr
    a = a_objs.keys()
    a.sort()
    if verbose: print 'sort', bstr
    b = b_objs.keys()
    b.sort()
    if verbose: print 'diff'
    ai = bi = 0
    while ai < len(a) and bi < len(b):
        if verbose: print 'diff', ai, a[ai], bi, b[bi]
        if a[ai] == b[bi]:
            k = a[ai]
            amd = a_objs[k]
            bmd = b_objs[k]
            if amd.size != bmd.size:
                print 'size', k, amd.size, bmd.size
            elif not ignore_md5 and amd.digest != bmd.digest:
                print 'md5', k, a_objs[k].digest, b_objs[k].digest
            elif not metaonly:
                if not file_cmp(amd, bmd):
                    print 'cmp', amd.key, bmd.key
            ai += 1
            bi += 1
        elif a[ai] < b[bi]:
            print bstr, 'missing', a[ai]
            ai += 1
        else:
            print astr, 'missing', b[bi]
            bi += 1
    while ai < len(a):
        print bstr, 'missing', a[ai]
        ai += 1
    while bi < len(b):
        print astr, 'missing', b[bi]
        bi += 1
def file_cmp(amd, bmd):
    afile = amd.name
    bfile = bmd.name
    try:
        if amd.obj:
            af, afile = tempfile.mkstemp()
            os.close(af)
            if verbose: print 'download', amd.name, afile
            amd.obj.download_file(afile)
        if bmd.obj:
            bf, bfile = tempfile.mkstemp()
            os.close(bf)
            if verbose: print 'download', bmd.name, bfile
        bmd.obj.download_file(bfile)
        r = real_file_cmp(afile, bfile)
        if amd.obj:
            os.unlink(afile)
        if bmd.obj:
            os.unlink(bfile)
    except:
        if verbose: print 'file cmp except', sys.exc_info()
        try:
            if amd.obj:
                os.unlink(afile)
        except:
            pass
        try:
            if bmd.obj:
                os.unlink(bfile)
        except:
            pass
        raise
    return r
def real_file_cmp(afile, bfile):
    if verbose: print 'real_file_cmp', afile, bfile
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
def get_s3_keys(s3, bucket, prefix):
    s3_keys = {}
    for k in bucket.objects.filter(Prefix=prefix):
        o = bucket.Object(k.key)
        md5_digest = string.strip(o.e_tag, '"')
        if o.metadata.has_key('user-md5'):
            md5_digest = o.metadata['user-md5']
        md = MD(k.key, o.content_length, md5_digest)
        s3_keys[k.key] = md
        md.obj = o
        if verbose: print 'get md5', k.key, md.__dict__
    return s3_keys
def get_local_files(dname, ignore_md5):
    allfiles = {}
    for path,dirs,files in os.walk(dname):
        files.sort()
        for f in files:
            if path != '.':
                fname = path + '/' + f
            else:
                fname = f
            digest = 0
            if not ignore_md5:
                digest = compute_md5(fname)
            allfiles[fname] = MD(fname, os.stat(fname).st_size, digest)
	    if verbose: print 'compute md5', fname, allfiles[fname].__dict__
    return allfiles
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
