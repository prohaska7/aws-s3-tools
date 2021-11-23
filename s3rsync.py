#!/usr/bin/env python3

import sys
import os
import hashlib
import boto3
import tempfile

def help():
    print('[-v|--verbose] [-n|--dryrun] [--delete] [--file-cmp] SRC DEST', file=sys.stderr)
    print('[-v|--verbose] (print debug info)', file=sys.stderr)
    print('[-n|--dryrun]  (find differences but do not execute copy or remove operations)', file=sys.stderr)
    print('[--delete]     (delete objects in DEST that are not in SRC)', file=sys.stderr)
    print('[--file-cmp]   (compare source and dest files)', file=sys.stderr)
    print('SRC DEST       (s3://BUCKET/KEY-PREFIX or local path)', file=sys.stderr)
    return 1

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
    def get_keys(self):
        keys = []
        for path,dirs,files in os.walk(self.dname):
            # print('walk', path,dirs, files)
            for f in files:
                if path == '.':
                    fname = f
                else:
                    fname = path + '/' + f
                if fname.startswith(self.dname + '/'):
                    fname = fname[len(self.dname)+1:]
                keys.append(fname)
        return keys
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
    def copy_data(self, skey, sname, dest, dkey, dname):
        # make dirs
        newdir = os.path.dirname(dname)
        if newdir != '':
            os.makedirs(newdir, exist_ok=True)
        # copy data
        with open(sname, 'rb') as infile, open(dname, 'wb') as outfile:
            n = 1024*1024
            b = infile.read(n)
            while b:
                outfile.write(b)
                b = infile.read(n)
    def remove_key(self, key):
        fname = self.get_full_name(key)
        os.remove(fname)
    def get_temp_file(self, key):
        return self.get_full_name(key)

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
        if md5 is None:
            print('get_md5 is None', pr(self.get_full_name(key)))
            assert md5 is not None
        return md5
    def upload_data(self, sname, dkey):
        obj = self.bucket.Object(dkey)
        smd5 = compute_md5(sname)
        with open(sname, 'rb') as f:
            obj.upload_fileobj(f, ExtraArgs={'Metadata': {'user-md5': smd5 }})
    def download_data(self, skey, dname):
        obj = self.bucket.Object(skey)
        # make dirs
        newdir = os.path.dirname(dname)
        if newdir != '':
            os.makedirs(newdir, exist_ok=True)
        # download to file
        with open(dname, 'wb') as f:
            obj.download_fileobj(f)
        # verify size
        st = os.stat(dname)
        assert st.st_size == obj.content_length
        # verify md5
        dmd5 = compute_md5(dname)
        assert 'user-md5' in obj.metadata
        smd5 = obj.metadata['user-md5']
        assert smd5 == dmd5
    def copy_data(self, skey, sname, dest, dkey, dname):
        global use_resource_copy
        if use_resource_copy:
            copysource = { 'Bucket': self.bucket_name, 'Key': skey }
            dest.bucket.copy(CopySource=copysource, Key=dkey)
        else:
            client = boto3.client('s3')
            client.copy_object(Bucket=dest.bucket_name, Key=dkey,
                               CopySource={'Bucket': self.bucket_name, 'Key': skey})

    def remove_key(self, key):
        fkey = self.get_full_key(key)
        obj = self.bucket.Object(fkey)
        resp = obj.delete()
        global verbose
        if verbose: print(resp)

    def get_temp_file(self, key):
        fkey = self.get_full_key(key)
        if self.tempname is not None:
            os.unlink(self.tempname)
            self.tempname = None
        tempf, self.tempname = tempfile.mkstemp()
        os.close(tempf)
        global verbose
        if verbose: print('download', self.get_full_name(key), self.tempname)
        self.download_data(fkey, self.tempname)
        return self.tempname

dryrun = False
delete_flag = False
verbose = False
use_resource_copy = False
do_file_cmp = False

def main():
    global dryrun, delete_flag, verbose, use_resource_copy, do_file_cmp
    args = []
    for arg in sys.argv[1:]:
        if arg == '-v' or arg == '--verbose':
            verbose = True
        if arg == '-n' or arg == '--dryrun' or arg == '--dry-run':
            dryrun = True
        if arg == '--delete':
            delete_flag = True
        if arg == '--use-resource-copy':
            use_resource_copy = True
        if arg == '--file-cmp':
            do_file_cmp = True
        if arg == '-h' or arg == '--help':
            return help()
        if not arg.startswith('-'):
            args.append(arg)
    assert len(args) == 2
    src = get_repo(args[0])
    if verbose: print('src=', src)
    dest = get_repo(args[1])
    if verbose: print('dest=', dest)
    diff(src, dest)
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

def diff(src, dest):
    global verbose, do_file_cmp
    
    src_keys = sorted(src.get_keys())
    if False:  print('src_keys=', src_keys)
    dest_keys = sorted(dest.get_keys())
    if False: print('dest_keys=', dest_keys)

    si = di = 0
    while si < len(src_keys) and di < len(dest_keys):
        skey = src_keys[si]
        dkey = dest_keys[di]
        # print('cmp', skey, dkey)
        if skey == dkey:
            # print('check size md5', skey, dkey)
            ssize = src.get_size(skey)
            dsize = dest.get_size(dkey)
            if ssize != dsize:
                if verbose: print('diff size', pr(src.get_full_name(skey)), ssize, pr(dest.get_full_name(dkey)), dsize)
                copy_key(src, skey, dest, dkey)
            else:
                smd5 = src.get_md5(skey)
                dmd5 = dest.get_md5(dkey)
                if smd5 != dmd5:
                    if verbose: print('diff md5', pr(src.get_full_name(skey)), smd5, pr(dest.get_full_name(dkey)), dmd5)
                    copy_key(src, skey, dest, dkey)
                elif do_file_cmp:
                    if not file_cmp(src, skey, dest, dkey):
                        if verbose: print('diff cmp', pr(src.get_full_name(skey)), pr(dest.get_full_name(dkey)))
                        copy_key(src, skey, dest, dkey)
            si += 1
            di += 1
        elif skey < dkey:
            if verbose: print('dest missing', pr(dest.get_full_name(skey)))
            copy_key(src, skey, dest, skey)
            si += 1
        else:
            if verbose: print('src missing', pr(src.get_full_name(dkey)))
            remove_key(dest, dkey)
            di += 1
    while si < len(src_keys):
        skey = src_keys[si]
        if verbose: print('E dest missing', pr(skey))
        copy_key(src, skey, dest, skey)
        si += 1
    while di < len(dest_keys):
        dkey = dest_keys[di]
        if verbose: print('E src missing', pr(dkey))
        remove_key(dest, dkey)
        di += 1

def copy_key(src, skey, dest, dkey):
    global dryrun
    if dryrun:
        print('SKIP copy', pr(src.get_full_name(skey)), pr(dest.get_full_name(dkey)))
        return
    print('copy', pr(src.get_full_name(skey)), pr(dest.get_full_name(dkey)))
    sd = src.get_type() + dest.get_type()
    if sd == 'FF' or sd == 'SS':
        src.copy_data(src.get_full_key(skey), src.get_full_name(skey),
                      dest, dest.get_full_key(dkey), dest.get_full_name(dkey))
    elif sd == 'SF':
        src.download_data(src.get_full_key(skey), dest.get_full_name(dkey))
    elif sd == 'FS':
        dest.upload_data(src.get_full_name(skey), dest.get_full_key(dkey))
    else:
        assert False

def remove_key(repo, key):
    global dryrun, delete_flag
    if dryrun or not delete_flag:
        print('SKIP remove', pr(repo.get_full_name(key)))
        return
    print('remove', pr(repo.get_full_name(key)))
    repo.remove_key(key)

def pr(s):
    try:
        s.encode(); return s
    except:
        t = ''
        for c in s:
            try:
                c.encode(); t += c
            except:
                t += '\\x%x' % (ord(c))
        return t

def compute_md5(fname):
    global verbose
    if verbose: print('compute md5', fname)
    md5 = hashlib.md5()
    with open(fname, 'rb') as f:
        n = 1024*1024
        b = f.read(n)
        while b:
            md5.update(b)
            b = f.read(n)
    return md5.hexdigest()

def file_cmp(src, skey, dest, dkey):
    sfile = src.get_temp_file(skey)
    dfile = dest.get_temp_file(dkey)
    global verbose
    if verbose: print('file cmp', sfile, dfile)
    r = False
    with open(sfile, 'rb') as sf, open(dfile, 'rb') as df:
        n = 1024*1024
        while True:
            sb = sf.read(n)
            db = df.read(n)
            if sb != db:
                r = False
                break
            if not sb:
                r = True
                break
    return r
                                          
if __name__ == '__main__':
    sys.exit(main())
