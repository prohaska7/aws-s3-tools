import sys
import re
import boto3

def main():
    verbose = False
    long_flag = False
    prefix = ''
    buckets = []
    for arg in sys.argv[1:]:
        if arg == '-l' or arg == '--long':
            long_flag = True
        match = re.match('--prefix=(.*)', arg)
        if match:
            prefix=match.group(1)
        if not arg.startswith('-'):
            buckets.append(arg)
    s3 = boto3.client('s3')
    if len(buckets) == 0:
        resp = s3.list_buckets()
        if verbose: print(resp)
        for bucket in resp['Buckets']:
            if verbose: print(bucket)
            if long_flag:
                print(bucket['CreationDate'], bucket['Name'])
            else:
                print(bucket['Name'])
    else:
        for name in buckets:
            if verbose: print(name)
            marker = ''
            maxkeys = 1000
            while True:
                resp = s3.list_objects(Bucket=name, MaxKeys=maxkeys, Marker=marker, Prefix=prefix)
                if verbose: print(resp)
                if 'Contents' not in resp:
                    break
                for key in resp['Contents']:
                    if verbose: print(key)
                    if long_flag:
                        print(key['LastModified'], key['Size'], key['Key'])
                    else:
                        print(key['Key'])
                    marker = key['Key']
                if False and not resp['IsTruncated']:
                    break

if __name__ == '__main__':
    sys.exit(main())
