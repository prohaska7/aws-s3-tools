# The AWS S3 tools
The AWS S3 tools are modeled on the unix directory and file abstraction.
An S3 bucket is like a unix directory, and an S3 key is like a unix file.
One can create and remove buckets, put files into buckets, get files from buckets, and list
the contents of buckets.

These tools include an MD5 checksum of the data that is stored the S3 object's metadata
(user-md5).

## Boto
The python boto3 library is the interface to the Amazon Web Services.
The library can be found at https://github.com/boto.

### Installing boto
See the boto3 README.

Ubuntu 20.10:
    $ apt install python3-boto3

## Installing the AWS tools
The makefile installs the tools in the user's bin directory ($HOME/bin).

    $ make install

## Setting up AWS access keys
    export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
    export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY

## API

### List the S3 buckets
    $ s3ls

### Make an S3 bucket
    $ s3mb s3://BUCKET
    $ aws s3 mb s3://BUCKET

### List the keys in an S3 bucket
    $ s3ls -l BUCKET

## List the keys with a given PREFIX
    $ s3ls --prefix=PREFIX BUCKET

### List the newest keys
    $ s3ls -l BUCKET | sort | tail

### Compute the sum of the key sizes in a bucket
    $ s3ls -l BUCKET | gawk -M '{s+=$3}END{print s};
    $ sels -l --prefix=PREFIX BUCKET | gawk -M '{s+=$3}END{print s}'

### Put a file into an S3 bucket
    $ s3put BUCKET KEY FILE

### Recursively put a directory into an S3 bucket
    $ s3put -r BUCKET directory

### Get an object from an S3 bucket
    $ s3get BUCKET KEY OUTFILE

### Recursively get a directory from an S3 Bucket
    $ s3get -r BUCKET directory

### Delete a key from an S3 bucket
    $ s3rm s3://BUCKET/KEY
    $ aws s3 rm s3://BUCKET/KEY

### Sync the contents of an S3 bucket with a local directory
    $ s3rsync LOCALDIR s3://BUCKET/LOCALDIR

s3rsync uses the object size and the user MD5 checksum to determine
if the objects differ.

### Sync the contents of a local directory with an S3 bucket
    $ s3rsync s3://BUCKET/LOCALDIR LOCALDIR

### Create a snapshot of an S3 bucket
    $ aws s3 mb s3://NEWBUCKET
    $ s3snap s3://OLDBUCKET s3://NEWBUCKET

Snapshots may be useful as a backup.
