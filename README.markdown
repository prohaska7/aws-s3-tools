# The AWS S3 tools
The AWS S3 tools are modeled on the unix directory and file abstraction.
An S3 bucket is like a unix directory, and an S3 key is like a unix file.
The basic operations are:
create and remove a bucket,
put files into a bucket,
get files from a bucket,
and list the contents of a bucket.

These tools
store an MD5 checksum of the data that comprises the S3 object in the
objects metadata (user-md5).

The s3rsync command can be used to sync a local directory/s3 bucket with
another local directory/s3 bucket, including the MD5 checksum of the
data objects.

The s3snap tool creates a snapshot of a source bucket in a destination
bucket. Since the AWS S3 copy tool does not always copy the source metadata,
including the user MD5 checksum of the data, the s3snap-fixup tool is
needed to update the destination objects with the user MD5 checksum.

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
    OR
    $ aws s3 ls

### Make an S3 bucket
    $ s3mb s3://BUCKET
    OR
    $ aws s3 mb s3://BUCKET

### List the keys in an S3 bucket
    $ s3ls -l s3://BUCKET
    OR
    $ aws s3 ls --recursive s3://BUCKET

Includes the creation date and time of the object, the object size, the MD5 checksum on the data, and the key name.

## List the keys with a given PREFIX
    $ s3ls s3://BUCKET/KEY-PREFIX

### List the newest keys
    $ s3ls -l s3://BUCKET | sort | tail

### Compute the sum of the key sizes in a bucket
    $ s3ls -l s3://BUCKET | gawk -M '{s+=$3}END{print s};
    $ s3ls -l s3://BUCKET/KEY-PREFIX | gawk -M '{s+=$3}END{print s}'

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
    OR
    $ aws s3 rm s3://BUCKET/KEY

### Sync the contents of an S3 bucket with a local directory
    $ s3rsync LOCAL-SRC-PATH s3://BUCKET/DEST-PATH

s3rsync uses the object size and the user MD5 checksum to determine
if the objects differ.

### Sync the contents of a local directory with an S3 bucket
    $ s3rsync s3://BUCKET/SRC-PATH LOCAL-DEST-PATH

### Create a snapshot of an S3 bucket
    $ aws s3 mb s3://NEW-BUCKET
    $ s3snap s3://OLD-BUCKET s3://NEW-BUCKET

Snapshots may be useful as a backup.
