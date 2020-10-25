# The AWS S3 tools
The AWS S3 tools are modeled on the unix directory and file abstraction.
An S3 bucket is like a unix directory, and an S3 key is like a unix file.
One can create and remove buckets, put files into buckets, get files from buckets, and list
the contents of buckets.   There are some simple but incomplete tools to 
control bucket and key permissions.

## Boto
The boto and boto3 libraries are the python interfaces to the Amazon Web Services.
They can be found at https://github.com/boto.

### Installing boto
See the boto and boto3 README's.

Ubuntu 20.10:
    $ apt install python3-boto3

## Installing the AWS tools
The makefile installs the tools in the user's bin directory.

    $ make install

## Setting up AWS access keys
    export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
    export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY

## API

### List the S3 buckets
    $ s3ls

### Make an S3 bucket
    $ s3mkbucket NEWBUCKET

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
    $ s3putdir BUCKET directory

### Get an object from an S3 bucket
    $ s3get BUCKET KEY OUTFILE

### Recursively get a directory from an S3 Bucket
    $ s3getdir BUCKET directory

### Delete a key from an S3 bucket
    $ s3rm BUCKET KEY

