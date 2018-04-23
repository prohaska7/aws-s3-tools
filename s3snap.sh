#!/bin/sh
set -e

# copy all of the objects from a source bucket to a destination bucket.
# implementation: use aws cli utility.

# just use the default multipart threshold
# aws configure set default.s3.multipart_threshold 1TB

# copy all object from source bucket to destination bucket
aws s3 cp --recursive $*

# aws s3 multipart copy has two problems.  first, it sometimes does not copy the
# metadata (see 'aws s3 cp help' metadata-directive documentation).  this includes
# any user md5 checksum that is stored in the metadata.  second, it may change the
# e_tag attribute, which sometimes equals the md5 checksum over the data.
# the s3snap-fixup utility fixes these two cases
s3snap-fixup --size 10000000 $*


