#!/usr/bin/env bash
set -e
if [ $# != 2 ] ; then exit 1; fi
bucket=$1
files=$(find $2 -type f -print)
for f in $files; do
    if [ -f $f ] ; then
        s3put $bucket $f $f
    fi
done
