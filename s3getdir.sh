#!/usr/bin/env bash
set -e
getargs="-r"
while [ $# -gt 0 ] ; do
    if [ "$1" = "--verbose" ] ; then getargs="$getargs $1"; shift; else break; fi
done
if [ $# != 2 ] ; then exit 1; fi
s3get $getargs "$1" "$2"
