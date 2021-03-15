#!/usr/bin/env bash
set -e
putargs="-r"
while [ $# -gt 0 ] ; do
    if [ "$1" = "--verbose" ] ; then putargs="$putargs $1"; shift; else break; fi
done
if [ $# != 2 ] ; then exit 1; fi
s3put $putargs "$1" "$2"
