#!/usr/bin/env bash
md5 $* | awk 'NF == 4{print $4," ", substr($2,2,length($2)-2));}'
