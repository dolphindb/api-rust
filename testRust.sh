#!/bin/bash

# cd api_rust firtly

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/api/src/apic


res=/hdd/hdd1/testing/api/result/api-rust/output.txt

rm -f $res

rm -f test.res

cargo test >> test.res

cat test.res | grep "test result" | awk 'BEGIN{sum=0;sum1=0}{ sum+=$4;sum1+=$6}END{print "total",sum+sum1, "passed ",sum,"failed ",sum1}' >>$res

