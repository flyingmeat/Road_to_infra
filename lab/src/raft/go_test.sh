#!/bin/bash

for i in `seq $1`; do
  echo "Running test $i..."
  go test -run 2A >> test_result.log
done
success=$(grep -E '^PASS$' test_result.log | wc -l)
echo "$success / $1 passed"
rm test_result.log
