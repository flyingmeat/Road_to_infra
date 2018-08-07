#!/bin/bash

for i in `seq $2`; do
  echo "Running test $i..."
  go test -run $1 >> test_result.log
done
success=$(grep -E '^PASS$' test_result.log | wc -l)
echo "$success / $2 passed"
rm test_result.log
