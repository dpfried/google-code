#!/bin/bash
for f in crawls/crawl*
do
  echo -n $f
  echo -ne "\t"
  grep -A 6 'usable repos' $f | tail -n7
done
