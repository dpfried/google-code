#!/bin/bash
for f in crawls/crawl*
do
  echo -n $f
  echo -ne "\t"
  grep -A 12 'usable repos' $f | tail -n13
done
