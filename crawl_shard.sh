#!/bin/bash

shard=$1

python -u crawl.py project_list.txt \
  --out_fname crawls/crawl_shard-${shard}.csv \
  --shard $shard \
  | tee crawls/crawl_shard-${shard}.out
