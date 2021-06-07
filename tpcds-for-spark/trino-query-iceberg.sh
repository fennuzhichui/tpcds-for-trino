#!/bin/bash
#查询两遍
for(( times=1;times<=2;times++));
do
 /data/xgimi/hadooop/trino/trino-server-356/bin/trino-cli --server localhost:8080 -f trino-query/iceberg-query-10T.sql >>result.txt 2>query.log
done