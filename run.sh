#!/bin/bash

export ENV_DELIM=,
export ENV_COMPRESSION='zstd(1)'

gencsv() {
	printf '%s,%s,%s\n' \
		2025-09-13T02:02:13.012345Z INFO 'hello, world' \
		2025-09-14T02:02:13.012345Z INFO 'hello world'
}

export ENV_COLUMN_SIZE=2
gencsv | ./rs-splited2parquet | file -

export ENV_COLUMN_SIZE=3
gencsv | ./rs-splited2parquet > ./sample.parquet

rsql --url parquet://sample.parquet -- "
	SELECT
		column_0::TEXT AS timestamp,
		column_1::TEXT AS severity,
		column_2::TEXT AS body
	FROM sample
"

export ENV_COLUMN_SIZE=9
gencsv | ./rs-splited2parquet | file -
