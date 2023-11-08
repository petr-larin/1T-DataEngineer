clickhouse-client -q "insert into hw.superstore format CSV" < /docker-entrypoint-initdb.d/ds.csv
