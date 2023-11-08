clickhouse-client --input_format_csv_skip_first_lines=1 \
--date_time_input_format=best_effort \
-q "insert into hw.vac format CSV" < /docker-entrypoint-initdb.d/IT_vacancies_full.csv
