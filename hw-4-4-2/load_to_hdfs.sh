hdfs dfs -mkdir /user/peter/db
hdfs dfs -mkdir /user/peter/db/cust
hdfs dfs -mkdir /user/peter/db/org
hdfs dfs -mkdir /user/peter/db/ppl
hdfs dfs -put cust_gr.csv /user/peter/db/cust
hdfs dfs -put org_gr.csv /user/peter/db/org
hdfs dfs -put ppl_gr.csv /user/peter/db/ppl