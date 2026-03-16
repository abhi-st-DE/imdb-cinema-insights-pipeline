/* @bruin

name: ingestion.ing_name_basics
type: duckdb.sql
connection: duckdb-default

materialization:
  type: table

tags: [ingestion]

@bruin */

SELECT * 
FROM read_csv('{{ var.url }}{{ var.name_basics }}', sep='\t', header=True, compression='gzip', nullstr='\N');