/* @bruin

name: ingestion.ing_title_principals
type: duckdb.sql
connection: duckdb-default

materialization:
  type: table

tags: [ingestion]

@bruin */

SELECT * 
FROM read_csv('{{ var.url }}{{ var.title_principals }}', sep='\t', header=True, compression='gzip', nullstr='\N');