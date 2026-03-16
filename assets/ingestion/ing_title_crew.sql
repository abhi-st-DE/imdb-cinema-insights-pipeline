/* @bruin

name: ingestion.ing_title_crew
type: duckdb.sql
connection: duckdb-default

materialization:
  type: table

tags: [ingestion]

@bruin */

SELECT * 
FROM read_csv('{{ var.url }}{{ var.title_crew }}', sep='\t', header=True, compression='gzip', nullstr='\N');