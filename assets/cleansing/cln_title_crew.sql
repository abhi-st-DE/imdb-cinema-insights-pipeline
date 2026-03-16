/* @bruin

name: cleansing.cln_title_crew
type: duckdb.sql

materialization:
    type: table

tags: [cleansing]

depends:
    - staging.stg_title_crew
    - cleansing.cln_title_basics

columns:
    - name: title_id
      type: string
      primary_key: true
      checks:
        - name: not_null
    
    - name: directors
      type: string
      description: "nconsts of the directors of the title (comma separated)"

    - name: writers
      type: string
      description: "nconsts of the writers of the title (comma separated)"

custom_checks:
    - name: row_count_positive
      description: "Ensures the table is not empty after cleansing"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_title_crew;

    - name: directors_not_null
      description: "Ensures directors and writers are not null"
      value: 0
      query: |
        SELECT COUNT(*) FROM cleansing.cln_title_crew WHERE directors IS NULL AND writers IS NULL;

@bruin */

SELECT
    c.title_id,
    c.directors,
    c.writers
FROM
    staging.stg_title_crew c
INNER JOIN
    cleansing.cln_title_basics t
ON
    t.title_id = c.title_id
WHERE 
    directors IS NOT NULL 
    AND writers IS NOT NULL