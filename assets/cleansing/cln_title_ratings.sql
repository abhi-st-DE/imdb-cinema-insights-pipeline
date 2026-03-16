/* @bruin

name: cleansing.cln_title_ratings
type: duckdb.sql

materialization:
    type: table

tags: [cleansing]

depends:
    - staging.stg_title_ratings
    - cleansing.cln_title_basics

columns:
    - name: title_id
      type: string
      description: "The primary unique identifier for the title (tt + 7-8 digits)"
      primary_key: true
      checks:
        - name: not_null
    
    - name: average_rating
      type: float
      description: "The weighted average of all individual user ratings"
      checks:
        - name: not_null

    - name: num_votes
      type: integer
      description: "Number of votes the title has received"
      checks:
        - name: not_null

custom_checks:
    - name: row_count_positive
      description: "Ensures the table is not empty after cleansing"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_title_ratings

@bruin */

SELECT 
  r.title_id,
  r.average_rating,
  r.num_votes 
FROM staging.stg_title_ratings r
INNER JOIN cleansing.cln_title_basics t
ON
t.title_id = r.title_id
WHERE 
    r.average_rating IS NOT NULL 
    AND r.num_votes IS NOT NULL
    AND r.title_id LIKE 'tt%';
