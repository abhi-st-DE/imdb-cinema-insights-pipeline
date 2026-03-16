/* @bruin

name: staging.stg_title_ratings
type: duckdb.sql

materialization:
    type: table

tags: [staging]

depends:
    - ingestion.ing_title_ratings

columns:
    - name: title_id
      type: string
      description: Alphanumeric unique identifier of the title.
      primary_key: true
      checks:
        - name: not_null

    - name: average_rating
      type: float
      description: The average rating of the title.

    - name: num_votes
      type: integer
      description: The number of votes for the title.

custom_checks:
  - name: row_count_positive
    description: Ensures the staging table is not empty after load
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM staging.stg_title_ratings

@bruin */

/*
with deduplicated as (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                tconst
            ORDER BY tconst
        ) AS ROW_NUM
    FROM ingestion.ing_title_ratings
)

*/

SELECT DISTINCT ON (tconst)
    -- unique title identifier
    CAST(tconst AS STRING) AS title_id,

    -- average rating of the title
    CAST(averageRating AS FLOAT) AS average_rating,

    -- number of votes for the title
    CAST(numVotes AS INTEGER) AS num_votes
FROM ingestion.ing_title_ratings
WHERE tconst IS NOT NULL;