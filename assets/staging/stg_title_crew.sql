/* @bruin

name: staging.stg_title_crew
type: duckdb.sql

materialization:
    type: table

tags: [staging]

depends:
    - ingestion.ing_title_crew

columns:
    - name: title_id
      type: string
      description: Alphanumeric unique identifier of the title.
      primary_key: true
      checks:
        - name: not_null

    - name: directors
      type: string
      description: Director(s) of the given title.

    - name: writers
      type: string
      description: Writer(s) of the given title.


custom_checks:
  - name: row_count_positive
    description: Ensures the staging table is not empty after load
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM staging.stg_title_crew

@bruin */

/*

with deduplicated AS(
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                tconst
            ORDER BY tconst
        ) AS ROW_NUM
    FROM ingestion.ing_title_crew
)

*/

SELECT DISTINCT ON (tconst)
    -- unique id
    CAST(tconst AS STRING) AS title_id,

    -- directors
    CAST(directors AS STRING) AS directors,

    -- writers
    CAST(writers AS STRING) AS writers
FROM ingestion.ing_title_crew
WHERE tconst IS NOT NULL;