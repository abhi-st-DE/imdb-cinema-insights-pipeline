/* @bruin

name: staging.stg_title_principals
type: duckdb.sql

materialization:
    type: table

tags: [staging]

depends:
    - ingestion.ing_title_principals

columns:
    - name: title_id
      type: string
      description: Alphanumeric identifier of the title.
      primary_key: true
      checks:
        - name: not_null

    - name: ordering
      type: integer
      description: A number to uniquely identify rows for a given titleId.
      checks:
        - name: not_null

    - name: person_id
      type: string
      description: Alphanumeric unique identifier of the name/person.
      primary_key: true
      checks:
        - name: not_null

    - name: category
      type: string
      description: The category of job that person was in.

    - name: job_title
      type: string
      description: The specific job title if applicable, else \N.

    - name: characters
      type: string
      description: The name of the character played if applicable, else \N.


custom_checks:
  - name: row_count_positive
    description: Ensures the staging table is not empty after load
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM staging.stg_title_principals

@bruin */


/*

with deduplicated as (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                tconst, nconst, ordering
            ORDER BY tconst, nconst, ordering
        ) AS ROW_NUM
    FROM ingestion.ing_title_principals
)

*/

SELECT DISTINCT ON (tconst, nconst, ordering)
    -- unique title identifier
    CAST(tconst AS STRING) AS title_id,

    -- ordering of the person in the title
    CAST(ordering AS INTEGER) AS ordering,

    -- unique person identifier
    CAST(nconst AS STRING) AS person_id,

    -- category of job that person was in
    CAST(category AS STRING) AS category,

    -- specific job title if applicable, else \N
    CAST(job AS STRING) AS job_title,

    -- name of the character played if applicable, else \N
    CAST(characters AS STRING) AS characters

FROM ingestion.ing_title_principals
WHERE tconst IS NOT NULL
AND nconst IS NOT NULL
AND ordering IS NOT NULL;
