/* @bruin

name: staging.stg_name_basics
type: duckdb.sql

materialization:
  type: table

tags: [staging]

depends:
    - ingestion.ing_name_basics

columns:
    - name: person_id
      type: string
      description: Alphanumeric unique identifier of the name/person.
      primary_key: true
      checks:
        - name: not_null

    - name: primary_name
      type: string
      description: Name by which the person is most often credited.
    
    - name: birth_year
      type: integer
      description: Birth year.

    - name: death_year
      type: integer
      description: Death year if applicable, else \N.

    - name: primary_profession
      type: string
      description: The top-3 professions of the person.

    - name: known_for_titles
      type: string
      description: Titles the person is known for.

    
custom_checks:
  - name: row_count_positive
    description: Ensures the staging table is not empty after load
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM staging.stg_name_basics


@bruin */

/*
with deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                nconst
            ORDER BY nconst
        ) AS ROW_NUM
    FROM ingestion.ing_name_basics
)

*/

SELECT DISTINCT ON (nconst)

    -- Unique identifier of the name/person.
    CAST(nconst AS STRING) AS person_id,

    -- Name by which the person is most often credited.
    CAST(primaryName AS STRING) AS primary_name,

    -- Birth year.
    CAST(birthYear AS INTEGER) AS birth_year,

    -- Death year if applicable, else \N.
    CAST(deathYear AS INTEGER) AS death_year,

    -- The top-3 professions of the person.
    CAST(primaryProfession AS STRING) AS primary_profession,

    -- Titles the person is known for.
    CAST(knownForTitles AS STRING) AS known_for_titles,
    
FROM ingestion.ing_name_basics
WHERE nconst IS NOT NULL;