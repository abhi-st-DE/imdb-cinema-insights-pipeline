/* @bruin

name: staging.stg_title_akas
type: duckdb.sql

materialization:
    type: table

tags: [staging]

depends:
    - ingestion.ing_title_akas

columns:
    - name: title_id
      type: string
      description: A tconst, an alphanumeric unique identifier of the title.
      primary_key: true
      checks:
        - name: not_null

    - name: ordering
      type: integer
      description: A number to uniquely identify rows for a given titleId.
      primary_key: true
      checks:
        - name: not_null

    - name: localised_title
      type: string
      description: The localized title.
    
    - name: region
      type: string
      description: The region for this localized title.
    
    - name: language
      type: string
      description: The language of the title.
    
    - name: types
      type: string
      description: One or more of "alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay".

    - name: attributes
      type: string
      description: Additional attributes of the alternative title.

    - name: is_original_title
      type: boolean
      description: Indicates if the title is the original title.

custom_checks:
  - name: row_count_positive
    description: Ensures the staging table is not empty after load
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM staging.stg_title_akas

@bruin */

/*

with deduplicated AS(
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                titleId,
                ordering
            ORDER BY titleId, ordering
        ) AS ROW_NUM
    FROM ingestion.ing_title_akas
)

*/

SELECT DISTINCT ON (titleId, ordering)

    -- Unique identifier of the title.
    CAST(titleId AS STRING) AS title_id,

    -- Ordering of the title.
    CAST(ordering AS INTEGER) AS ordering,

    -- Titles information. 
    CAST(title AS STRING) AS localised_title,
    CAST(region AS STRING) AS region,
    CAST(language AS STRING) AS language,
    CAST(types AS STRING) AS types,
    CAST(attributes AS STRING) AS attributes,

    -- Original Title or not.
    CAST(isOriginalTitle AS BOOLEAN) AS is_original_title
FROM ingestion.ing_title_akas
WHERE titleId IS NOT NULL
AND ordering IS NOT NULL;