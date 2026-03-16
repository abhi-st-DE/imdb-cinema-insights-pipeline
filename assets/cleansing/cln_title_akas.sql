/* @bruin

name: cleansing.cln_title_akas
type: duckdb.sql

materialization:
    type: table

tags: [cleansing]

depends:
    - staging.stg_title_akas
    - cleansing.cln_title_basics

columns:
    - name: title_id
      type: string
      description: "The id of the title"
      primary_key: true
      checks:
        - name: not_null

    - name: region
      type: string
      description: "The region of the title"

    - name: language
      type: string
      description: "The language of the title"

    - name: localised_title
      type: string
      description: "The localised title."

    - name: ordering
      type: integer
      description: "A number to uniquely identify rows for a given titleId."

    - name: is_original_title
      type: boolean
      description: "Whether the title is the original title"

    - name: attributes
      type: string
      description: "The attributes of the title"

    - name: types
      type: string
      description: "The types of the title like 'imdb display title', etc."

custom_checks:
    - name: row_count_positive
      description: Ensures the cleansing table is not empty after load
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_title_akas


    - name: region is null and language is null and original title is false.
      description: Ensures types is not null
      value: 0
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_title_akas WHERE region IS NULL AND language IS NULL AND is_original_title = false

    - name: technical_deduplication_check
      description: "Ensures no movie has multiple entries for the same Region/Language combination"
      value: 0
      query: |
        SELECT COUNT(*)
        FROM (
            SELECT 1
            FROM cleansing.cln_title_akas
            GROUP BY title_id, region, language
            HAVING COUNT(*) > 1
        ) AS duplicates

@bruin */

SELECT
    -- keeping the first title found for each title_id, region, language combination, of all or any of the three.
    DISTINCT ON (a.title_id, a.region, a.language)
    a.title_id,
    a.region,
    a.language,
    a.localised_title,
    a.ordering,
    a.is_original_title,
    a.attributes,
    a.types
FROM staging.stg_title_akas a
INNER JOIN
cleansing.cln_title_basics b
ON
b.title_id = a.title_id
WHERE
(

    -- keeping only the rows where 
    region IS NOT NULL
    OR language IS NOT NULL 
    OR is_original_title = true

)
ORDER BY a.title_id, a.region, a.language, a.ordering ASC;

