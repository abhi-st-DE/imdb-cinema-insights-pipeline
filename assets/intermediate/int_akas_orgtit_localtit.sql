/* @bruin

name: intermediate.int_akas_orgtit_localtit
type: duckdb.sql

materialization:
    type: view

tags: [intermediate]

depends:
    - cleansing.cln_title_akas
    - cleansing.cln_title_basics

columns:
    - name: title_id
      type: string
      description: "The id of the movie"
      primary_key: true

    - name: original_title
      type: string
      description: "The original title of the movie"

    - name: localised_title
      type: string
      description: "The localised title of the movie"

    - name: region
      type: string
      description: "The region of the movie"

    - name: language
      type: string
      description: "The language of the movie"

    - name: ordering
      type: integer
      description: "The ordering of the movie"

    - name: is_original_title
      type: boolean
      description: "The is original title of the movie"

custom_checks:
    - name: row_count_positive
      description: "Ensures that the intermediate table is not empty"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM intermediate.int_akas_orgtit_localtit

@bruin */

SELECT 
    a.title_id,
    b.original_title,      -- The Master Name (e.g., "The Matrix")
    a.localised_title,     -- The Local Name (e.g., "Matrix")
    a.region,              -- Country (e.g., "FR")
    a.language,            -- Language (e.g., "fr")
    a.ordering,
    a.is_original_title    -- Flag to know if this is the 'official' one
FROM cleansing.cln_title_akas a
JOIN cleansing.cln_title_basics b ON a.title_id = b.title_id;
