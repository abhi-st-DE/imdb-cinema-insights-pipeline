/* @bruin

name: intermediate.int_princi_titid_orgid_prsnid_name
type: duckdb.sql

materialization:
    type: view

tags: [intermediate]

depends:
    - cleansing.cln_title_principals
    - cleansing.cln_name_basics
    - cleansing.cln_title_basics

columns:
    - name: character_name
      type: string
      description: "The name of the character"

    - name: role
      type: string
      description: "The role of the person in the movie"

    - name: title_id
      type: string
      description: "The id of the movie"
      primary_key: true

    - name: title
      type: string
      description: "The original title of the movie"

    - name: ordering
      type: integer
      description: "The ordering of the person in the movie"

    - name: person_id
      type: string
      description: "The id of the person"
      primary_key: true

    - name: person_name
      type: string
      description: "The name of the person"

custom_checks:
    - name: row_count_positive
      description: "Ensures that the intermediate table is not empty"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM intermediate.int_princi_titid_orgid_prsnid_name


@bruin */

SELECT 
    tp.character_name,
    tp.role,
    tp.title_id,
    tb.original_title AS title,
    tp.ordering,
    tp.person_id,
    nb.primary_name AS person_name
FROM cleansing.cln_title_principals tp
JOIN cleansing.cln_name_basics nb ON tp.person_id = nb.person_id
JOIN cleansing.cln_title_basics tb ON tp.title_id = tb.title_id;