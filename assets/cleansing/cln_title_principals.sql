/* @bruin

name: cleansing.cln_title_principals
type: duckdb.sql

materialization:
    type: table

tags: [cleansing]

depends:
    - staging.stg_title_principals
    - cleansing.cln_name_basics
    - cleansing.cln_title_basics

columns:
    - name: character_name
      type: string
      description: The name of the character played if applicable, else NULL.

    - name: role
      type: string
      description: combining category and job.

    - name: title_id
      type: string

    - name: ordering
      type: integer

    - name: person_id
      type: string

    
custom_checks:
    - name: row_count_positive
      description: "Ensures we didn't accidentally delete the whole table during cleansing"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_title_principals

    - name: weird_characters exists !!
      description: 'characters like [,]," exists'
      value: 0
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_title_principals WHERE character_name LIKE '[%' OR character_name LIKE '%]' OR character_name LIKE '"%' OR character_name LIKE '%"'

@bruin */

SELECT 
    -- We nest REPLACE functions to scrub '[', ']', and '"'
    REPLACE(REPLACE(REPLACE(p.characters, '[', ''), ']', ''), '"', '') AS character_name,
    CASE 
        WHEN p.job_title IS NULL THEN p.category -- If job is empty, use category
        ELSE p.category || ' (' || p.job_title || ')' -- If both exist, combine them: "Actor (Stunt Double)"
    END AS role,
    p.title_id,
    p.ordering,
    p.person_id
FROM staging.stg_title_principals p
INNER JOIN
cleansing.cln_name_basics n
ON
p.person_id = n.person_id
INNER JOIN
cleansing.cln_title_basics t
ON
t.title_id = p.title_id;