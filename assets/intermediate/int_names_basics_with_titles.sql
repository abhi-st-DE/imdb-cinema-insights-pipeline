/* @bruin

name: intermediate.int_names_basics_with_titles
type: duckdb.sql

materialization:
    type: view

tags: [intermediate]

depends:
    - cleansing.cln_name_basics
    - cleansing.cln_title_basics

columns:
    - name: person_id
      type: string

    - name: display_name
      type: string

    - name: age
      type: integer

    - name: is_alive
      type: boolean

    - name: birth_century
      type: string

    - name: birth_year
      type: integer

    - name: death_year
      type: integer

    - name: specific_profession
      type: string

    - name: known_for_titles
      type: string

    - name: known_for_titles_names
      type: string
      description: Comma - separated list of English movie/series etc names

custom_checks:
    - name: row_count_positive
      description: "Ensures that the intermediate table is not empty"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM intermediate.int_names_basics_with_titles

@bruin */

SELECT 
    nam.person_id,
    nam.display_name,
    nam.age,
    nam.is_alive,
    nam.birth_century,
    nam.birth_year,
    nam.death_year,
    TRIM(p.unnested_prof) AS specific_profession,
    nam.known_for_titles,
    STRING_AGG(DISTINCT title.primary_title, ', ' ORDER BY title.primary_title) AS known_for_titles_names
FROM cleansing.cln_name_basics nam
CROSS JOIN UNNEST(STRING_SPLIT(nam.primary_profession, ',')) AS p(unnested_prof)
LEFT JOIN LATERAL
UNNEST(
    STRING_SPLIT(nam.known_for_titles, ',')
)  AS n(single_title_id) ON TRUE
LEFT JOIN cleansing.cln_title_basics title
ON
n.single_title_id = title.title_id
GROUP BY
    nam.person_id,
    nam.birth_year,
    nam.death_year,
    nam.known_for_titles,
    nam.display_name,
    nam.age,
    nam.is_alive,
    nam.birth_century,
    p.unnested_prof
;