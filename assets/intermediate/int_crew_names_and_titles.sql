/* @bruin

name: intermediate.int_crew_names_and_titles
type: duckdb.sql

materialization:
    type: view

tags: [intermediate]

depends:
    -  cleansing.cln_name_basics
    -  cleansing.cln_title_basics
    -  cleansing.cln_title_crew

columns:

    - name: title_id
      type: string

    - name: title
      type: string

    - name: director_id
      type: string

    - name: director_name
      type: string

    - name: writer_id
      type: string

    - name: writers_name
      type: string

custom_checks:
    - name: row_count_positive
      description: "Ensures that the intermediate table is not empty"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM intermediate.int_crew_names_and_titles
        
@bruin */


SELECT 
    tc.title_id,
    tb.original_title AS title,
    nb_dir.person_id AS director_id,
    nb_dir.primary_name AS director_name,
    nb_writ.person_id AS writer_id,
    nb_writ.primary_name AS writers_name
FROM cleansing.cln_title_crew tc
-- Unnest directors and join
CROSS JOIN UNNEST(string_to_array(tc.directors, ',')) AS d(dir_id)
JOIN cleansing.cln_name_basics nb_dir ON d.dir_id = nb_dir.person_id
-- Unnest writers and join
CROSS JOIN UNNEST(string_to_array(tc.writers, ',')) AS w(writ_id)
JOIN cleansing.cln_name_basics nb_writ ON w.writ_id = nb_writ.person_id
JOIN cleansing.cln_title_basics tb ON tc.title_id = tb.title_id;
