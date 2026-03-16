/* @bruin

name: cleansing.cln_name_basics
type: duckdb.sql

materialization:
    type: table

depends:
    -  staging.stg_name_basics

tags: [cleansing]
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
      checks:
        - name: not_null

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

    - name: is_alive
      type: boolean
      description: Whether the person is alive or not.

    - name: age
      type: integer
      description: Age of the person.

    - name: birth_century
      type: string
      description: Birth century of the person.


custom_checks:
    - name: row_count_positive
      description: Ensures the cleansing table is not empty after load
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_name_basics

    - name: birth_year_death_year_check
      description: Ensures birth year is less than death year
      value: 0
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_name_basics WHERE birth_year > death_year;


    - name: primary_name_check
      description: Ensures primary name is not null
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_name_basics WHERE primary_name IS NOT NULL;

@bruin */



SELECT
    person_id,
    primary_name,
    -- because name could be same, we add birth year if available
    CASE 
        WHEN birth_year IS NOT NULL THEN primary_name || ' (' || birth_year || ')'
        ELSE primary_name 
    END AS display_name,
    birth_year,
    death_year,
    primary_profession,
    known_for_titles,
    -- alive or not
    CASE 
        WHEN death_year IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_alive,

    -- age of the person.
    CASE 
        WHEN death_year IS NOT NULL AND birth_year IS NOT NULL THEN death_year - birth_year
        WHEN death_year IS NULL AND birth_year IS NOT NULL THEN date_part('year', current_date) - birth_year
        ELSE NULL 
    END AS age,

    -- birth century of the person.
    CASE 
        WHEN birth_year IS NULL THEN 'Unknown'
        WHEN birth_year < 1900 THEN '19th Century or older'
        WHEN birth_year BETWEEN 1900 AND 1999 THEN '20th Century'
        WHEN birth_year >= 2000 THEN '21st Century'
        ELSE 'Unknown'
    END AS birth_century

FROM staging.stg_name_basics
-- keeping the rows where primary name is not null.
WHERE primary_name IS NOT NULL
AND (
-- selecting only those whose birth year is less than death year.
birth_year <= death_year
OR death_year IS NULL
OR birth_year IS NULL
);
