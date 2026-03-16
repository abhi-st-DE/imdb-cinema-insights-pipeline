/* @bruin

name: cleansing.cln_title_basics
type: duckdb.sql

materialization:
    type: table

tags: [cleansing]

depends:
    - staging.stg_title_basics
    
columns:
    - name: title_id
      type: string
      description: "The primary unique identifier for the title (e.g., tt1234567)"
      primary_key: true
      checks:
        - name: not_null

    - name: type_of_title
      type: string
      description: "The type of title (e.g., movie, short, etc.)"
      checks:
        - name: not_null

    - name: primary_title
      type: string
      description: "The more popular title / the title used by the filmmakers on promotional materials(region and language wise sometimes) at the point of release."

    - name: decade
      type: string
      description: "Feature: The decade derived from start_year (e.g., 1990s)"
      checks:
        - name: not_null

    - name: is_adult
      type: boolean
      description: "Feature: Whether the title is for adults (e.g., R-rated movies)"

    - name: runtime_minutes
      type: integer
      description: "Primary runtime of the title, in minutes."

    - name: start_year
      type: integer
      description: "The year the title was released or started"

    - name: end_year
      type: integer
      description: "The year the title ended."

custom_checks:
    - name: row_count_positive
      description: "Ensures we didn't accidentally delete the whole table during cleansing"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_title_basics

    - name: runtime_integrity_check
      description: "Ensures no 'Angry Grandpas' (runtimes > 5000) survived the cleansing"
      value: 0
      query: |
        SELECT COUNT(*) FROM cleansing.cln_title_basics WHERE runtime_minutes > 5000

    - name: decade_format_check
      description: "Ensures the 'decade' feature always ends with an 's'"
      value: 0
      query: |
        SELECT COUNT(*) FROM cleansing.cln_title_basics 
        WHERE decade != 'Unknown' AND decade NOT LIKE '%s'


@bruin */

SELECT 
    title_id,
    type_of_title,
    start_year,
    -- 1. Redefine 'end_year' (repairing the pre-1970 mystery series)
    CASE 
        WHEN start_year < 1970 AND end_year IS NULL THEN start_year
        ELSE end_year 
    END AS end_year,
    -- 2. Redefine 'primary_title' (trimming paragraph-long titles)
    CASE 
        WHEN len(primary_title) > 40 THEN LEFT(primary_title, 40) || '...'
        ELSE primary_title 
    END AS primary_title,
    -- 3. Redefine 'original_title' (trimming paragraph-long titles)
    CASE 
        WHEN len(original_title) > 40 THEN LEFT(original_title, 40) || '...'
        ELSE original_title 
    END AS original_title,
    -- 4. Add your new feature
    CASE 
        WHEN start_year IS NULL THEN 'Unknown'
        ELSE CAST(FLOOR(start_year / 10.0) * 10 AS INTEGER)::VARCHAR || 's'
    END AS decade,
    is_adult,
    genres,
    runtime_minutes
FROM staging.stg_title_basics
WHERE
    start_year >= 1874 -- Keeping the historic Passage of Venus
    AND
    runtime_minutes != 0 
    AND
    runtime_minutes <= 5000
    AND
    len(original_title) < 120
    AND
    len(primary_title) < 120
    AND
    (start_year <= end_year OR end_year IS NULL);