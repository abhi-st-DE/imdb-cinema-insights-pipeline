/* @bruin

name: staging.stg_title_basics
type: duckdb.sql

materialization:
    type: table

tags: [staging]

depends:
    -  ingestion.ing_title_basics

columns:
    - name: title_id
      type: string
      description: Alphanumeric unique identifier of the title.
      primary_key: true
      checks:
        - name: not_null

    - name: type_of_title
      type: string
      description: The type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc.).

    - name: primary_title
      type: string
      description: The more popular title / the title used by the filmmakers on promotional materials at the point of release.

    - name: original_title
      type: string
      description: Original title, in the original language.

    - name: is_adult
      type: boolean
      description: "0: non-adult title; 1: adult title."

    - name: start_year
      type: integer
      description: Represents the release year of a title. In the case of TV Series, it is the series start year.

    - name: end_year
      type: integer
      description: TV Series end year. \N for all other title types.

    - name: runtime_minutes
      type: integer
      description: Primary runtime of the title, in minutes.

    - name: genres
      type: string
      description: Includes up to three genres associated with the title.

custom_checks:
  - name: row_count_positive
    description: Ensures the staging table is not empty after load
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM staging.stg_title_basics

@bruin */


/*

with deduplicated AS(
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                tconst
            ORDER BY tconst
        ) AS ROW_NUM
    FROM ingestion.ing_title_basics
)

*/

SELECT DISTINCT ON (tconst)

    -- Unique identifier of the title.
    CAST(tconst AS STRING) AS title_id,

   -- Titles of the movies, series etc. 
    CAST(titleType AS STRING) AS type_of_title,
    CAST(primaryTitle AS STRING) AS primary_title,
    CAST(originalTitle AS STRING) AS original_title,

    -- Age_Group
    CAST(isAdult AS BOOLEAN) AS is_adult,

    -- Series Start and End Year.
    CAST(startYear AS INTEGER) AS start_year,
    CAST(endYear AS INTEGER) AS end_year,

    -- Runtime in minutes.
    CAST(runtimeMinutes AS INTEGER) AS runtime_minutes,

    -- Genres of the movie.
    CAST(genres AS STRING) AS genres,
    
FROM ingestion.ing_title_basics
WHERE tconst IS NOT NULL;