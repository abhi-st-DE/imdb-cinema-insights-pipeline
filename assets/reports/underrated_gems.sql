/* @bruin

name: reports.underrated_gems
type: duckdb.sql

materialization:
    type: table
    
tags: [reports]

depends:
    - intermediate.int_akas_orgtit_localtit
    - intermediate.int_ratings_titid_orgtit

columns:
    - name: region
      type: string
      checks:
        - name: not_null

    - name: languages
      type: string
      checks:
        - name: not_null

    - name: localised_title
      type: string
      checks:
        - name: not_null

    - name: original_title
      type: string
      checks:
        - name: not_null

    - name: start_year
      type: integer
      checks:
        - name: not_null

    - name: genres
      type: string
      checks:
        - name: not_null

    - name: average_rating
      type: double
      checks:
        - name: not_null

    - name: num_votes
      type: integer
      checks:
        - name: not_null

    - name: rank_per_region
      type: integer
      checks:
        - name: not_null

custom_checks:
    - name: row_count_positive
      description: "Ensures we found at least some indie gems"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM reports.underrated_gems

@bruin */

WITH unique_regional_titles AS (
    -- Get the most 'official' title (lowest ordering) per region/movie
    -- and aggregate all languages into one string to prevent duplication
    SELECT 
        title_id,
        region,
        ARG_MIN(localised_title, ordering) AS localised_title,
        STRING_AGG(DISTINCT language, ', ') AS languages
    FROM intermediate.int_akas_orgtit_localtit
    WHERE region IS NOT NULL 
      AND language IS NOT NULL 
      AND localised_title IS NOT NULL
    GROUP BY title_id, region
)
SELECT
    urt.region,
    urt.languages,
    urt.localised_title,
    rat.title AS original_title,
    rat.start_year,
    rat.genres,
    rat.average_rating,
    rat.num_votes,
    DENSE_RANK() OVER (
        PARTITION BY urt.region 
        ORDER BY rat.average_rating DESC, rat.num_votes DESC
    ) AS rank_per_region
FROM unique_regional_titles urt
JOIN intermediate.int_ratings_titid_orgtit rat ON urt.title_id = rat.title_id
WHERE rat.average_rating > 8.0 
  AND rat.num_votes BETWEEN 500 AND 10000 
  AND rat.title IS NOT NULL
  AND rat.start_year IS NOT NULL
  AND rat.genres IS NOT NULL
  AND rat.type_of_title IN ('movie', 'tvSeries', 'tvMiniSeries', 'short', 'tvMovie')
ORDER BY urt.region, rank_per_region ASC;