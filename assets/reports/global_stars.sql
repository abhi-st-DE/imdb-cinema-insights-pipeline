/* @bruin

name: reports.global_stars
type: duckdb.sql

materialization:
    type: table

tags: [reports]

depends:
    - intermediate.int_princi_titid_orgid_prsnid_name
    - intermediate.int_names_basics_with_titles
    - intermediate.int_akas_orgtit_localtit
    - intermediate.int_ratings_titid_orgtit

columns:
    - name: display_name
      type: string
      checks:
        - name: not_null

    - name: primary_profession
      type: string
      checks:
        - name: not_null

    - name: total_movies
      type: integer
      checks:
        - name: not_null

    - name: avg_rating
      type: double
      checks:
        - name: not_null

    - name: total_votes
      type: integer
      checks:
        - name: not_null

    - name: peak_global_reach
      type: integer
      checks:
        - name: not_null

    - name: global_star_index
      type: double
      checks:
        - name: not_null

    - name: rank_in_profession
      type: integer
      description: "Rank of the artist specifically within their profession"
      checks:
        - name: not_null

custom_checks:
    - name: row_count_positive
      description: "Ensures that the intermediate table is not empty"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM reports.global_stars

@bruin */

WITH title_footprint AS (
    SELECT 
        title_id, 
        COUNT(DISTINCT region) as countries_reached,
        COUNT(DISTINCT language) as tongues_spoken
    FROM intermediate.int_akas_orgtit_localtit
    GROUP BY title_id
)
, artist_work AS (
    SELECT 
        nam.display_name,
        nam.specific_profession,
        rat.average_rating,
        rat.num_votes,
        f.countries_reached
    FROM intermediate.int_princi_titid_orgid_prsnid_name prin
    JOIN intermediate.int_names_basics_with_titles nam ON prin.person_id = nam.person_id
    JOIN intermediate.int_ratings_titid_orgtit rat ON prin.title_id = rat.title_id
    JOIN title_footprint f ON prin.title_id = f.title_id
)
SELECT 
    display_name,
    specific_profession as primary_profession,
    COUNT(*) as total_movies,
    AVG(average_rating) as avg_rating,
    SUM(num_votes) as total_votes,
    MAX(countries_reached) as peak_global_reach,
    -- THE FORMULA: Score = Quality * Reach * Volume
    (AVG(average_rating) * LOG10(SUM(num_votes)) * AVG(countries_reached)) AS global_star_index,
    -- THE RANK: Numerical rank within their specific profession
    RANK() OVER (
        PARTITION BY specific_profession 
        ORDER BY (AVG(average_rating) * LOG10(SUM(num_votes)) * AVG(countries_reached)) DESC
    ) AS rank_in_profession
FROM artist_work
GROUP BY 1, 2
ORDER BY global_star_index DESC;

