/* @bruin

name: reports.when_the_show_gets_boring
type: duckdb.sql

materialization:
    type: table

tags: [reports]

depends:
    - intermediate.int_ratings_titid_orgtit
    - intermediate.int_crew_names_and_titles
    - intermediate.int_ep_epid_prntepid_title

columns:
    - name: series_title
      type: string
      checks:
        - name: not_null
    - name: season_number
      type: integer
      checks:
        - name: not_null
    - name: avg_integrity_score
      type: double
      checks:
        - name: not_null
    - name: avg_episode_rating
      type: double
      checks:
        - name: not_null
    - name: total_votes
      type: integer
      checks:
        - name: not_null

custom_checks:
    - name: row_count_positive
      description: "Ensures we have integrity data for franchises"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM reports.when_the_show_gets_boring

@bruin */

WITH series_architects AS (
    -- Identify the "Original Creators" (Directors or Writers) of the Series
    SELECT DISTINCT
        c.title_id AS series_id,
        c.director_id AS architect_id
    FROM intermediate.int_crew_names_and_titles c
    JOIN intermediate.int_ratings_titid_orgtit r ON c.title_id = r.title_id
    WHERE r.type_of_title = 'tvSeries'
    UNION DISTINCT
    SELECT DISTINCT
        c.title_id AS series_id,
        c.writer_id AS architect_id
    FROM intermediate.int_crew_names_and_titles c
    JOIN intermediate.int_ratings_titid_orgtit r ON c.title_id = r.title_id
    WHERE r.type_of_title = 'tvSeries'
),

episode_execution AS (
    -- Get every person who worked on every episode
    SELECT DISTINCT
        ep.parent_series_id,
        ep.season_number,
        ep.episode_id,
        c_ep.director_id AS worker_id
    FROM intermediate.int_ep_epid_prntepid_title ep
    JOIN intermediate.int_crew_names_and_titles c_ep ON ep.episode_id = c_ep.title_id
    WHERE ep.season_number IS NOT NULL
    UNION DISTINCT
    SELECT DISTINCT
        ep.parent_series_id,
        ep.season_number,
        ep.episode_id,
        c_ep.writer_id AS worker_id
    FROM intermediate.int_ep_epid_prntepid_title ep
    JOIN intermediate.int_crew_names_and_titles c_ep ON ep.episode_id = c_ep.title_id
    WHERE ep.season_number IS NOT NULL
),

integrity_check AS (
    -- Check if ANY of the episode workers are original Architects
    SELECT 
        ee.parent_series_id,
        ee.season_number,
        ee.episode_id,
        MAX(CASE WHEN sa.architect_id IS NOT NULL THEN 1 ELSE 0 END) AS has_architect
    FROM episode_execution ee
    LEFT JOIN series_architects sa 
        ON ee.parent_series_id = sa.series_id 
        AND ee.worker_id = sa.architect_id
    GROUP BY 1, 2, 3
)

SELECT 
    r.title AS series_title,
    ic.season_number,
    AVG(ic.has_architect) AS avg_integrity_score,
    AVG(r_ep.average_rating) AS avg_episode_rating,
    SUM(r_ep.num_votes) AS total_votes
FROM integrity_check ic
JOIN intermediate.int_ratings_titid_orgtit r ON ic.parent_series_id = r.title_id
JOIN intermediate.int_ratings_titid_orgtit r_ep ON ic.episode_id = r_ep.title_id
GROUP BY r.title, ic.season_number
ORDER BY r.title, ic.season_number;
