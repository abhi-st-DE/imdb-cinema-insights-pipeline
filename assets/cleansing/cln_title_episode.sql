/* @bruin

name: cleansing.cln_title_episode
type: duckdb.sql

materialization:
    type: table

tags: [cleansing]

depends:
    - staging.stg_title_episode
    - cleansing.cln_title_basics

columns:
    - name: episode_id
      type: string
      description: "Unique identifier for the episode"
      primary_key: true
      checks:
        - name: not_null
    
    - name: parent_series_id
      type: string
      description: "ID of the parent TV series"
      checks:
        - name: not_null

    - name: season_number
      type: integer

    - name: episode_number
      type: integer

custom_checks:
    - name: row_count_positive
      description: "Ensures the table is not empty"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM cleansing.cln_title_episode

@bruin */


SELECT 
    e.episode_id,
    e.parent_series_id,
    e.season_number,
    e.episode_number
FROM staging.stg_title_episode e
-- 1. Check the EPISODE
INNER JOIN cleansing.cln_title_basics b_ep
    ON e.episode_id = b_ep.title_id
-- 2. Check the SERIES
INNER JOIN cleansing.cln_title_basics b_series
    ON e.parent_series_id = b_series.title_id;