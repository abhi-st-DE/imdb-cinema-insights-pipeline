/* @bruin

name: intermediate.int_ep_epid_prntepid_title
type: duckdb.sql

materialization:
    type: view

tags: [intermediate]

depends:
    - cleansing.cln_title_episode
    - cleansing.cln_title_basics

columns:
    - name: episode_id
      type: string
      description: "The id of the episode"
      primary_key: true

    - name: episode_title
      type: string
      description: "The title of the episode"

    - name: parent_series_id
      type: string
      description: "The id of the parent series"

    - name: series_title
      type: string
      description: "The title of the parent series"

    - name: season_number
      type: integer
      description: "The season number of the episode"

    - name: episode_number
      type: integer
      description: "The episode number of the episode"

custom_checks:
    - name: row_count_positive
      description: "Ensures that the intermediate table is not empty"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM intermediate.int_ep_epid_prntepid_title

@bruin */

SELECT 
    ep.episode_id,
    b_ep.original_title AS episode_title, -- Name of the episode
    ep.parent_series_id,
    b_series.original_title AS series_title, -- Name of the show
    ep.season_number,
    ep.episode_number
FROM cleansing.cln_title_episode ep
JOIN cleansing.cln_title_basics b_ep ON ep.episode_id = b_ep.title_id
JOIN cleansing.cln_title_basics b_series ON ep.parent_series_id = b_series.title_id;
