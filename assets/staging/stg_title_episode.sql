/* @bruin

name: staging.stg_title_episode
type: duckdb.sql

materialization:
    type: table

tags: [staging]

depends:
    - ingestion.ing_title_episode

columns:
    - name: episode_id
      type: string
      description: Alphanumeric identifier of the episode.
      checks:
        - name: not_null

    - name: parent_series_id
      type: string
      description: Alphanumeric identifier of the parent TV Series.
      checks:
        - name: not_null

    - name: season_number
      type: integer
      description: Season number the episode belongs to.

    - name: episode_number
      type: integer
      description: Episode number of the given episode of the given episode_id.

custom_checks:
  - name: row_count_positive
    description: Ensures the staging table is not empty after load
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM staging.stg_title_episode

@bruin */

/*

with deduplicated AS(
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                tconst, parentTconst
            ORDER BY tconst
        ) AS ROW_NUM
    FROM ingestion.ing_title_episode
)

*/

SELECT DISTINCT ON (tconst, parentTconst)
    -- unique id
    CAST(tconst AS STRING) AS episode_id,

    -- parent series id
    CAST(parentTconst AS STRING) AS parent_series_id,

    -- season number
    CAST(seasonNumber AS INTEGER) AS season_number,

    -- episode number
    CAST(episodeNumber AS INTEGER) AS episode_number
FROM ingestion.ing_title_episode
WHERE tconst IS NOT NULL
AND parentTconst IS NOT NULL;