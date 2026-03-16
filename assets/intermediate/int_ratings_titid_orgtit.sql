/* @bruin

name: intermediate.int_ratings_titid_orgtit
type: duckdb.sql

materialization:
    type: view

tags: [intermediate]

depends:
    - cleansing.cln_title_ratings
    - cleansing.cln_title_basics


columns:
    - name: title_id
      type: string
      description: "The id of the movie"
      primary_key: true

    - name: type_of_title
      type: string

    - name: start_year
      type: integer

    - name: genres
      type: string

    - name: average_rating
      type: float
      description: "The average rating of the movie"

    - name: num_votes
      type: integer
      description: "The number of votes for the movie"

custom_checks:
    - name: row_count_positive
      description: "Ensures that the intermediate table is not empty"
      value: 1
      query: |
        SELECT COUNT(*) > 0 FROM intermediate.int_ratings_titid_orgtit

@bruin */

SELECT 
    tr.title_id,
    tb.original_title AS title,
    tb.type_of_title,
    tb.start_year,
    tb.genres,
    tr.average_rating,
    tr.num_votes
FROM cleansing.cln_title_ratings tr
JOIN cleansing.cln_title_basics tb ON tr.title_id = tb.title_id;