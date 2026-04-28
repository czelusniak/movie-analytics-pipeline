-- vw_scatter_popularity_vs_quality
-- show the movies that have at least 50 ratings in total
SELECT * FROM `netflix-project-czelusniak.netflix_analytical.vw_scatter_popularity_vs_quality`
WHERE avg_rating < 1
LIMIT 100;

SELECT MIN(avg_rating), MAX(avg_rating)
FROM `netflix-project-czelusniak.netflix_analytical.vw_scatter_popularity_vs_quality`;

CREATE OR REPLACE VIEW `netflix-project-czelusniak.netflix_analytical.vw_scatter_popularity_vs_quality` AS 
  SELECT
    movie_id,
    title,
    genres,
    release_year,
    total_rating,
    avg_rating
  FROM `netflix-project-czelusniak.netflix_analytical.vw_movies_kpis` 
  WHERE total_rating >= 50;


-- vw_ratings_heatmap
SELECT * FROM `netflix-project-czelusniak.netflix_analytical.vw_ratings_heatmap` LIMIT 100;

CREATE OR REPLACE VIEW `netflix-project-czelusniak.netflix_analytical.vw_ratings_heatmap` AS 
  SELECT
    EXTRACT(YEAR FROM rating_ts) as year,
    EXTRACT(MONTH FROM rating_ts) as month_number,
    FORMAT_TIMESTAMP('%b', rating_ts) as month_name,
    COUNT(*) as total_ratings
  FROM `netflix-project-czelusniak.netflix_analytical.fact_ratings`
  GROUP BY 1, 2, 3
  ORDER BY year, month_number;



-- vw_genre_performance
SELECT * FROM `netflix-project-czelusniak.netflix_analytical.vw_genre_performance` LIMIT 100;

CREATE OR REPLACE VIEW `netflix-project-czelusniak.netflix_analytical.vw_genre_performance` AS

  WITH filtered_movies AS (
    SELECT *
    FROM `netflix-project-czelusniak.netflix_analytical.dim_movies`
    WHERE genres IS NOT NULL
    AND genres != '(no genres listed)'
  ),

  exploded AS (
    SELECT 
      genre,
      r.rating
    FROM filtered_movies as m
    INNER JOIN `netflix-project-czelusniak.netflix_analytical.fact_ratings` as r -- filtering out movies without ratings
    ON m.movie_id = r.movie_id
    CROSS JOIN UNNEST(SPLIT(genres, '|')) AS genre
  )

  SELECT
    genre,
    COUNT(*) AS total_ratings,
    AVG(rating) AS avg_rating,
    STDDEV(rating) AS std_rating
  FROM exploded
  GROUP BY 1
  ORDER BY total_ratings DESC, avg_rating DESC;

-- vw_top_movies
SELECT * FROM `netflix-project-czelusniak.netflix_analytical.vw_top_movies` LIMIT 100;

CREATE OR REPLACE VIEW `netflix-project-czelusniak.netflix_analytical.vw_top_movies` AS
  SELECT
    movie_id,
    title,
    genres,
    release_year,
    total_rating,
    ROUND(avg_rating, 2) AS avg_rating
  FROM `netflix-project-czelusniak.netflix_analytical.vw_movies_kpis`
  WHERE total_rating >= 20
  AND avg_rating BETWEEN 0 AND 5
  ORDER BY avg_rating DESC, total_rating DESC
  LIMIT 10;

-- vw_user_activity
SELECT * FROM `netflix-project-czelusniak.netflix_analytical.vw_user_activity` LIMIT 100;

CREATE OR REPLACE VIEW netflix-project-czelusniak.netflix_analytical.vw_user_activity AS
SELECT
  user_id,
  COUNT(*) AS total_ratings,
  COUNT(DISTINCT movie_id) AS distinct_movies_rated,
  AVG(rating) AS avg_rating,
  STDDEV(rating) AS std_rating,
  MIN(rating_ts) AS first_activity_ts,
  MAX(rating_ts) AS last_activity_ts
FROM netflix-project-czelusniak.netflix_analytical.fact_ratings
GROUP BY 1
ORDER BY total_ratings DESC, avg_rating DESC
;

-- vw_movies_kpis
SELECT * FROM `netflix-project-czelusniak.netflix_analytical.vw_movies_kpis` LIMIT 100;

CREATE OR REPLACE VIEW `netflix-project-czelusniak.netflix_analytical.vw_movies_kpis` AS
  SELECT
    r.movie_id,
    m.title,
    m.genres,
    m.release_year,
    COUNT(*) as total_rating,
    AVG(r.rating) as avg_rating,
    STDDEV(r.rating) AS std_rating,
    MIN(r.rating_ts) AS first_rating_ts,
    MAX(r.rating_ts) AS last_rating_ts
  FROM `netflix-project-czelusniak.netflix_analytical.fact_ratings` AS r
  LEFT JOIN `netflix-project-czelusniak.netflix_analytical.dim_movies` AS m --LEFT JOIN garante que todos os ratings aparecem, mesmo que o filme não exista na dim_movies.
  ON r.movie_id = m.movie_id
  --WHERE m.title IS NOT NULL
  -- 10.647 movie_ids em fact_ratings não existem em dim_movies
  -- descomentar para excluir ratings órfãos da análise
  GROUP BY 1,2,3,4
  ORDER BY avg_rating DESC;

SELECT t1.*
FROM `netflix-project-czelusniak.netflix_analytical.fact_ratings` as t1
LEFT JOIN `netflix-project-czelusniak.netflix_analytical.dim_movies` as t2 
ON t1.movie_id = t2.movie_id
WHERE t2.movie_id IS NULL
LIMIT 1000;

SELECT
COUNT(DISTINCT movie_id)
FROM `netflix-project-czelusniak.netflix_analytical.dim_movies`;

SELECT COUNT(DISTINCT movie_id)
FROM `netflix-project-czelusniak.netflix_analytical.fact_ratings`;

SELECT COUNT(DISTINCT t1.movie_id)
FROM `netflix-project-czelusniak.netflix_analytical.fact_ratings` as t1
LEFT JOIN `netflix-project-czelusniak.netflix_analytical.dim_movies` as t2 
ON t1.movie_id = t2.movie_id
WHERE t2.movie_id IS NULL
LIMIT 1000;

--- 1) DIM TABLE: dim_movies
SELECT * FROM `netflix-project-czelusniak.netflix_analytical.dim_movies`
WHERE title is null
LIMIT 100;

CREATE OR REPLACE TABLE `netflix-project-czelusniak.netflix_analytical.dim_movies` AS
SELECT
	SAFE_CAST(movieId AS INT64) AS movie_id,
  TRIM(REGEXP_REPLACE(title, r'\(\d{4}\)', '')) AS title,      -- O TRIM já garante que é STRING e limpa os espaços
  TRIM(genres) AS genres,    -- O mesmo vale para os gêneros
	SAFE_CAST(REGEXP_EXTRACT(CAST(title AS STRING), r'\((\d{4})\)\s*$') AS INT64) AS release_year
FROM `netflix-project-czelusniak.netflix_raw.raw_movies`
;

--- 2) FACT TABLE: fact_ratings
SELECT * FROM `netflix-project-czelusniak.netflix_analytical.fact_ratings` LIMIT 100;

CREATE OR REPLACE TABLE `netflix-project-czelusniak.netflix_analytical.fact_ratings` AS

WITH all_ratings AS (
  SELECT
    SAFE_CAST(userId AS INT64) AS user_id,
    SAFE_CAST(movieId AS INT64) AS movie_id,

    SAFE_CAST(rating AS FLOAT64) AS rating,

    COALESCE(
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez',tstamp),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',tstamp)
    ) AS rating_ts, 

    'user_rating_history' AS src
  FROM `netflix-project-czelusniak.netflix_raw.user_rating_history`

  UNION ALL

  SELECT
    SAFE_CAST(userId AS INT64) AS user_id,
    SAFE_CAST(movieId AS INT64) AS movie_id,

    SAFE_CAST(rating AS FLOAT64) AS rating,

    COALESCE(
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez',tstamp),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',tstamp)
    ) AS rating_ts, 

    'ratings_for_additional_users' AS src

  FROM `netflix-project-czelusniak.netflix_raw.ratings_for_additional_users`
)

SELECT
  user_id,
  movie_id,
  rating,
  rating_ts,
  src
FROM all_ratings
WHERE 
  user_id IS NOT NULL
  AND movie_id IS NOT NULL
  AND rating IS NOT NULL
  AND rating_ts IS NOT NULL
  AND src IS NOT NULL;



--- === raw_movies

CREATE OR REPLACE EXTERNAL TABLE `netflix-project-czelusniak.netflix_raw.raw_movies`
(
  movieID STRING,
  title STRING,
  genres STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://bucket-netflix-czelusniak/bronze/movies.csv'],
	skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

-- ==== user_rating_history ====
CREATE OR REPLACE EXTERNAL TABLE `netflix-project-czelusniak.netflix_raw.user_rating_history`
(
  userId STRING,
  movieId STRING,
  rating STRING,
  tstamp STRING -- Mudei de 'timestamp' para 'tstamp' aqui
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://bucket-netflix-czelusniak/bronze/user_rating_history.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

-- ==== ratings_for_additional_users (não é user_additional_rating)====
CREATE OR REPLACE EXTERNAL TABLE `netflix-project-czelusniak.netflix_raw.ratings_for_additional_users`
(
  userId STRING,
  movieId STRING,
  rating STRING,
  tstamp STRING -- Mudei de 'timestamp' para 'tstamp' aqui
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://bucket-netflix-czelusniak/bronze/ratings_for_additional_users.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

-- ==== belief_data ====
CREATE OR REPLACE EXTERNAL TABLE `netflix-project-czelusniak.netflix_raw.belief_data`
(
  userId STRING,
  movieId STRING,
  isSeen STRING,
  watchDate STRING,
  userElicitRating STRING,
  userPredictRating STRING,
  userCertainty STRING,
  tstamp STRING,
  month_idx STRING,
  source STRING,
  systemPredictRating STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://bucket-netflix-czelusniak/bronze/belief_data.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

-- ==== movie_elicitation_set ====
CREATE OR REPLACE EXTERNAL TABLE `netflix-project-czelusniak.netflix_raw.movie_elicitation_set`
(
  movieId STRING,
  month_idx STRING,
  source STRING,
  tstamp STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://bucket-netflix-czelusniak/bronze/movie_elicitation_set.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

-- ==== user_recommendation_history ====
CREATE OR REPLACE EXTERNAL TABLE `netflix-project-czelusniak.netflix_raw.user_recommendation_history`
(
  userId STRING,
  tstamp STRING,
  movieId STRING,
  predictedRating STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://bucket-netflix-czelusniak/bronze/user_recommendation_history.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);
