with source_int_ratings_unified as (
    select * from {{ ref('int_ratings_unified') }}
),

source_stg_movies as (
    select * from {{ ref('stg_movies') }}
),

renamed as (
    select
        r.movie_id,
        m.title,
        m.genres,
        m.release_year,
        count(*) as total_rating,
        avg(r.rating) as avg_rating,
        stddev(r.rating) as std_rating,
        min(r.rating_ts) as first_rating_ts,
        max(r.rating_ts) as last_rating_ts
    from source_int_ratings_unified as r
    left join source_stg_movies as m -- left join keeps all ratings, even for movies absent from stg_movies
        on r.movie_id = m.movie_id
    --where m.title IS NOT NULL
    -- ~10k movie_ids in int_ratings_unified have no match in stg_movies (orphan ratings kept by design)
    -- uncomment the where clause above to exclude orphan ratings from the analysis
    group by r.movie_id, m.title, m.genres, m.release_year
    -- order by avg_rating desc -- only marts should be ordered, not intermediate models
)

select * from renamed
