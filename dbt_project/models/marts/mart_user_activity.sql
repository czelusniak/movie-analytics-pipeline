with
source as (
    select * from {{ ref('int_ratings_unified') }}
),

renamed as (
    select 
        user_id,
        count(*) AS total_ratings,
        count(DISTINCT movie_id) AS distinct_movies_rated,
        avg(rating) AS avg_rating,
        stddev(rating) AS std_rating,
        min(rating_ts) AS first_activity_ts,
        max(rating_ts) AS last_activity_ts
    from source
    group by user_id
)

select * from renamed
order by total_ratings desc, avg_rating desc