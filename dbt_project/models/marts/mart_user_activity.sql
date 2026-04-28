with
source as (
    select * from {{ ref('int_ratings_unified') }}
),

renamed as (
    select
        user_id,
        count(*) as total_ratings,
        count(distinct movie_id) as distinct_movies_rated,
        avg(rating) as avg_rating,
        stddev(rating) as std_rating,
        min(rating_ts) as first_activity_ts,
        max(rating_ts) as last_activity_ts
    from source
    group by user_id
)

select * from renamed
order by total_ratings desc, avg_rating desc
