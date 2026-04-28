with user_rating_history as (
    select
        user_id,
        movie_id,
        rating,
        rating_ts,
        'user_rating_history' as src
    from {{ ref('stg_user_rating_history') }}
),

ratings_for_additional_users as (
    select
        user_id,
        movie_id,
        rating,
        rating_ts,
        'ratings_for_additional_users' as src
    from {{ ref('stg_ratings_for_additional_users') }}
),

all_ratings as (
    select * from user_rating_history
    union all
    select * from ratings_for_additional_users
),

filtered as (
    select
        user_id,
        movie_id,
        rating,
        rating_ts,
        src
    from all_ratings
    where
        user_id is not null
        and movie_id is not null
        and rating is not null
        and rating_ts is not null
        and src is not null
        and rating between 0 and 5  --filtering out invalid ratings such as -1
)

select * from filtered
