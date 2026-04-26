with
source as (
    select * from {{ source('raw', 'user_rating_history') }}
),

renamed as (
    select
        try_cast(userId as bigint)  as user_id,
        try_cast(movieId as bigint) as movie_id,
        try_cast(rating as double)  as rating,
        coalesce(
            try_strptime(cast(tstamp as varchar), '%Y-%m-%d %H:%M:%S%z'),
            try_strptime(cast(tstamp as varchar), '%Y-%m-%d %H:%M:%S')) as rating_ts
    from source
)

select * from renamed