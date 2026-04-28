with
source as (
    select * from {{ source('raw', 'movies') }}
),

renamed as (
    select
        try_cast(movieid as integer) as movie_id,
        trim(regexp_replace(title, '\(\d{4}\)', '')) as title,
        trim(genres) as genres,
        try_cast(trim(regexp_extract(title, '\((\d{4})\)\s*$', 1)) as integer) as release_year
    from source
)

select * from renamed
