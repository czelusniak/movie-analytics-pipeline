with
source_movies as (
    select * from {{ ref('stg_movies') }}
),

source_ratings as (
    select * from {{ ref('int_ratings_unified') }}
),

filtered_movies as (
    select *
    from source_movies
    where
        genres is not null
        and genres != '(no genres listed)'

),

exploded as (
    select
        genre,
        r.rating
    from filtered_movies as m
    inner join source_ratings as r -- filtering out movies without ratings
        on m.movie_id = r.movie_id
    cross join unnest(string_split(genres, '|')) as t(genre)
),

renamed as (
    select
        genre,
        count(*) as total_ratings,
        avg(rating) as avg_rating,
        stddev(rating) as std_rating
    from exploded
    group by genre

)

select * from renamed
where genre != ''
order by total_ratings desc, avg_rating desc
