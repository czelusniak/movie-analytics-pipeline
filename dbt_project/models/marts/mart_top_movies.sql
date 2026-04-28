with
source as (
    select * from {{ ref('int_movie_kpi') }}
),

renamed as (
    select
        id_wrongggggggggg,
        title,
        genres,
        release_year,
        total_rating,
        round(avg_rating, 2) as avg_rating
    from source
    where
        total_rating >= 20
        and avg_rating between 0 and 5
)

select *
from renamed
order by avg_rating desc, total_rating desc
limit 10
