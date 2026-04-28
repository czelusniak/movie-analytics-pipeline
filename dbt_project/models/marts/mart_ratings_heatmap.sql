with
source_ratings_unified as (
    select * from {{ ref('int_ratings_unified') }}
),

aggregated as (
    select
        extract(year from rating_ts) as year,
        extract(month from rating_ts) as month_number,
        strftime(rating_ts, '%b') as month_name,
        count(*) as total_ratings
    from source_ratings_unified
    group by year, month_number, month_name
)

select * from aggregated
order by year, month_number
