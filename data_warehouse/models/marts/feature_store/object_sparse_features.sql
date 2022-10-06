{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "ds",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}
--generate programatically (jinja or python)
--build incrementally
--start with restaurant table
--for given ds join all restaurant sparse features

with restaurant_df as (
    select
        restaurant_id
    from {{ ref('int_objects') }}
    where ds = (select max(ds) from {{ ref('int_objects') }})
)

select
    CURRENT_DATE('America/New_York') as ds,
    a.restaurant_id,
    b.restaurant_neighborhood_id
from restaurant_df a
left join {{ ref('object_sparse_restaurant_neighborhood_id') }} b
on a.restaurant_id = b.restaurant_id