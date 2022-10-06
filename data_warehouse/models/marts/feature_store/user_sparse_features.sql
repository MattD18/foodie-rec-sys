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
--start with user table
--for given ds join all user sparse features

with user_df as (
    select
        user_id
    from {{ ref('int_users') }}
    where ds = (select max(ds) from {{ ref('int_users') }})
)

select
    CURRENT_DATE('America/New_York') as ds,
    a.user_id,
    b.user_neighborhood_id
from user_df a
left join {{ ref('user_sparse_neighborhood_id') }} b
on a.user_id = b.user_id