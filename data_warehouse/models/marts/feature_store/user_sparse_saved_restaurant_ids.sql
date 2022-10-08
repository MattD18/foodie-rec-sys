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

with saved_df as (
  select
    ds,
    engagement_id,
    restaurant_id,
    user_id
  from {{ ref('int_interactions') }}
  where engagement_type = 'save'
  and ds = (
      select max(ds) from {{ ref('int_interactions') }}
      where ds <= CAST('{{ var("ds") }}' as DATE)
    )
)  

SELECT
  CAST('{{ var("ds") }}' as DATE) as ds,
  user_id,
  user_saved_restaurant_ids,
  [
    STRUCT('int_interactions' as table_name, ds as table_ds)
  ] as source_table_ds
from (
  select
    ds,
    user_id,
    array_agg(restaurant_id) as user_saved_restaurant_ids,
  FROM saved_df
  group by 1, 2
)
  