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
--ID of restaurant neighborhood
--as one-hot encoded categorical feature, or ID in embedding look-up table

--autogen lookup features with py file
--logical layer that generate feature for most recent partition of underlying data

with restaurant_df as (
    select
        *
    from {{ ref('int_objects') }}
    where ds = (
      select max(ds) from {{ ref('int_objects') }}
      where ds <= CAST('{{ var("ds") }}' as DATE)
    )
), neighborhood_df as (
    select
        *
    from {{ ref('lookup_neighborhood_id') }}
)


select
  CAST('{{ var("ds") }}' as DATE) as ds,
  a.restaurant_id,
  [
    case 
      when b.id is not null then b.id 
      else (select count(*) from neighborhood_df) 
    end
  ] as restaurant_neighborhood_id,
  [
    STRUCT('int_objects' as table_name, a.ds as table_ds),
    STRUCT('lookup_neighborhood_id' as table_name, b.ds as table_ds)
  ] as source_table_ds
from restaurant_df a
left join neighborhood_df b
on a.neighborhood = b.neighborhood