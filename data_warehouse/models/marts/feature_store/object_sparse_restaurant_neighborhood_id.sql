


--ID of restaurant neighborhood
--as one-hot encoded categorical feature, or ID in embedding look-up table

--autogen lookup features with py file
--logical layer that generate feature for most recent partition of underlying data

with restaurant_df as (
    select
        *
    from {{ ref('int_objects') }}
    where ds = (select max(ds) from {{ ref('int_objects') }})
), neighborhood_df as (
    select
        *
    from {{ ref('lookup_neighborhood_id') }}
)


select
  a.ds,
  a.restaurant_id,
  case 
    when b.id is not null then b.id 
    else (select count(*) from {{ ref('lookup_neighborhood_id') }}) 
  end as restaurant_neighborhood_id
from restaurant_df a
left join neighborhood_df b
on a.neighborhood = b.neighborhood