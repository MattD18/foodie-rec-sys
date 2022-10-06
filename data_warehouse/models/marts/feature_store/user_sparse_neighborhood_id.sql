
--ID of restaurant neighborhood
--as one-hot encoded categorical feature, or ID in embedding look-up table

--autogen lookup features with py file
--logical layer that generate feature for most recent partition of underlying data

with user_df as (
    select
        *
    from {{ ref('int_users') }}
    where ds = (select max(ds) from {{ ref('int_users') }})
), neighborhood_df as (
    select
        *
    from {{ ref('lookup_neighborhood_id') }}
)


select
  a.ds,
  a.user_id,
  case 
    when b.id is not null then b.id 
    else (select count(*) from {{ ref('lookup_neighborhood_id') }}) 
  end as user_neighborhood_id
from user_df a
left join neighborhood_df b
on a.neighborhood = b.neighborhood