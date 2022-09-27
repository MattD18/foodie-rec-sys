{{ config(materialized='table') }}
--generate programatically (jinja or python)

--need guarantee that all users are in table
--left join on full user table, need to figure out timestamps
select
    coalesce(a.ds, b.ds) as ds,
    coalesce(a.user_id, b.user_id) as user_id,
    a.home_zipcode_id,
    b.list_impression_r7d_restaurant_ids
from {{ ref('user_sparse_home_zipcode_id') }} a
full join {{ ref('user_sparse_list_impression_r7d_restaurant_ids') }} b
on a.ds = b.ds and a.user_id = b.user_id