{{ config(materialized='table') }}
--generate programatically (jinja or python)

select
    coalesce(a.ds) as ds,
    coalesce(a.restaurant_id) as restaurant_id,
    a.restaurant_zipcode_id,
    b.most_freq_user_home_zipcode_id
from {{ ref('object_sparse_restaurant_zipcode_id') }} a
full join {{ ref('object_sparse_most_freq_visit_user_home_zipcode_id') }} b
on a.ds = b.ds and a.restaurant_id = b.restaurant_id