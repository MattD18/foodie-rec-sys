{{ config(materialized='table') }}

--build incrementally by ds moving forward
select 
    a.ds,
    a.user_id,
    a.restaurant_id,
    b.*  except (ds, restaurant_id),
    a.label
from {{ ref('endpoint_like_given_impression') }} a
left join {{ ref('object_sparse_features') }} b
on a.restaurant_id = b.restaurant_id