with google_maps_data as (
  select
    place_id,
    name,
    formatted_address,
    max_by(rating, updated_at) as google_maps_rating,
    max_by(geo, updated_at) as geo
  from {{ ref('stg_restaurant_data__google_maps_place_logs') }}
  group by 1, 2,3
)


select
  a.id,
  max(c.google_maps_rating) as ranking_quality_score,
  ARRAY_AGG(DISTINCT d.name IGNORE NULLS) as place_tags
from {{ ref('stg_application_test__dim_restaurant') }} a
left join {{ ref('stg_restaurant_data__restaurant_id_mapping') }} b
on a.id = b.application_id
left join google_maps_data c
on b.google_maps_id = c.place_id
left join {{ ref('stg_application_test__dim_place') }} d
on ST_CONTAINS(ST_GEOGFROMTEXT(d.geo), ST_GEOGFROMTEXT(c.geo))
group by 1
