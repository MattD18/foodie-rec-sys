

SELECT 
  distinct
  CURRENT_TIMESTAMP() as ts,
  a.id,
  a.google_maps_id,
  a.name,
  b.rating as google_maps_rating,
  b.neighborhood_id as neighborhood_id
FROM  {{ ref('stg_application__dim_restaurant') }} a
left join {{ ref('stg_restaurant_data__google_maps_v2') }} b
on a.google_maps_id = b.place_id
