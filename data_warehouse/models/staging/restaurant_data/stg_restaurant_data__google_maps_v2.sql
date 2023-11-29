
SELECT 
  a.*,
  b.geo,
  c.id as neighborhood_id,
  c.name as neighborhood_name,
  c.borough
FROM {{ source('restaurant_data','google_maps') }} a
LEFT JOIN {{ source('restaurant_data','google_maps_extra_cols') }} b
on a.place_id = b.place_id
LEFT JOIN {{ source('restaurant_data','neighborhoods') }} c
on ST_CONTAINS(c.geo, b.geo)
