select * from {{ source('restaurant_data','google_maps_place_logs') }}
