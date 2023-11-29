select * from {{ source('restaurant_data','google_maps_extra_cols') }}
