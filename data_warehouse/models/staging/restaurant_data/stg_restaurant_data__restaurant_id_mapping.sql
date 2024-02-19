select * from {{ source('restaurant_data','restaurant_id_mapping') }}
