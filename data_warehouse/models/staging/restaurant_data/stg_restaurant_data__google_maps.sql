with source as (

    select * from {{ source('restaurant_data','google_maps') }}

)

select * from source
