with source as (

    select * from {{ source('application','objects') }}

)

select * from source
