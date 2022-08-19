with source as (

    select * from {{ source('application','interactions') }}

)

select * from source
