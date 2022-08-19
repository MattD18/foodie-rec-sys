with source as (

    select * from {{ source('application','users') }}

)

--https://docs.getdbt.com/guides/best-practices/how-we-structure/2-staging

select * from source
