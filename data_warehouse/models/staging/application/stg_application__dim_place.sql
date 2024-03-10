select 
    *
from {{ source('application_prod','dim_place') }}