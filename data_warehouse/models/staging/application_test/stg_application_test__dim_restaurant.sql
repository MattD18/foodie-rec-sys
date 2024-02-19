select 
    *
from {{ source('application_test','dim_restaurant') }}