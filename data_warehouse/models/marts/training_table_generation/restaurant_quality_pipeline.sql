{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "ds",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}


select
    CURRENT_DATE() as ds,
    '1' as user_id,
    restaurant_id,
    restaurant_google_rating_float,
    restaurant_name_text,
    r'1\d{4}' as restaurant_zipcode_enum
from {{ ref('google_maps_basic_features') }}