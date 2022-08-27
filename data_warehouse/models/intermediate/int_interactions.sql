{{ config(materialized='table') }}

select
    engagement_id,
    PARSE_DATETIME('%Y-%m-%d %H:%M:%S', ts) as ts,
    user_id,
    restaurant_id,
    engagement_type
from {{ ref('stg_application__interactions') }}