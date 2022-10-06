select
    ds,
    id as engagement_id,
    restaurant_id,
    user_id,
    action as engagement_type,
    created_at,
from {{ ref('stg_application__interactions') }}