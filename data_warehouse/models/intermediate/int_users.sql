select
    ds,
    id as user_id,
    neighborhood,
    saved_list,
    last_login,
    created_at
from {{ ref('stg_application__users') }}