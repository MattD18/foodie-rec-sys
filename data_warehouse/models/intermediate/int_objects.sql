select
    ds,
    id as restaurant_id,
    name,
    created_at,
    address,
    zipcode,
    neighborhood,
    cuisine,
    tags,
    price_est,
    website_url,
    menu_url,
from {{ ref('stg_application__objects') }}