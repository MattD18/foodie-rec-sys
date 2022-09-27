select
    CURRENT_DATE() as ds,
    user_id,
    array_agg(distinct restaurant_id) as list_impression_r7d_restaurant_ids
from {{ ref('int_interactions') }}
where ts >= datetime_sub(current_datetime(), INTERVAL 7 DAY)
and engagement_type = 'impression'
group by 1, 2