-- endpoint to train P(like|impression model)


with impressions as (
  select 
    *
  from {{ ref('int_interactions') }}
  where engagement_type = 'impression'
), likes as (
  select
    *
  from {{ ref('int_interactions') }}
  where engagement_type = 'like'
), df as (
    select
    engagement_id,
    ts,
    user_id,
    restaurant_id,
    case when sum(label) > 0 then 1 else 0 end as label
    from (
    select 
        a.engagement_id,
        a.ts,
        a.user_id,
        a.restaurant_id,
        case when b.engagement_type is not null then 1 else 0 end as label
    from impressions a
    left join likes b
    on a.user_id = b.user_id 
        and a.restaurant_id = b.restaurant_id
        and a.ts <= b.ts
        and DATETIME_ADD(a.ts, INTERVAL 30 SECOND) >= b.ts
    ) group by 1, 2, 3 , 4
)




select
    CURRENT_DATE() as ds,
    *
from df 
order by ts