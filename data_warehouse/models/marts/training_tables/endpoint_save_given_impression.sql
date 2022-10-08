--logical view just calculates impressions from the previous day
with impressions as (
  --in future change impressions to log once per day
  --for now aggreagate to once per day in endpoint def
  select 
    ds,
    extract(date from created_at) as engagement_ds,
    min(engagement_id) as engagement_id,
    restaurant_id,
    user_id,
    any_value(engagement_type) as engagement_type,
    min(created_at) as engagement_ts
  from {{ ref('int_interactions') }}
  where engagement_type = 'impression'
  and ds = (select max(ds) from {{ ref('int_interactions') }})
  group by 1, 2, 4, 5
), saves as (
  select
    ds,
    extract(date from created_at) as engagement_ds,
    engagement_id,
    restaurant_id,
    user_id,
    engagement_type,
    created_at as engagement_ts
  from {{ ref('int_interactions') }}
  where engagement_type = 'save'
  and ds = (select max(ds) from {{ ref('int_interactions') }})
), df as (
  --core logic assumes we log one impression a day, and attribute same day save to that impression
  --doesn't take into account someone viewing restaurant multiple times in same day in different contexts
  --fine for now, later will want to include real-time or intra-day context
  select 
      a.ds,
      a.engagement_id,
      a.engagement_ds,
      a.user_id,
      a.restaurant_id,
      case when b.engagement_type is not null then 1 else 0 end as label
  from impressions a
  left join saves b
  on a.user_id = b.user_id 
      and a.restaurant_id = b.restaurant_id
      and a.engagement_ds = b.engagement_ds
      and a.engagement_ts < b.engagement_ts

)




select
    *
from df