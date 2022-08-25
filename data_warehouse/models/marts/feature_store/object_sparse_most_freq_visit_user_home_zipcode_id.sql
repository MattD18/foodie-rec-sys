with df as (
  select 
    a.restaurant_id,
    case 
      when c.id is not null then c.id 
      else (select count(*) from {{ ref('lookup_zipcode_id') }}) 
    end as user_home_zipcode_id
  from {{ ref('int_interactions') }} a
  left join {{ ref('int_users') }} b
  on a.user_id = b.user_id
  left join {{ ref('lookup_zipcode_id') }} c
  on cast(b.home_zipcode as string) = c.zip_code
  where a.engagement_type = 'visit'
)


select
    CURRENT_DATE() as ds,
    restaurant_id,
    APPROX_TOP_COUNT(user_home_zipcode_id, 1)[OFFSET(0)].value as most_freq_user_home_zipcode_id
from df
group by 1, 2