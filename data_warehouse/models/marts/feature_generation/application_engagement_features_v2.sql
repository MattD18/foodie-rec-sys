
SELECT
  CURRENT_TIMESTAMP() as ts,
  user_id,
  array_agg(restaurant_id) as engagement_sms_impression_restaurant_list
FROM {{ ref('stg_application__fct_user_engagement') }}
where ds = (SELECT max(ds) FROM {{ ref('stg_application__fct_user_engagement') }})
and action = 'sms_impression'
group by 1, 2