

SELECT 
  CURRENT_TIMESTAMP() as ts,
  a.user_id,
  array_concat_agg(b.restaurant_id_list order by prediction_logging_ts desc limit 2) as engagement_sms_impression_restaurant_last_2_prediction_lists
FROM {{ ref('stg_application__user_prediction_logging') }} a
LEFT JOIN {{ ref('stg_inference__predictions') }} b
on a.prediction_id = b.prediction_id
group by 1, 2
