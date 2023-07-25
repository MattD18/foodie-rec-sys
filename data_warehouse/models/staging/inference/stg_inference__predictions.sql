select
    prediction_ts as prediction_inference_ts,
    prediction_id,
    model_id,
    user_id,
    JSON_EXTRACT_ARRAY(PARSE_JSON(restaurant_id_list)) as restaurant_id_list
from {{ source('inference','predictions') }}
