select 
    ts as prediction_logging_ts,
    user_id,
    prediction_id
from {{ source('application','user_prediction_logging') }}