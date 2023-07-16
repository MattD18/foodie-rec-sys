

SELECT 
  CURRENT_TIMESTAMP() as ts,
  place_id as restaurant_id,
  rating as restaurant_google_rating_float,
  name as restaurant_name_text,
  REGEXP_EXTRACT(formatted_address,r'1\d{4}') as restaurant_zipcode_enum,
FROM {{ ref('stg_restaurant_data__google_maps') }}
