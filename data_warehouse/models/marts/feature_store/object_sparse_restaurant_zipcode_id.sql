

--ID of restaurant neighborhood
--as one-hot encoded categorical feature, or ID in embedding look-up table

--autogen lookup features with py file
--need to align ds with creation date, underlying data

select
  CURRENT_DATE() as ds,
  a.restaurant_id,
  case 
    when b.id is not null then b.id 
    else (select count(*) from {{ ref('lookup_zipcode_id') }}) 
  end as restaurant_zipcode_id
from {{ ref('int_objects') }} a
left join {{ ref('lookup_zipcode_id') }} b
on a.zip = b.zip_code