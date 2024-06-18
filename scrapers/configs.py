restaurant_records_schema = [
  {
    "name": "restaurant_name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "description_list",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "review_link",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "cusine",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "perfect_for",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "price",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "neighborhood",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "address",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "rating",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "guide_link",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]


guide_record_schema = [
  {
    "name": "title",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "publish_date",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "description_list",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "guide_url",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  
]


guide_skip_list =[
  'https://www.theinfatuation.com/new-york/guides/best-diners-nyc'    
]


manual_guide_list = [
    'https://www.theinfatuation.com'
]