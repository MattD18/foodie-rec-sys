

--build infrequently
--write python file to autogenerate lookup tables
with zipcodes as (
  select 
    distinct zip as zip_code
  from {{ ref('int_objects') }}
), lookup_table as (
  select
    zip_code,
    ROW_NUMBER() OVER (ORDER BY zip_code) AS id
  from zipcodes
)

select
  *
from lookup_table
union all
select 
  'UNK' as zip_code, 
  (select count(*) from lookup_table) + 1 as id

