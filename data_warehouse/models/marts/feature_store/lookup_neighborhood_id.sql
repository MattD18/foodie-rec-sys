

{{ config(materialized='table') }}

--build infrequently
--write python file to autogenerate lookup tables
with neighborhoods as (
  select
    Name as neighborhood
  from {{ ref('neighborhoods')}}
), lookup_table as (
  select
    neighborhood,
    ROW_NUMBER() OVER (ORDER BY neighborhood) AS id
  from neighborhoods
)

select
  *
from lookup_table
union all
select 
  'UNK' as neighborhood, 
  (select count(*) from lookup_table) + 1 as id

