{{ config(materialized='table') }}

select * from {{ ref('stg_application__objects') }}