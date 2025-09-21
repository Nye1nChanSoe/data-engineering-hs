{{ config(materialized='table') }}

select
  toInt32(customer_id)               as customer_id,
  initcap(first_name)                as first_name,
  initcap(last_name)                 as last_name,
  ifNull(house, 'Unknown')           as house,
  toDate(signup_date)                as signup_date
from {{ source('raw', 'customers') }}