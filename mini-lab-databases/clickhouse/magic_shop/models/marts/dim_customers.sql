{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['customer_id']
) }}

select
  c.customer_id,
  concat(c.first_name, ' ', c.last_name) as full_name,
  c.house,
  c.signup_date
from {{ ref('stg_customers') }} c