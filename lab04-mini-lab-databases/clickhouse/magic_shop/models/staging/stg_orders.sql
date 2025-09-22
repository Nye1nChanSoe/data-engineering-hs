{{ config(materialized='table') }}

select
  toInt32(order_id)                  as order_id,
  toInt32(customer_id)               as customer_id,
  toDateTime(order_ts)               as order_ts,
  item                               as item,
  lower(status)                      as status,
  toDecimal64(amount_galleons, 2)    as amount_galleons
from {{ source('raw', 'orders') }}