{{ config(materialized='table') }}

select
  toInt32(payment_id)                as payment_id,
  toInt32(order_id)                  as order_id,
  lower(method)                      as method,
  toDecimal64(paid_galleons, 2)      as paid_galleons,
  toDateTime(paid_at)                as paid_at
from {{ source('raw', 'payments') }}