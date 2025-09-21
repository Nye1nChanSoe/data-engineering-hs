-- Configure how dbt should build this model in ClickHouse
-- materialized='incremental'   → Only update recent data instead of full rebuild
-- unique_key='order_id'        → The natural key for deduplication
-- incremental_strategy=...     → Strategy for updating rows (delete then insert)
-- engine='MergeTree()'         → ClickHouse storage engine
-- order_by=['order_id']        → Sorting key for MergeTree (optimizes queries on order_id)
-- partition_by=...             → Partition table by month of order_ts; each partition = one directory on disk

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    engine='MergeTree()',
    order_by=['order_id'],
    partition_by=['toYYYYMM(order_ts)']
) }}


-- CTE
with o as (
  select * from {{ ref('stg_orders') }}
  {% if is_incremental() %}
    -- only reprocess recent data on incremental runs
    where order_ts >= now() - INTERVAL 30 DAY
  {% endif %}
),
p as (
  select order_id, sum(paid_galleons) as total_paid_galleons
  from {{ ref('stg_payments') }}
  {% if is_incremental() %}
    where paid_at >= now() - INTERVAL 30 DAY
  {% endif %}
  group by order_id
)

select
  o.order_id,
  o.customer_id,
  o.order_ts,
  o.item,
  o.status,
  o.amount_galleons,
  ifNull(p.total_paid_galleons, toDecimal64(0,2)) as total_paid_galleons,
  (o.amount_galleons - ifNull(p.total_paid_galleons, toDecimal64(0,2))) as outstanding_galleons
from o
left join p using (order_id)