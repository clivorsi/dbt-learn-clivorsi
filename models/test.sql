with
payments as (
  select * from {{ ref('stg_payments') }}
)
select 
  order_id,
  {% set payment_methods = dbt_utils.get_column_values(table=ref('stg_payments'), column='payment_method') %}
  {%- for method in payment_methods %}
  sum(case when payment_method = '{{ method }}' then amount_dollars else 0 end) as {{ method }}_amount
  {%- if not loop.last %},{% endif %}
  {%- endfor %}
from payments
group by 1