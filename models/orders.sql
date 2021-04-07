with orders as (
    
select * from {{ ref('stg_orders') }}

),

payments as (

select * from {{ ref('stg_payments') }}

)


select order_id,
    customer_id,
    order_date,
    sum(amount_dollars) amount_dollars
from orders
    left join payments using (order_id)
where status = 'success'
group by 1, 2, 3