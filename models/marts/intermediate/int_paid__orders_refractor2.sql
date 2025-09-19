with customers as (
    select
        *
    from {{ ref('stg_jaffle_shop__customers_refractor2') }}
),
orders as (
    select
       *
    from {{ ref('stg_jaffle_shop__orders_refractor2') }}
),
payments as (
    select 
       *

    from {{ ref('stg_stripe__payments_refractor2') }}
),
paid_orders as (
    select 

        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name
from orders

left join  payments on orders.order_id = payments.order_id

left join customers on orders.customer_id = customers.customer_id 

)
select * from paid_orders