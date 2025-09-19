with orders as (
    select * from {{ ref('stg_jaffle_shop__orders_refractor2') }}
),
customer_orders as (
    select 

        customer_id,
        min(order_placed_at) as first_order_date,
        max(order_placed_at) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by 1

)
select * from customer_orders