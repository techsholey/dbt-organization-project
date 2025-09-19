with paid_orders as (
    select * from {{ ref('int_paid__orders_refractor2') }}
),
lifetime_value as (
    select
        order_id,
        sum(total_amount_paid) over (
            partition by customer_id 
            order by order_id
            rows between unbounded preceding 
            and current row
        ) as clv_bad

    from paid_orders
   
)
select * from lifetime_value