with orders as (
    select  
        *
    from dbt-tutorial.jaffle_shop.orders
),
transformed as (
    select
        id as order_id,
        user_id	as customer_id,
        order_date as order_placed_at,
        status as order_status
    from orders
)
select * from transformed