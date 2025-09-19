with payments as (
    select
        *
    from dbt-tutorial.stripe.payment
),
transformed as (
    select 

        orderid as order_id, 
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid

    from payments
    where status <> 'fail'
    group by 1 
)
select * from transformed