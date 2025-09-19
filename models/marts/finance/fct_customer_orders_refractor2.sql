
with customer_orders as (
    select 
        *
    from  {{ ref('int_customer__orders_refractor2') }}   
),
paid_orders as (
    select 
        *
    from {{ ref('int_paid__orders_refractor2') }}
),
lifetime_value as (
    select * from {{ ref('int_customer_lifetime_value_refractor2') }}
),
final as(
    select
        p.*,
        row_number() over (order by p.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
        case when c.first_order_date = p.order_placed_at
        then 'new'
        else 'return' end as nvsr,
        x.clv_bad as customer_lifetime_value,
        c.first_order_date as fdos
from paid_orders p
left join customer_orders as c using (customer_id)
left outer join  lifetime_value x on x.order_id = p.order_id
order by order_id
)
select * from final