with cst as (select * from {{ ref('stg_customers') }}),

ord as (select * from {{ ref('stg_orders') }}),

pay as (
    select order_id, 
           max(payment_date) as payment_finalized_date,
           sum(amount) as total_amount_paid
    from {{ ref('stg_payments') }}
    where payment_status <> 'fail'
    group by 1
),

paid_orders as (
    select ord.order_id,
           ord.customer_id,
           ord.order_placed_at,
           ord.order_status,
           pay.total_amount_paid,
           pay.payment_finalized_date,
           cst.customer_first_name,
           cst.customer_last_name
    from ord
    left join pay using (order_id)
    left join cst using (customer_id)
),

x_tbl as (
    select p1.order_id,
           sum(p2.total_amount_paid) as clv_bad 
    from paid_orders p1
    left join paid_orders p2 on p1.customer_id = p2.customer_id
          and p1.order_id>=p2.order_id
    group by 1
    order by p1.order_id 
),

customer_orders as (
    select cst.customer_id,
           min(ord.order_placed_at) as first_order_date,
           max(order_placed_at) as most_recent_order_date,
           count(ord.order_id) as number_of_orders  
    from cst 
    left join ord using(customer_id)
    group by 1
),

final as (
    select pay.*,
           row_number() over (order by pay.order_id) as transaction_seq,
           row_number() over (partition by customer_id order by pay.order_id) as customer_sales_seq,
           case
             when customer_orders.first_order_date = pay.order_placed_at
             then 'new'
             else 'return'
           end as nvsr,
           x_tbl.clv_bad as customer_lifetime_value,
           customer_orders.first_order_date as fdos
    from paid_orders
    left join customer_orders using (customer_id)
    left join x_tbl using (order_id)
    order by customer_orders.order_id
)

select * from final


