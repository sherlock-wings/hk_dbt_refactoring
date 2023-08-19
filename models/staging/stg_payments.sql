with final as(
    select id as payment_id,
           orderid as order_id,
           created as payment_date,
           paymentmethod as payment_method,
           status as payment_status,
           round(amount/100,2) as amount
    from {{ source('legacy', 'payments') }}
)

select * from final