with final as (
    select id as customer_id,
           first_name as customer_first_name,
           last_name as customer_last_name 
    from {{ source('legacy', 'customers') }}
)

select * from final