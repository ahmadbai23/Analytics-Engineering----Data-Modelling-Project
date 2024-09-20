with stg_products as (
select
    p.id as product_id,
    p.product_code,
    p.product_name,
    p.description,
    s.company as supplier_company,
    p.standard_cost,
    p.list_price,
    p.reorder_level,
    p.target_level,
    p.quantity_per_unit,
    p.discontinued,
    p.minimum_reorder_quantity,
    p.category,
    p.attachments,
    current_timestamp() as insertion_timestamp,
    ROW_NUMBER() OVER(PARTITION BY p.id) as row_number
from
    {{ref ("stg_products")}} p
left join
    {{ref ("stg_suppliers")}} s on p.supplier_ids = s.id
)

select
    *
except
    (row_number)
from stg_products
where row_number = 1
order by product_id