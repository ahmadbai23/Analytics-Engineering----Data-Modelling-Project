with stg_employees as (
    select
        id as employee_id,
        company,
        last_name,
        first_name,
        email_address,
        job_title,
        business_phone,
        home_phone,
        mobile_phone,
        fax_number,
        address,
        city,
        state_province,
        zip_postal_code,
        country_region,
        web_page,
        notes,
        attachments,
        current_timestamp() as insertion_timestamp,
        ROW_NUMBER() OVER(PARTITION BY id) as row_number
    from
        {{ref("stg_employees")}}
)

select
    *
except
    (row_number)
from stg_employees
where row_number = 1
order by employee_id