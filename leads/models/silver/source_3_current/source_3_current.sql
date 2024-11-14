select 
    operation,
    operation_name,
    address,
    city,
    state,
    zip,
    county,
    regexp_replace(phone, '[-\s]', '', 'g') as phone,
    type,
    status,
    strptime(issue_date, '%m/%d/%Y')::date as issue_date,
    capacity,
    email_address,
    facility_id,
    (infant == 'Y') as infant,
    (toddler == 'Y') as toddler,
    (preschool == 'Y') as preschool,
    (school == 'Y') as school
from
{{ source('csv_sources', 'source_3_raw') }}
where _modification_date = (select max(_modification_date) from {{ source('csv_sources', 'source_3_raw') }})