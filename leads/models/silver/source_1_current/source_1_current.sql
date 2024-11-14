select 
    name,
    credential_type,
    replace(credential_number, '-', '')::BIGINT as credential_number,
    status,
    strptime(expiration_date, '%m/%d/%Y')::date as expiration_date,
    (disciplinary_action == 'Y') as has_disciplinary_action,
    address,
    state,
    county,
    replace(phone, '-', '') as phone,
    strptime(first_issue_date, '%m/%d/%Y')::date as first_issue_date,
    primary_contact_name,
    primary_contact_role
from
{{ source('csv_sources', 'source_1_raw') }}
where _modification_date = (select max(_modification_date) from {{ source('csv_sources', 'source_1_raw') }})