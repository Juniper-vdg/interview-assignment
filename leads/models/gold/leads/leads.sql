select
    null as accepts_financial_aid,
    null as ages_served,
    null as capacity,
    expiration_date as certificate_expiration_date,
    address as city, -- TODO parse from the address field
    address as address1, -- TODO parse from the address field
    address as address2, -- TODO parse from the address field
    name as company,
    phone,
    null as phone2,
    county,
    credential_type as curriculum_type,
    null as email,
    null as first_name,
    null as language,
    null as last_name,
    case when (expiration_date < current_date()) then 'EXPIRED' else 'ACTIVE' end as license_status,
    null as license_issued,
    credential_number as license_number,
    null as license_renewed,
    credential_type as license_type,
    primary_contact_name as licensee_name,
    null as max_age,
    null as min_age,
    case when (primary_contact_role == 'OPERATOR') then primary_contact_name else null end as operator,
    null as provider_id,
    null as schedule,
    state,
    name as title,
    null as website_address,
    null as zip, -- TODO parse from the address field
    null as facility_type
from {{ ref('source_1_current') }}

UNION
select
    accepts_subsidy as accepts_financial_aid,
    array_to_string(list_distinct([
        case when accepts_infant then 'Infants' else null end,
        case when accepts_toddler then 'Toddlers' else null end,
        case when accepts_preschool then 'Preschool' else null end,
        case when accepts_school then 'School-age' else null end,
    ]), ', ') as ages_served,
    total_cap as capacity,
    null as certificate_expiration_date,
    city, 
    address1, 
    null as address2, 
    company,
    phone,
    null as phone2,
    null as county,
    null as curriculum_type,
    email,
    null as first_name,
    null as language,
    null as last_name,
    null as license_status,
    null as license_issued,
    credential_number as license_number,
    null as license_renewed,
    credential_type as license_type,
    primary_caregiver as licensee_name,
    coalesce(
        case when accepts_school then '5 years-older' else null end,
        case when accepts_preschool then '24-48 months; 2-4 yrs.' else null end,
        case when accepts_toddler then '12-23 months; 1yr.' else null end,
        case when accepts_infant then '0-11 months' else null end
    ) as max_age,
    coalesce(
        case when accepts_infant then '0-11 months' else null end,
        case when accepts_toddler then '12-23 months; 1yr.' else null end,
        case when accepts_preschool then '24-48 months; 2-4 yrs.' else null end,
        case when accepts_school then '5 years-older' else null end
    ) as min_age,
    null as operator,
    null as provider_id,
    array_to_string([
        'Monday: ' || monday_hours,
        'Tuesday: ' || tuesday_hours,
        'Wednesday: ' || wednesday_hours,
        'Thursday: ' || thursday_hours,
        'Friday: ' || friday_hours,
        'Saturday: ' || saturday_hours,
        'Sunday: ' || sunday_hours,
    ], ', ') as schedule,
    state,
    company as title,
    null as website_address,
    zip,
    null as facility_type
from {{ ref('source_2_current') }}

UNION
select
    null as accepts_financial_aid,
    array_to_string(list_distinct([
        case when infant then 'Infants' else null end,
        case when toddler then 'Toddlers' else null end,
        case when preschool then 'Preschool' else null end,
        case when school then 'School-age' else null end,
    ]), ', ') as ages_served,
    capacity,
    null as certificate_expiration_date,
    city, 
    address as address1, 
    null as address2, 
    operation_name as company,
    phone,
    null as phone2,
    county,
    null as curriculum_type,
    email_address as email,
    null as first_name,
    null as language,
    null as last_name,
    status as license_status,
    issue_date as license_issued,
    operation as license_number,
    null as license_renewed,
    type as license_type,
    null as licensee_name,
    coalesce(
        case when school then '5 years-older' else null end,
        case when preschool then '24-48 months; 2-4 yrs.' else null end,
        case when toddler then '12-23 months; 1yr.' else null end,
        case when infant then '0-11 months' else null end
    ) as max_age,
    coalesce(
        case when infant then '0-11 months' else null end,
        case when toddler then '12-23 months; 1yr.' else null end,
        case when preschool then '24-48 months; 2-4 yrs.' else null end,
        case when school then '5 years-older' else null end
    ) as min_age,
    null as operator,
    null as provider_id,
    null as schedule,
    state,
    operation_name as title,
    null as website_address,
    zip,
    type as facility_type
from {{ ref('source_3_current') }}