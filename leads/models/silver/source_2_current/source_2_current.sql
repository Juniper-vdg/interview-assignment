select 
    trim(split_part(type_license, '-', 0)) as credential_type,
    trim(split_part(type_license, '-', 1)) as credential_number,
    company,
    accepts_subsidy,
    (year_round == 'Year Round') as is_year_round,
    (daytime_hours == 'Daytime Hours') as is_daytime_hours,
    left(star_level,1) as star_level,
    mon as monday_hours,
    tues as tuesday_hours,
    wed as wednesday_hours,
    thurs as thursday_hours,
    friday as friday_hours,
    saturday as saturday_hours,
    sunday as sunday_hours,
    primary_caregiver,
    regexp_replace(phone, '[-\s\(\)]', '', 'g') as phone,
    email,
    address1,
    city,
    state,
    zip::varchar as zip,
    subsidy_contract_number,
    total_cap,
    list_contains([ages_accepted_1, aa2, aa3, aa4], 'Infants (0-11 months)') as accepts_infant,
    list_contains([ages_accepted_1, aa2, aa3, aa4], 'Toddlers (12-23 months; 1yr.)') as accepts_toddler,
    list_contains([ages_accepted_1, aa2, aa3, aa4], 'Preschool (24-48 months; 2-4 yrs.)') as accepts_preschool,
    list_contains([ages_accepted_1, aa2, aa3, aa4], 'School-age (5 years-older)') as accepts_school,
    strptime(right(license_monitoring_since, 10), '%Y/%m/%d')::date as license_monitoring_since
from
{{ source('csv_sources', 'source_2_raw') }}
where _modification_date = (select max(_modification_date) from {{ source('csv_sources', 'source_2_raw') }})