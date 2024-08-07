/***** NFIRS Municipal Analysis Table Creation *****/
/*** 
The below model will select specific columns from 'nfirs_processed__incidents' to build visuals in Tableau
***/

{{ config(
    materialized='table',
    schema='analysis'
) }}

WITH processed_incidents_source AS (
    SELECT * FROM {{ ref('nfirs_processed__incidents') }}
),

muni_incidents_final AS (
    SELECT
        incident_key,
        inc_date,
        DAYNAME(inc_date) AS week_day,
        CASE
            WHEN MONTH(inc_date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(inc_date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(inc_date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(inc_date) IN (9, 10, 11) THEN 'Fall'
        END AS season,
        inc_no,
        CASE
            WHEN state IN ('ME', 'NH', 'VT', 'MA', 'CT', 'RI', 'NY', 'NJ', 'PA', 'MD', 'DE', 'DC') THEN 'Northeast'
            WHEN state IN ('WV', 'VA', 'KY', 'TN', 'NC', 'SC', 'AR', 'LA', 'MS', 'AL', 'GA', 'FL') THEN 'Southeast'
            WHEN state IN ('OH', 'MI', 'IN', 'WI', 'IL', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'Midwest'
            WHEN state IN ('OK', 'TX', 'NM', 'AZ') THEN 'Southwest'
            WHEN state IN ('MT', 'WY', 'CO', 'ID', 'UT', 'WA', 'OR', 'NV', 'CA', 'AK', 'HI') THEN 'West'
            ELSE 'Other'
        END AS region,
        state,
        fd_name,
        fd_city,
        fd_zip,
        streetname,
        streettype,
        streetsuf,
        apt_no,
        city,
        state_id,
        zip5
    FROM processed_incidents_source
)

SELECT * FROM muni_incidents_final