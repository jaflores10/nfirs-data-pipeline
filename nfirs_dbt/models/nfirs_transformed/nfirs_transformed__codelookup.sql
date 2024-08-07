/***** NFIRS Transformed Basic Incident Table Creation *****/
/***
No data transformations required. Table will be a copy of the raw table.
    
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH codelookup_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'codelookup') }}
),

codelookup_final AS (
    SELECT
        FIELDID,
        CODE_VALUE,
        CODE_DESCR
    FROM codelookup_source
)

SELECT * FROM codelookup_final