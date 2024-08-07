/***** NFIRS Transformed Hazardous Material Mobile Property Type Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH hazmobprop_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'hazmobprop') }}
),

hazmobprop_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE,
    FROM hazmobprop_source
),

hazmobprop_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        VERSION,
        MP_TYPE,
        MP_MAKE,
        MP_MODEL,
        MP_YEAR,
        MP_LICENSE,
        MP_STATE,
        MP_DOT_ICC
    FROM hazmobprop_col_data_type_update
)

SELECT * FROM hazmobprop_final