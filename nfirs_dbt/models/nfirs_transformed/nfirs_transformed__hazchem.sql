/***** NFIRS Transformed Hazardous Material Chemical Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
    - CONT_CAP --> FLOAT
    - AMOUNT_REL --> FLOAT
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH hazchem_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'hazchem') }}
),

hazchem_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE,
        CAST(CONT_CAP AS FLOAT) AS CONT_CAP_UPDATE,
        CAST(AMOUNT_REL AS FLOAT) AS AMOUNT_REL_UPDATE
    FROM hazchem_source
),

hazchem_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        SEQ_NUMBER,
        VERSION,
        UN_NUMBER,
        DOT_CLASS,
        CAS_REGIS,
        CHEM_NAME,
        CONT_TYPE,
        CONT_CAP_UPDATE AS CONT_CAP,
        CAP_UNIT,
        AMOUNT_REL_UPDATE AS AMOUNT_REL,
        UNITS_REL,
        PHYS_STATE,
        REL_INTO
    FROM hazchem_col_data_type_update
)

SELECT * FROM hazchem_final