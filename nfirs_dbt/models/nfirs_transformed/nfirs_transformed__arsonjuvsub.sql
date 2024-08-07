/***** NFIRS Transformed Arson Juvenile Subject Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
    - AGE --> INTEGER
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH arsonjuvsub_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'arsonjuvsub') }}
),

arsonjuvsub_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE,
        CAST(AGE AS INT) AS AGE_UPDATE
    FROM arsonjuvsub_source
),

arsonjuvsub_final AS (
    SELECT
        INCIDENT_KEY,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        SUB_SEQ_NO,
        VERSION,
        AGE_UPDATE AS AGE,
        GENDER,
        RACE,
        ETHNICITY,
        FAM_TYPE,
        RISK_FACT1,
        RISK_FACT2,
        RISK_FACT3,
        RISK_FACT3,
        RISK_FACT4,
        RISK_FACT5,
        RISK_FACT6,
        RISK_FACT7,
        RISK_FACT8,
        JUV_DISPO
    FROM arsonjuvsub_col_data_type_update
)

SELECT * FROM arsonjuvsub_final