/***** NFIRS Transformed EMS Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
    - ARRIVAL --> DATETIME
    - TRANSPORT --> DATETIME
    - AGE --> INTEGER
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH ems_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'ems') }}
),

ems_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE,
        STRPTIME(ARRIVAL, '%m%d%Y%H%M') AS ARRIVAL_UPDATE,
        STRPTIME(TRANSPORT, '%m%d%Y%H%M') AS TRANSPORT_UPDATE,
        CAST(AGE AS INT) AS AGE_UPDATE
    FROM ems_source
),

ems_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        PATIENT_NO,
        VERSION,
        ARRIVAL_UPDATE AS ARRIVAL,
        TRANSPORT_UPDATE AS TRANSPORT,
        PROVIDER_A,
        AGE_UPDATE,
        GENDER,
        RACE,
        ETH_EMS,
        HUM_FACT1,
        HUM_FACT2,
        HUM_FACT3,
        HUM_FACT4,
        HUM_FACT5,
        HUM_FACT6,
        HUM_FACT7,
        HUM_FACT8,
        OTHER_FACT,
        SITE_INJ1,
        SITE_INJ2,
        SITE_INJ3,
        SITE_INJ4,
        SITE_INJ5,
        INJ_TYPE1,
        INJ_TYPE2,
        INJ_TYPE3,
        INJ_TYPE4,
        INJ_TYPE5,
        CAUSE_ILL,
        PROC_USE1,
        PROC_USE2,
        PROC_USE3,
        PROC_USE4,
        PROC_USE5,
        PROC_USE6,
        PROC_USE7,
        PROC_USE8,
        PROC_USE9,
        PROC_USE10,
        PROC_USE11,
        PROC_USE12,
        PROC_USE13,
        PROC_USE14,
        PROC_USE15,
        PROC_USE16,
        PROC_USE17,
        PROC_USE18,
        PROC_USE19,
        PROC_USE20,
        PROC_USE21,
        PROC_USE22,
        PROC_USE23,
        PROC_USE24,
        PROC_USE25,
        SAFE_EQP1,
        SAFE_EQP2,
        SAFE_EQP3,
        SAFE_EQP4,
        SAFE_EQP5,
        SAFE_EQP6,
        SAFE_EQP7,
        SAFE_EQP8,
        ARREST,
        ARR_DES1,
        ARR_DES2,
        AR_RHYTHM,
        IL_CARE,
        HIGH_CARE,
        PAT_STATUS,
        PULSE,
        EMS_DISPO,
    FROM ems_col_data_type_update
)

SELECT * FROM ems_final