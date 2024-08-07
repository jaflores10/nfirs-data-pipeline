/***** NFIRS Transformed Arson Agency Referal Table Creation *****/
/***
The below model will transform the 'INC_DATE' column to DATE data type.
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH arsonagencyreferal_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'arsonagencyreferal') }}
),

arsonagencyreferal_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE
    FROM arsonagencyreferal_source    
),

arsonagencyreferal_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        AGENCY_NAM,
        VERSION,
        AG_ST_NUM,
        AG_ST_PREF,
        AG_STREET,
        AG_ST_TYPE,
        AG_ST_SUFF,
        AG_APT_NO,
        AG_CITY,
        AG_STATE,
        AG_ZIP5,
        AG_ZIP4,
        AG_PHONE,
        AG_CASE_NO,
        AG_ORI,
        AG_FID,
        AG_FDID
    FROM arsonagencyreferal_col_data_type_update
)

SELECT * FROM arsonagencyreferal_final