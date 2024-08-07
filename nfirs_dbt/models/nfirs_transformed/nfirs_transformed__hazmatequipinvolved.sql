/***** NFIRS Transformed Hazmat Material Equipment Involved Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH hazmatequipinvolved_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'hazmatequipinvolved') }}
),

hazmatequipinvolved_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE
    FROM hazmatequipinvolved_source
),

hazmatequipinvolved_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        VERSION,
        EQ_BRAND,
        EQ_MODEL,
        EQ_SER_NO,
        EQ_YEAR
    FROM hazmatequipinvolved_col_data_type_update
)

SELECT * FROM hazmatequipinvolved_final