/***** NFIRS Transformed Fire Service Equipment Failure Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH ffequipfail_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'ffequipfail') }}
),

ffequipfail_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE
    FROM ffequipfail_source
),

ffequipfail_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        CAS_SEQ_NO,
        EQP_SEQ_NO,
        VERSION,
        EQUIP_ITEM,
        EQP_PROB,
        EQP_MAN,
        EQP_MOD,
        EQP_SER_NO
    FROM ffequipfail_col_data_type_update
)

SELECT * FROM ffequipfail_final