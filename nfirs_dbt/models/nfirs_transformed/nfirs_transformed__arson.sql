/***** NFIRS Transformed Arson Table Creation *****/
/***
The below model will transform the 'INC_DATE' column to DATE data type.
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH arson_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'arson') }}
),

arson_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE
    FROM arson_source
),

arson_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        VERSION,
        CASE_STAT,
        AVAIL_MFI,
        MOT_FACTS1,
        MOT_FACTS2,
        MOT_FACTS3,
        GRP_INVOL1,
        GRP_INVOL2,
        GRP_INVOL3,
        ENTRY_METH,
        EXT_FIRE,
        DEVI_CONT,
        DEVI_IGNIT,
        DEVI_FUEL,
        INV_INFO1,
        INV_INFO2,
        INV_INFO3,
        INV_INFO4,
        INV_INFO5,
        INV_INFO6,
        INV_INFO7,
        INV_INFO8,
        PROP_OWNER,
        INIT_OB1,
        INIT_OB2,
        INIT_OB3,
        INIT_OB4,
        INIT_OB5,
        INIT_OB6,
        INIT_OB7,
        INIT_OB8,
        LAB_USED1,
        LAB_USED2,
        LAB_USED3,
        LAB_USED4,
        LAB_USED5,
        LAB_USED6
    FROM arson_col_data_type_update
)

SELECT * FROM arson_final