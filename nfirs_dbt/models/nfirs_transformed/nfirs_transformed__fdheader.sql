/***** NFIRS Transformed FD Header Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - NO_STATION --> FLOAT
    - NO_PD_FF --> FLOAT
    - NO_VOL_FF --> FLOAT
    - NO_VOL_PDC --> FLOAT
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH fdheader_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'fdheader') }}
),

fdheader_col_data_type_update AS (
    SELECT
        *,
        CAST(NO_STATION AS FLOAT) AS NO_STATION_UPDATE,
        CAST(NO_PD_FF AS FLOAT) AS NO_PD_FF_UPDATE,
        CAST(NO_VOL_FF AS FLOAT) AS NO_VOL_FF_UPDATE,
        CAST(NO_VOL_PDC AS FLOAT) AS NO_VOL_PDC_UPDATE
    FROM fdheader_source
),

fdheader_final AS (
    SELECT
        STATE,
        FDID,
        FD_NAME,
        FD_STR_NO,
        FD_STR_PRE,
        FD_STREET,
        FD_STR_TYP,
        FD_STR_SUF,
        FD_CITY,
        FD_ZIP,
        FD_PHONE,
        FD_FAX,
        FD_EMAIL,
        FD_FIP_CTY,
        NO_STATION_UPDATE AS NO_STATION,
        NO_PD_FF_UPDATE AS NO_PD_FF,
        NO_VOL_FF_UPDATE AS NO_VOL_FF,
        NO_VOL_PDC_UPDATE AS NO_VOL_PDC
    FROM fdheader_col_data_type_update
)

SELECT * FROM fdheader_final