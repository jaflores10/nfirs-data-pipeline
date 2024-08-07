/***** NFIRS Transformed Cause for Structure Fire Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH structurefirecauses_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'structurefirecauses') }}
),

structurefirecauses_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(LPAD(INC_DATE, 8, '0'), '%m%d%Y') AS DATE) AS INC_DATE_UPDATE
    FROM structurefirecauses_source
),

structurefirecauses_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        PCC,
        CAUSE_CODE,
        GCC
    FROM structurefirecauses_col_data_type_update
)

SELECT * FROM structurefirecauses_final