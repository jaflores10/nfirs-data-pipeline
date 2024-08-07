/***** NFIRS Transformed Basic Aid Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH basicaid_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'basicaid') }}
),

basicaid_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE
    FROM basicaid_source
),

basicaid_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        NFIR_VER,
        FDIDRECAID,
        FDIDSTREC,
        INC_NOFDID
    FROM basicaid_col_data_type_update
)

SELECT * FROM basicaid_final