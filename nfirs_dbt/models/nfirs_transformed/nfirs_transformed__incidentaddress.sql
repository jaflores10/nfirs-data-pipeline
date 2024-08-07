/***** NFIRS Transformed Incident Address Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH incidentaddress_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'incidentaddress') }}
),

incidentaddress_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE
    FROM incidentaddress_source
),

incidentaddress_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        LOC_TYPE,
        NUM_MILE,
        STREET_PRE,
        STREETNAME,
        STREETTYPE,
        STREETSUF,
        APT_NO,
        CITY,
        STATE_ID,
        ZIP5,
        ZIP4,
        X_STREET
    FROM incidentaddress_col_data_type_update
)

SELECT * FROM incidentaddress_final