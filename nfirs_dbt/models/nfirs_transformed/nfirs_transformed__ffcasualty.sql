/***** NFIRS Transformed Firefighter Casualty Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
    - AGE --> INTEGER
    - INJ_DATE --> DATETIME
    - RESPONSES --> FLOAT
    - STORY --> FLOAT
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH ffcasualty_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'ffcasualty') }}
),

ffcasualty_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE,
        CAST(AGE AS INT) AS AGE_UPDATE,
        STRPTIME(INJ_DATE, '%m%d%Y%H%M') AS INJ_DATE_UPDATE,
        CAST(RESPONSES AS FLOAT) AS RESPONSES_UPDATE,
        CAST(STORY AS FLOAT) AS STORY_UPDATE
    FROM ffcasualty_source
),

ffcasualty_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        FF_SEQ_NO,
        VERSION,
        GENDER,
        CAREER,
        AGE_UPDATE AS AGE,
        INC_DATE_UPDATE AS INJ_DATE,
        RESPONSES_UPDATE AS RESPONSES,
        ASSIGNMENT,
        PHYS_COND,
        SEVERITY,
        TAKEN_TO,
        ACTIVITY,
        SYMPTOM,
        PABI,
        CAUSE,
        FACTOR,
        OBJECT,
        WIO,
        RELATION,
        STORY_UPDATE AS STORY,
        LOCATION,
        VEHICLE,
        PROT_EQP
    FROM ffcasualty_col_data_type_update
)

SELECT * FROM ffcasualty_final