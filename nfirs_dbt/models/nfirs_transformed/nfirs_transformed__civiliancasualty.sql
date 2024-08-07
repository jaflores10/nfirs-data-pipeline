/***** NFIRS Transformed Civilian Casualty Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
    - AGE --> INTEGER
    - INJ_DT_TIM --> DATETIME
    - STORY_INC --> FLOAT
    - STOR_INJ --> FLOAT
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH civiliancasualty_source AS (
    SELECT * FROM {{ source ('nfirs_raw', 'civiliancasualty') }}
),

civiliancasualty_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE,
        CAST(AGE AS INT) AS AGE_UPDATE,
        STRPTIME(INJ_DT_TIM, '%m%d%Y%H%M') AS INJ_DT_TIM_UPDATE,
        CAST(STORY_INC AS FLOAT) AS STORY_INC_UPDATE,
        CAST(STORY_INJ AS FLOAT) AS STORY_INJ_UPDATE
    FROM civiliancasualty_source
),

civiliancasualty_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        SEQ_NUMBER,
        VERSION,
        GENDER,
        AGE_UPDATE AS AGE,
        RACE,
        ETHNICITY,
        AFFILIAT,
        INJ_DT_TIM_UPDATE AS INJ_DT_TM,
        SEV,
        CAUSE_INJ,
        HUM_FACT1,
        HUM_FACT2,
        HUM_FACT3,
        HUM_FACT4,
        HUM_FACT5,
        HUM_FACT6,
        HUM_FACT7,
        HUM_FACT8,
        FACT_INJ1,
        FACT_INJ2,
        FACT_INJ3,
        ACTIV_INJ,
        LOC_INC,
        GEN_LOC_IN,
        STORY_INC_UPDATE AS STORY_INC,
        STORY_INJ_UPDATE AS STORY_INJ,
        SPC_LOC_IN,
        PRIM_SYMP,
        BODY_PART,
        CC_DISPOS
    FROM civiliancasualty_col_data_type_update
)

SELECT * FROM civiliancasualty_final