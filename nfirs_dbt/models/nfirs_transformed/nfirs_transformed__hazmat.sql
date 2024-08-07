/***** NFIRS Transformed Hazardous Material Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
    - REL_STORY --> FLOAT
    - POP_DENS --> FLOAT
    - AFFEC_MEAS --> FLOAT
    - EVAC_MEAS --> FLOAT
    - PEOP_EVAC --> FLOAT
    - BLDG_EVAC --> FLOAT
    - HAZ_DEATH --> FLOAT
    - HAZ_INJ --> FLOAT
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH hazmat_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'hazmat') }}
),

hazmat_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE,
        CAST(REL_STORY AS FLOAT) AS REL_STORY_UPDATE,
        CAST(POP_DENS AS FLOAT) AS POP_DENS_UPDATE,
        CAST(AFFEC_MEAS AS FLOAT) AS AFFEC_MEAS_UPDATE,
        CAST(EVAC_MEAS AS FLOAT) AS EVAC_MEAS_UPDATE,
        CAST(PEOP_EVAC AS FLOAT) AS PEOP_EVAC_UPDATE,
        CAST(BLDG_EVAC AS FLOAT) AS BLDG_EVAC_UPDATE,
        CAST(HAZ_DEATH AS FLOAT) AS HAZ_DEATH_UPDATE,
        CAST(HAZ_INJ AS FLOAT) AS HAZ_INJ_UPDATE
    FROM hazmat_source
),

hazmat_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        VERSION,
        REL_FROM,
        REL_STORY_UPDATE AS REL_STORY,
        POP_DENS_UPDATE AS POP_DENS,
        AFFEC_MEAS_UPDATE AS AFFEC_MEAS,
        AFFEC_UNIT,
        EVAC_MEAS_UPDATE AS EVAC_MEAS,
        EVAC_UNIT,
        PEOP_EVAC_UPDATE AS PEOP_EVAC,
        BLDG_EVAC_UPDATE AS BLDG_EVAC,
        HAZ_ACT1,
        HAZ_ACT2,
        HAZ_ACT3,
        OCCUR_FIRS,
        CAUSE_REL,
        FACT_REL1,
        FACT_REL2,
        FACT_REL3,
        MIT_FACT1,
        MIT_FACT2,
        MIT_FACT3,
        EQ_INV_REL,
        HAZ_DISPO,
        HAZ_DEATH_UPDATE AS HAZ_DEATH,
        HAZ_INJ_UPDATE AS HAZ_INJ
    FROM hazmat_col_data_type_update
)

SELECT * FROM hazmat_final