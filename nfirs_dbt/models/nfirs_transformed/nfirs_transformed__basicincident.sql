/***** NFIRS Transformed Basic Incident Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
    - ALARM --> DATETIME
    - ARRIVAL --> DATETIME
    - INC_CONT --> DATETIME
    - LU_CLEAR --> DATETIME
    - SUP_APP --> FLOAT
    - EMS_APP --> FLOAT
    - OTH_APP --> FLOAT
    - SUP_PER --> FLOAT
    - EMS_PER --> FLOAT
    - OTH_PER --> FLOAT
    - PROP_LOSS --> FLOAT
    - CONT_LOSS --> FLOAT
    - PROP_VAL --> FLOAT
    - CONT_VAL --> FLOAT
    - FF_DEATH --> FLOAT
    - OTH_DEATH --> FLOAT
    - FF_INJ --> FLOAT
    - OTH_INJ --> FLOAT
    
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH basicincident_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'basicincident') }}
),

basicincident_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE,
        STRPTIME(ALARM, '%m%d%Y%H%M') AS ALARM_UPDATE,
        STRPTIME(ARRIVAL, '%m%d%Y%H%M') AS ARRIVAL_UPDATE,
        STRPTIME(INC_CONT, '%m%d%Y%H%M') AS INC_CONT_UPDATE,
        STRPTIME(LU_CLEAR, '%m%d%Y%H%M') AS LU_CLEAR_UPDATE,
        CAST(SUP_APP AS FLOAT) AS SUP_APP_UPDATE,
        CAST(EMS_APP AS FLOAT) AS EMS_APP_UPDATE,
        CAST(OTH_APP AS FLOAT) AS OTH_APP_UPDATE,
        CAST(SUP_PER AS FLOAT) AS SUP_PER_UPDATE,
        CAST(EMS_PER AS FLOAT) AS EMS_PER_UPDATE,
        CAST(OTH_PER AS FLOAT) AS OTH_PER_UPDATE,
        CAST(PROP_LOSS AS FLOAT) AS PROP_LOSS_UPDATE,
        CAST(CONT_LOSS AS FLOAT) AS CONT_LOSS_UPDATE,
        CAST(PROP_VAL AS FLOAT) AS PROP_VAL_UPDATE,
        CAST(CONT_VAL AS FLOAT) AS CONT_VAL_UPDATE,
        CAST(FF_DEATH AS FLOAT) AS FF_DEATH_UPDATE,
        CAST(OTH_DEATH AS FLOAT) AS OTH_DEATH_UPDATE,
        CAST(FF_INJ AS FLOAT) AS FF_INJ_UPDATE,
        CAST(OTH_INJ AS FLOAT) AS OTH_INJ_UPDATE
    FROM basicincident_source
),

basicincident_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        VERSION,
        DEPT_STA,
        INC_TYPE,
        ADD_WILD,
        AID,
        ALARM_UPDATE AS ALARM,
        ARRIVAL_UPDATE AS ARRIVAL,
        INC_CONT_UPDATE AS INC_CONT,
        LU_CLEAR_UPDATE AS LU_CLEAR,
        SHIFT,
        ALARMS,
        DISTRICT,
        ACT_TAK1,
        ACT_TAK2,
        ACT_TAK3,
        APP_MOD,
        SUP_APP_UPDATE AS SUP_APP,
        EMS_APP_UPDATE AS EMS_APP,
        OTH_APP_UPDATE AS OTH_APP,
        SUP_PER_UPDATE AS SUP_PER,
        EMS_PER_UPDATE AS EMS_PER,
        OTH_PER_UPDATE AS OTH_PER,
        RESOU_AID,
        PROP_LOSS_UPDATE AS PROP_LOSS,
        CONT_LOSS_UPDATE AS CONT_LOSS,
        PROP_VAL_UPDATE AS PROP_VAL,
        CONT_VAL_UPDATE AS CONT_VAL,
        FF_DEATH_UPDATE AS FF_DEATH,
        OTH_DEATH_UPDATE AS OTH_DEATH,
        FF_INJ_UPDATE AS FF_INJ,
        OTH_INJ_UPDATE AS OTH_INJ,
        DET_ALERT,
        HAZ_REL,
        MIXED_USE,
        PROP_USE,
        CENSUS
    FROM basicincident_col_data_type_update
)

SELECT * FROM basicincident_final