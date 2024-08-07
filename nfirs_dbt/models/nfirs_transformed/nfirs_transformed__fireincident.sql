/***** NFIRS Transformed Fire Incident Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
    - NUM_UNIT --> FLOAT
    - BLDG_INVOL --> FLOAT
    - ACRES_BURN --> FLOAT
    - AGE --> INTEGER
    - BLDG_ABOVE --> FLOAT
    - BLDG_BELOW --> FLOAT
    - BLDG_LGTH --> FLOAT
    - BLDG_WIDTH --> FLOAT
    - TOT_SQ_FT --> FLOAT
    - ST_DAM_MIN --> FLOAT
    - ST_DAM_SIG --> FLOAT
    - ST_DAM_HVY --> FLOAT
    - ST_DAM_XTR --> FLOAT
    - NO_SPR_OP --> FLOAT
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH fireincident_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'fireincident') }}
),

fireincident_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE,
        CAST(NUM_UNIT AS FLOAT) AS NUM_UNIT_UPDATE,
        CAST(BLDG_INVOL AS FLOAT) AS BLDG_INVOL_UPDATE,
        CAST(ACRES_BURN AS FLOAT) AS ACRES_BURN_UPDATE,
        CAST(AGE AS INT) AS AGE_UPDATE,
        CAST(BLDG_ABOVE AS FLOAT) AS BLDG_ABOVE_UPDATE,
        CAST(BLDG_BELOW AS FLOAT) AS BLDG_BELOW_UPDATE,
        CAST(BLDG_LGTH AS FLOAT) AS BLDG_LGTH_UPDATE,
        CAST(BLDG_WIDTH AS FLOAT) AS BLDG_WIDTH_UPDATE,
        CAST(TOT_SQ_FT AS FLOAT) AS TOT_SQ_FT_UPDATE,
        CAST(ST_DAM_MIN AS FLOAT) AS ST_DAM_MIN_UPDATE,
        CAST(ST_DAM_SIG AS FLOAT) AS ST_DAM_SIG_UPDATE,
        CAST(ST_DAM_HVY AS FLOAT) AS ST_DAM_HVY_UPDATE,
        CAST(ST_DAM_XTR AS FLOAT) AS ST_DAM_XTR_UPDATE,
        CAST(NO_SPR_OP AS FLOAT) AS NO_SPR_OP_UPDATE
    FROM fireincident_source
),

fireincident_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        VERSION,
        NUM_UNIT_UPDATE AS NUM_UNIT,
        NOT_RES,
        BLDG_INVOL_UPDATE AS BLDG_INVOL,
        ACRES_BURN_UPDATE AS ACRES_BURN,
        LESS_1ACRE,
        ON_SITE_M1,
        MAT_STOR1,
        ON_SITE_M2,
        MAT_STOR2,
        ON_SITE_M3,
        MAT_STOR3,
        AREA_ORIG,
        HEAT_SOURC,
        FIRST_IGN,
        CONF_ORIG,
        TYPE_MAT,
        CAUSE_IGN,
        FACT_IGN_1,
        FACT_IGN_2,
        HUM_FAC_1,
        HUM_FAC_2,
        HUM_FAC_3,
        HUM_FAC_4,
        HUM_FAC_5,
        HUM_FAC_6,
        HUM_FAC_7,
        HUM_FAC_8,
        AGE_UPDATE AS AGE,
        SEX,
        EQUIP_INV,
        SUP_FAC_1,
        SUP_FAC_2,
        SUP_FAC_3,
        MOB_INVOL,
        MOB_TYPE,
        MOB_MAKE,
        MOB_MODEL,
        MOB_YEAR,
        MOB_LIC_PL,
        MOB_STATE,
        MOB_VIN_NO,
        EQ_BRAND,
        EQ_MODEL,
        EQ_SER_NO,
        EQ_YEAR,
        EQ_POWER,
        EQ_PORT,
        FIRE_SPRD,
        STRUC_TYPE,
        STRUC_STAT,
        BLDG_ABOVE_UPDATE AS BLDG_ABOVE,
        BLDG_BELOW_UPDATE AS BLDG_BELOW,
        BLDG_LGTH_UPDATE AS BLDG_LGTH,
        BLDG_WIDTH_UPDATE AS BLDG_WIDTH,
        TOT_SQ_FT_UPDATE AS TOT_SQ_FT,
        FIRE_ORIG,
        ST_DAM_MIN_UPDATE AS ST_DM_MIN_UPDATE,
        ST_DAM_SIG_UPDATE AS ST_DM_SIG,
        ST_DAM_HVY_UPDATE AS ST_DAM_HVY,
        ST_DAM_XTR_UPDATE AS ST_DAM_XTR,
        FLAME_SPRD,
        ITEM_SPRD,
        MAT_SPRD,
        DETECTOR,
        DET_TYPE,
        DET_POWER,
        DET_OPERAT,
        DET_EFFECT,
        DET_FAIL,
        AES_PRES,
        AES_TYPE,
        AES_OPER,
        NO_SPR_OP_UPDATE AS NO_SPR_OP,
        AES_FAIL
    FROM fireincident_col_data_type_update
)

SELECT * FROM fireincident_final