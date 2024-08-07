/***** NFIRS Transformed Wildland Fire Table Table Creation *****/
/***
The below model will transform the folling columns to the appropriate data type:
    - INC_DATE --> DATE
    - WIND_SPEED --> FLOAT
    - AIR_TEMP --> FLOAT
    - REL_HUMID --> FLOAT
    - FUEL_MOIST --> FLOAT
    - BLDG_INV --> FLOAT
    - BLDG_THR --> FLOAT
    - ACRES_BURN --> FLOAT
    - UNDET_BURN --> FLOAT
    - TAX_BURN --> FLOAT
    - NOTAX_BURN --> FLOAT
    - LOCAL_BURN --> FLOAT
    - COUTY_BURN --> FLOAT
    - ST_BURN --> FLOAT
    - FED_BURN --> FLOAT
    - FOREI_BURN --> FLOAT
    - MILIT_BURN --> FLOAT
    - OTHER_BURN --> FLOAT
    - AGE --> INTEGER
    - HORIZ_DIS --> FLOAT
    - ELEVATION --> FLOAT
    - FLAME_LGTH --> FLOAT
    - SPREAD_RAT --> FLOAT
***/

{{ config(
    materialized='table',
    schema='transformed'
) }}

WITH wildlands_source AS (
    SELECT * FROM {{ source('nfirs_raw', 'wildlands') }}
),

wildlands_col_data_type_update AS (
    SELECT
        *,
        CAST(STRPTIME(INC_DATE, '%m%d%Y') AS DATE) AS INC_DATE_UPDATE,
        CAST(WIND_SPEED AS FLOAT) AS WIND_SPEED_UPDATE,
        CAST(AIR_TEMP AS FLOAT) AS AIR_TEMP_UPDATE,
        CAST(REL_HUMID AS FLOAT) AS REL_HUMID_UPDATE,
        CAST(FUEL_MOIST AS FLOAT) AS FUEL_MOIST_UPDATE,
        CAST(BLDG_INV AS FLOAT) AS BLDG_INV_UPDATE,
        CAST(BLDG_THR AS FLOAT) AS BLDG_THR_UPDATE,
        CAST(ACRES_BURN AS FLOAT) AS ACRES_BURN_UPDATE,
        CAST(UNDET_BURN AS FLOAT) AS UNDET_BURN_UPDATE,
        CAST(TAX_BURN AS FLOAT) AS TAX_BURN_UPDATE,
        CAST(NOTAX_BURN AS FLOAT) AS NOTAX_BURN_UPDATE,
        CAST(LOCAL_BURN AS FLOAT) AS LOCAL_BURN_UPDATE,
        CAST(COUTY_BURN AS FLOAT) AS COUTY_BURN_UPDATE,
        CAST(ST_BURN AS FLOAT) AS ST_BURN_UPDATE,
        CAST(FED_BURN AS FLOAT) AS FED_BURN_UPDATE,
        CAST(FOREI_BURN AS FLOAT) AS FOREI_BURN_UPDATE,
        CAST(MILIT_BURN AS FLOAT) AS MILIT_BURN_UPDATE,
        CAST(OTHER_BURN AS FLOAT) AS OTHER_BURN_UPDATE,
        CAST(AGE AS INT) AS AGE_UPDATE,
        CAST(HORIZ_DIS AS FLOAT) AS HORIZ_DIS_UPDATE,
        CAST(ELEVATION AS FLOAT) AS ELEVATION_UPDATE,
        CAST(FLAME_LGTH AS FLOAT) AS FLAME_LGTH_UPDATE,
        CAST(SPREAD_RAT AS FLOAT) AS SPREAD_RAT_UPDATE
    FROM wildlands_source
),

wildlands_final AS (
    SELECT
        INCIDENT_KEY,
        STATE,
        FDID,
        INC_DATE_UPDATE AS INC_DATE,
        INC_NO,
        EXP_NO,
        VERSION,
        LATITUDE,
        LONGITUDE,
        TOWNSHIP,
        NORTH_SOU,
        RANGE,
        EAST_WEST,
        SECTION,
        SUBSECTION,
        MERIDIAN,
        AREA_TYPE,
        FIRE_CAUSE,
        HUM_FACT1,
        HUM_FACT2,
        HUM_FACT3,
        HUM_FACT4,
        HUM_FACT5,
        HUM_FACT6,
        HUM_FACT7,
        HUM_FACT8,
        FACT_IGN1,
        FACT_IGN2,
        SUPP_FACT1,
        SUPP_FACT2,
        SUPP_FACT3,
        HEAT_SOURC,
        MOB_PROP,
        EQ_INV_IGN,
        NFDRS_ID,
        WEATH_TYPE,
        WIND_DIR,
        WIND_SPEED_UPDATE AS WIND_SPEED,
        AIR_TEMP_UPDATE AS AIR_TEMP,
        REL_HUMID_UPDATE AS REL_HUMID,
        FUEL_MOIST_UPDATE AS FUEL_MOIST,
        DANGR_RATE,
        BLDG_INV_UPDATE AS BLDG_INV,
        BLDG_THR_UPDATE AS BLDG_THR,
        ACRES_BURN_UPDATE AS ACRES_BURN,
        CROP_BURN1,
        CROP_BURN2,
        CROP_BURN3,
        UNDET_BURN_UPDATE AS UNDET_BURN,
        TAX_BURN_UPDATE AS TAX_BURN,
        NOTAX_BURN_UPDATE AS NOTAX_BURN,
        LOCAL_BURN_UPDATE AS LOCAL_BURN,
        COUTY_BURN_UPDATE AS COUTY_BURN,
        ST_BURN_UPDATE AS ST_BURN,
        FED_BURN_UPDATE AS FED_BURN,
        FOREI_BURN_UPDATE AS FOREI_BURN,
        MILIT_BURN_UPDATE AS MILIT_BURN,
        OTHER_BURN_UPDATE AS OTHER_BURN,
        PROP_MANAG,
        FED_CODE,
        NFDRS_FM,
        PERSON_FIR,
        GENDER,
        AGE_UPDATE AS AGE,
        ACTIVITY_W,
        HORIZ_DIS_UPDATE AS HORIZ_DIS,
        TYPE_ROW,
        ELEVATION_UPDATE AS ELEVATION,
        POS_SLOPE,
        ASPECT,
        FLAME_LGTH_UPDATE AS FLAME_LGTH,
        SPREAD_RAT_UPDATE AS SPREAD_RAT
    FROM wildlands_col_data_type_update
)

SELECT * FROM wildlands_final