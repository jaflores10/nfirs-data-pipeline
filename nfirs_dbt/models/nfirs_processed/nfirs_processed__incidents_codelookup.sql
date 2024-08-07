/***** NFIRS Processed Incidents with Code Descriptions Table Creation *****/
/***
The below model will join the following tables together for further analysis:
- nfirs_transformed__fdheader (from nfirs_processed__incidents)
- nfirs_transformed__basicincident (from nfirs_processed__incidents)
- nfirs_transformed__fireincident (from nfirs_processed__incidents)
- nfirs_transformed__codelookup with the below logic

The following logic will be applied:
- nfirs_processed__incidents serves as the base table
- Create CTEs for INC_TYPE, ACT_TAK1, ACT_TAK2, ACT_TAK3, SUP_FAC_1,
  SUP_FAC_2, SUP_FAC_3, DETECTOR, DET_TYPE, DET_OPERAT, DET_EFFECT,
  DET_FAIL, AES_PRES, AES_TYPE, AES_OPER, AES_FAIL

All columns will be taken from nfirs_processed__incidents and the code
descriptions for the above fields/codes.
***/

{{ config(
    materialized='table',
    schema='processed'
) }}

-- Create Incidents CTE
WITH incidents_source AS (
    SELECT * FROM {{ ref('nfirs_processed__incidents') }}
),

-- Create Code Lookup CTEs
inc_type_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'INC_TYPE'
),

act_tak1_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'ACT_TAK1'
),

act_tak2_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'ACT_TAK2'
),

act_tak3_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'ACT_TAK3'
),

sup_fac_1_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'SUP_FAC_1'
),

sup_fac_2_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'SUP_FAC_2'
),

sup_fac_3_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'SUP_FAC_3'
),

detector_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'DETECTOR'
),

det_type_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'DET_TYPE'
),

det_power_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'DET_POWER'
),

det_operat_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'DET_OPERAT'
),

det_effect_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'DET_EFFECT'
),

det_fail_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'DET_FAIL'
),

aes_pres_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'AES_PRES'
),

aes_type_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'AES_TYPE'
),

aes_oper_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'AES_OPER'
),

aes_fail_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'AES_FAIL'
),

incidents_codelookup_final AS (
    SELECT
        inc.*,
        inc_type.code_descr AS inc_type_code_descr,
		act_tak1.code_descr AS act_tak1_code_descr,
		act_tak2.code_descr AS act_tak2_code_descr,
		act_tak3.code_descr AS act_tak3_code_descr,
		sup_fac_1.code_descr AS sup_fac_1_code_descr,
		sup_fac_2.code_descr AS sup_fac_2_code_descr,
		sup_fac_3.code_descr AS sup_fac_4_code_descr,
		det.code_descr AS detector_code_descr,
		det_type.code_descr AS det_type_code_descr,
		det_power.code_descr AS det_power_code_descr,
		det_operat.code_descr AS det_operat_code_descr,
		det_fail.code_descr AS det_fail_code_descr,
		aes_pres.code_descr AS aes_pres_code_descr,
		aes_type.code_descr AS aes_type_code_descr,
		aes_oper.code_descr AS aes_oper_code_descr,
		aes_fail.code_descr AS aes_fail_code_descr
    FROM incidents_source inc
    LEFT JOIN inc_type_code_lookup_source inc_type
        ON inc.inc_type = inc_type.code_value
    LEFT JOIN act_tak1_code_lookup_source act_tak1
        ON inc.act_tak1 = act_tak1.code_value
    LEFT JOIN act_tak2_code_lookup_source act_tak2
        ON inc.act_tak2 = act_tak2.code_value
    LEFT JOIN act_tak3_code_lookup_source act_tak3
        ON inc.act_tak3 = act_tak3.code_value
    LEFT JOIN sup_fac_1_code_lookup_source sup_fac_1
        ON inc.sup_fac_1 = sup_fac_1.code_value
    LEFT JOIN sup_fac_2_code_lookup_source sup_fac_2
        ON inc.sup_fac_2 = sup_fac_2.code_value
    LEFT JOIN sup_fac_3_code_lookup_source sup_fac_3
        ON inc.sup_fac_3 = sup_fac_3.code_value
    LEFT JOIN detector_code_lookup_source det
        ON inc.detector = det.code_value
    LEFT JOIN det_type_code_lookup_source det_type
        ON inc.det_type = det_type.code_value
    LEFT JOIN det_power_code_lookup_source det_power
        ON inc.det_power = det_power.code_value
    LEFT JOIN det_operat_code_lookup_source det_operat
        ON inc.det_operat = det_operat.code_value
    LEFT JOIN det_fail_code_lookup_source det_fail
        ON inc.det_fail = det_fail.code_value
    LEFT JOIN aes_pres_code_lookup_source aes_pres
        ON inc.aes_pres = aes_pres.code_value
    LEFT JOIN aes_type_code_lookup_source aes_type
        ON inc.aes_type = aes_type.code_value
    LEFT JOIN aes_oper_code_lookup_source aes_oper
        ON inc.aes_oper = aes_oper.code_value
    LEFT JOIN aes_fail_code_lookup_source aes_fail
        ON inc.aes_fail = aes_fail.code_value
)

SELECT * FROM incidents_codelookup_final