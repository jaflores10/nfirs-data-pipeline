/***** NFIRS Processed Incidents and Wildlands Data Table Creation *****/
/***
The below model will join the following tables together for further analysis:
- nfirs_transformed__fdheader (from nfirs_processed__incidents)
- nfirs_transformed__basicincident (from nfirs_processed__incidents)
- nfirs_transformed__fireincident (from nfirs_processed__incidents)
- nfirs_transformed__wildlands

The following logic will be applied:
- nfirs_processed__incidents serves as the base table
- Create CTE for nfirs_transformed__wildlands and join it to the prior CTE

All columns from nfirs_processed__incidents and all columns excluding duplicative columns from
nfirs_transformed_wildlands will be taken.
***/

{{ config(
    materialized='table',
    schema='processed'
) }}

-- Create Incidents CTE
WITH incidents_source AS (
    SELECT * FROM {{ ref('nfirs_processed__incidents') }}
),

-- Create Wildlands CTE
wildlands_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__wildlands') }}
),

-- Code Lookup CTE
inc_type_code_lookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'INC_TYPE'
),

fire_cause_codelookup_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__codelookup') }}
    WHERE fieldid = 'FIRE_CAUSE'
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

-- Incidents and Wildlands JOIN
incidents_wildlands_join AS (
    SELECT
        inc.*,
        wld.* EXCLUDE(state, fdid, inc_date, inc_no, exp_no)
    FROM incidents_source inc
    LEFT JOIN wildlands_source wld
        ON inc.incident_key = wld.incident_key
),

-- Incidents, Wildlands, and Code Descriptions JOIN
incidents_wildlands_final AS (
    SELECT
        inc.*,
        inc_type.code_descr AS inc_type_code_descr,
        fc.code_descr AS fire_cause_code_descr,
		act_tak1.code_descr AS act_tak1_code_descr,
		act_tak2.code_descr AS act_tak2_code_descr,
		act_tak3.code_descr AS act_tak3_code_descr
    FROM incidents_wildlands_join inc
    LEFT JOIN inc_type_code_lookup_source inc_type
        ON inc.inc_type = inc_type.code_value
    LEFT JOIN fire_cause_codelookup_source fc
        ON inc.fire_cause = fc.code_value
    LEFT JOIN act_tak1_code_lookup_source act_tak1
        ON inc.act_tak1 = act_tak1.code_value
    LEFT JOIN act_tak2_code_lookup_source act_tak2
        ON inc.act_tak2 = act_tak2.code_value
    LEFT JOIN act_tak3_code_lookup_source act_tak3
        ON inc.act_tak3 = act_tak3.code_value
)

SELECT * FROM incidents_wildlands_final