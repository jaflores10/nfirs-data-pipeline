/***** NFIRS Processed Incidents, Basic Aid, and Casualties Data Table Creation *****/
/***
The below model will join the following tables together for further analysis:
- nfirs_transformed__fdheader (from nfirs_processed__incidents)
- nfirs_transformed__basicincident (from nfirs_processed__incidents)
- nfirs_transformed__fireincident (from nfirs_processed__incidents)
- nfirs_transformed__basicaid
- nfirs_transformed__civiliancasualty
- nfirs_transformed__ffcasualty

The following logic will be applied:
- nfirs_processed__incidents serves as the base table
- Create CTE for nfirs_transformed_basicaid and join the data with nfirs_processed__incidents
- Create CTEs for nfirs_transformed__civiliancasualty and nfirs_transformed__ffcasualty and
  join them to the prior CTE 
- Columns from nfirs_transformed__civiliancasualty and nfirs_transformed__ffcasualty will end in '_cvl' and '_ff' respectively as they have column headers
  with the same name

All columns will be taken from nfirs_processed__incidents and all columns from the three other
tables.

There will be duplication in data because there is a one to many relationship between the incidents
data and civilian casualty/ff casualty data.
***/

{{ config(
    materialized='table',
    schema='processed'
) }}

-- Create Incidents CTE
WITH incidents_source AS (
    SELECT * FROM {{ ref('nfirs_processed__incidents') }}
),

-- Create Basic Aid CTE
basicaid_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__basicaid') }}
),

-- Create Incidents and Basic Aid Join CTE
incidents_basicaid_source AS (
    SELECT
        inc.*,
        b.fdidrecaid,
        b.fdidstrec,
        b.inc_nofdid
    FROM incidents_source inc
    LEFT JOIN basicaid_source b
        ON inc.incident_key = b.incident_key AND inc.inc_no = b.inc_no AND inc.exp_no = b.exp_no
),

-- Create Civilian Casualty CTE
civiliancasualty_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__civiliancasualty') }}
),

-- Create FF Casualty CTE
ffcasualty_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__ffcasualty') }}
),

-- Create final CTE
incidents_basicaid_casualty_final AS (
    SELECT
        inc.*,
        cvl.gender AS gender_cvl,
        cvl.age AS age_cvl,
        cvl.race AS race_cvl,
        cvl.ethnicity AS ethnicity_cvl,
        cvl.affiliat AS affiliat_cvl,
        cvl.inj_dt_tm AS inj_dt_tm_cvl,
        cvl.sev AS sev_cvl,
        cvl.cause_inj AS cause_inj_cvl,
        cvl.hum_fact1 AS hum_fact1_cvl,
        cvl.hum_fact2 AS hum_fact2_cvl,
        cvl.hum_fact3 AS hum_fact3_cvl,
        cvl.hum_fact4 AS hum_fact4_cvl,
        cvl.hum_fact5 AS hum_fact5_cvl,
        cvl.hum_fact6 AS hum_fact6_cvl,
        cvl.hum_fact7 AS hum_fact7_cvl,
        cvl.hum_fact8 AS hum_fact8_cvl,
        cvl.fact_inj1 AS fact_inj1_cvl,
        cvl.fact_inj2 AS fact_inj2_cvl,
        cvl.fact_inj3 AS fact_inj3_cvl,
        cvl.activ_inj AS activ_inj_cvl,
        cvl.loc_inc AS loc_inc_cvl,
        cvl.gen_loc_in AS gen_loc_in_cvl,
        cvl.story_inc AS story_inc_cvl,
        cvl.story_inj AS story_inj_cvl,
        cvl.spc_loc_in AS spc_loc_in_cvl,
        cvl.prim_symp AS prim_symp_cvl,
        cvl.body_part AS body_part_cvl,
        cvl.cc_dispos AS cc_dispos_cvl,
        ff.gender AS gender_ff,
        ff.career AS career_ff,
        ff.age AS age_ff,
        ff.inj_date AS inj_date_ff,
        ff.responses AS responses_ff,
        ff.assignment AS assignment_ff,
        ff.phys_cond AS phys_cond_ff,
        ff.severity AS severity_ff,
        ff.taken_to AS taken_to_ff,
        ff.activity AS activity_ff,
        ff.symptom AS symptom_ff,
        ff.pabi AS pabi_ff,
        ff.cause AS cause_ff,
        ff.factor AS factor_ff,
        ff.object AS object_ff,
        ff.wio AS wio_ff,
        ff.relation AS relation_ff,
        ff.story AS story_ff,
        ff.location AS location_ff,
        ff.vehicle AS vehicle_ff,
        ff.prot_eqp AS prot_eqp
    FROM incidents_basicaid_source inc
    LEFT JOIN civiliancasualty_source cvl
        ON inc.incident_key = cvl.incident_key AND inc.inc_date = cvl.inc_date AND inc.inc_no = cvl.inc_no AND inc.exp_no = cvl.exp_no
    LEFT JOIN ffcasualty_source ff
        ON inc.incident_key = ff.incident_key AND inc.inc_date = ff.inc_date AND inc.inc_no = ff.inc_no AND inc.exp_no = ff.exp_no

)

SELECT * FROM incidents_basicaid_casualty_final