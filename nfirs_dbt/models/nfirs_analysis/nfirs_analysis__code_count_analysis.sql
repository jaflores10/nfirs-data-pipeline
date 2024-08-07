/***** NFIRS Code Type Count Table Creation *****/
/*** 
The below model will select specific columns from 'nfirs_processed__incidents_codelookup' to build visuals in Tableau
***/

{{ config(
    materialized='table',
    schema='analysis'
) }}

WITH processed_incidents_codelookup_source AS (
    SELECT * FROM {{ ref('nfirs_processed__incidents_codelookup') }}
),

inc_type_cte AS (
	SELECT
		state,
		city,
		fd_name,
		'inc_type' AS code_type,
		inc_type AS code_number,
		inc_type_code_descr AS code_description,
		count(inc_type) AS code_count
	FROM processed_incidents_codelookup_source
	GROUP BY ALL
	ORDER BY 7 DESC
),

act_tak1_cte AS (
	SELECT
		state,
		city,
		fd_name,
		'act_tak1' AS code_type,
		act_tak1 AS code,
		act_tak1_code_descr AS code_description,
		count(act_tak1) AS code_count
	FROM processed_incidents_codelookup_source
	GROUP BY ALL
	ORDER BY 7 DESC
),

act_tak2_cte AS (
	SELECT
		state,
		city,
		fd_name,
		'act_tak2' AS code_type,
		act_tak2 AS code,
		act_tak2_code_descr AS code_description,
		count(act_tak2) AS code_count
	FROM processed_incidents_codelookup_source
	GROUP BY ALL
	ORDER BY 7 DESC
),

act_tak3_cte AS (
	SELECT
		state,
		city,
		fd_name,
		'act_tak3' AS code_type,
		act_tak3 AS code,
		act_tak3_code_descr AS code_description,
		count(act_tak3) AS code_count
	FROM processed_incidents_codelookup_source
	GROUP BY ALL
	ORDER BY 7 DESC
),

union_cte_final AS (
    SELECT * FROM inc_type_cte
    UNION ALL
    SELECT * FROM act_tak1_cte
    UNION ALL
    SELECT * FROM act_tak2_cte
    UNION ALL
    SELECT * FROM act_tak3_cte
)

SELECT * FROM union_cte_final