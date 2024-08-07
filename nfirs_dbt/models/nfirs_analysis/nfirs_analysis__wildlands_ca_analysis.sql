/***** NFIRS CA Wildlands Analysis Table Creation *****/
/*** 
The below model will select specific columns from 'nfirs_processed__incidents_wilds' for California to build visuals in Tableau
***/

-- depends_on: {{ ref('nfirs_processed__incidents_wildlands') }}

{{ config(
    materialized='table',
    schema='analysis'
) }}

{{ nfirs_analysis__wildlands_select_state('CA') }}
 