/***** Specify State for Wildlands data *****/
/***
The below macro will take a state's abbreviation as an argument to simply call the macro and receive wildland data
***/

{% macro nfirs_analysis__wildlands_select_state (state_abbr) %}

    SELECT
        *
    FROM {{ ref('nfirs_processed__incidents_wildlands') }}
    WHERE state = '{{ state_abbr }}'

{% endmacro %}