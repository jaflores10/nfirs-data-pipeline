import os

from dagster import Definitions
from dagster_dbt import DbtCliResource
#from dagster_duckdb import DuckDBResource

from .assets import nfirs_dbt_project_new_dbt_assets, raw_arson, raw_arsonagencyreferal, raw_arsonjuvsub, raw_basicaid, raw_basicincident, raw_civiliancasualty, raw_codelookup, raw_ems, raw_fdheader, raw_ffcasualty, raw_ffequipfail, raw_fireincident, raw_hazchem, raw_hazmat, raw_hazmatequipinvolved, raw_hazmobprop, raw_incidentaddress, raw_structurefirecauses, raw_wildlands
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[raw_arson, 
            raw_arsonagencyreferal,
            raw_arsonjuvsub,
            raw_basicaid,
            raw_basicincident,
            raw_civiliancasualty,
            raw_codelookup,
            raw_ems,
            raw_fdheader,
            raw_ffcasualty,
            raw_ffequipfail,
            raw_fireincident,
            raw_hazchem,
            raw_hazmat,
            raw_hazmatequipinvolved,
            raw_hazmobprop,
            raw_incidentaddress,
            raw_structurefirecauses,
            raw_wildlands,
            nfirs_dbt_project_new_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)