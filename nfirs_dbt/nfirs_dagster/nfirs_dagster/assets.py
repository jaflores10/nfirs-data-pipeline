import os

import duckdb
import pandas as pd
from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_duckdb import DuckDBResource

from .constants import dbt_manifest_path

duckdb_database_path = "C:\\Users\\javier.flores\\Desktop\\Portfolio\\NFIRS Data Pipeline and Analysis\\nfirs_database\\nfirs.duckdb"

# Base directory
base_dir = "C:\\Users\\javier.flores\\Desktop\\Portfolio\\NFIRS Data Pipeline and Analysis\\nfirs_data"
# File names
arson = "arson.txt"
arsonagencyreferal = "arsonagencyreferal.txt"
arsonjuvsub = "arsonjuvsub.txt"
basicaid = "basicaid.txt"
basicincident = "basicincident.txt"
civiliancasualty = "civiliancasualty.txt"
codelookup = "codelookup.txt"
ems = "ems.txt"
fdheader = "fdheader.txt"
ffcasualty = "ffcasualty.txt"
ffequipfail = "ffequipfail.txt"
fireincident = "fireincident.txt"
hazchem = "hazchem.txt"
hazmat = "hazmat.txt"
hazmatequipinvolved = "hazmatequipinvolved.txt"
hazmobprop = "hazmobprop.txt"
incidentaddress = "incidentaddress.txt"
structurefirecauses = "structurefirecauses.txt"
wildlands = "wildlands.txt"

# Dagster raw data assets
@asset(compute_kind="python")
def raw_arson(context: AssetExecutionContext) -> None:
    arson_data = pd.read_csv(os.path.join(base_dir, arson), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.arson AS SELECT * FROM arson_data")

    # Log some metadata about the table we just wrote. It will show up in the UI.
    context.add_output_metadata({"num_rows": arson_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_arsonagencyreferal(context: AssetExecutionContext) -> None:
    arsonagencyreferal_data = pd.read_csv(os.path.join(base_dir, arsonagencyreferal), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.arsonagencyreferal AS SELECT * FROM arsonagencyreferal_data")

    context.add_output_metadata({"num_rows": arsonagencyreferal_data.shape[0]})

@asset(compute_kind="python")
def raw_arsonjuvsub(context: AssetExecutionContext) -> None:
    arsonjuvsub_data = pd.read_csv(os.path.join(base_dir, arsonjuvsub), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.arsonjuvsub AS SELECT * FROM arsonjuvsub_data")

    context.add_output_metadata({"num_rows": arsonjuvsub_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_basicaid(context: AssetExecutionContext) -> None:
    basicaid_data = pd.read_csv(os.path.join(base_dir, basicaid), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.basicaid AS SELECT * FROM basicaid_data")

    context.add_output_metadata({"num_rows": basicaid_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_basicincident(context: AssetExecutionContext) -> None:
    basicincident_data = pd.read_csv(os.path.join(base_dir, basicincident), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.basicincident AS SELECT * FROM basicincident_data")

    context.add_output_metadata({"num_rows": basicincident_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_civiliancasualty(context: AssetExecutionContext) -> None:
    civiliancasualty_data = pd.read_csv(os.path.join(base_dir, civiliancasualty), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.civiliancasualty AS SELECT * FROM civiliancasualty_data")

    context.add_output_metadata({"num_rows": civiliancasualty_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_codelookup(context: AssetExecutionContext) -> None:
    codelookup_data = pd.read_csv(os.path.join(base_dir, codelookup), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.codelookup AS SELECT * FROM codelookup_data")

    context.add_output_metadata({"num_rows": codelookup_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_ems(context: AssetExecutionContext) -> None:
    ems_data = pd.read_csv(os.path.join(base_dir, ems), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.ems AS SELECT * FROM ems_data")

    context.add_output_metadata({"num_rows": ems_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_fdheader(context: AssetExecutionContext) -> None:
    fdheader_data = pd.read_csv(os.path.join(base_dir, fdheader), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.fdheader AS SELECT * FROM fdheader_data")

    context.add_output_metadata({"num_rows": fdheader_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_ffcasualty(context: AssetExecutionContext) -> None:
    ffcasualty_data = pd.read_csv(os.path.join(base_dir, ffcasualty), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.ffcasualty AS SELECT * FROM ffcasualty_data")

    context.add_output_metadata({"num_rows": ffcasualty_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_ffequipfail(context: AssetExecutionContext) -> None:
    ffequipfail_data = pd.read_csv(os.path.join(base_dir, ffequipfail), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.ffequipfail AS SELECT * FROM ffequipfail_data")

    context.add_output_metadata({"num_rows": ffequipfail_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_fireincident(context: AssetExecutionContext) -> None:
    fireincident_data = pd.read_csv(os.path.join(base_dir, fireincident), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.fireincident AS SELECT * FROM fireincident_data")

    context.add_output_metadata({"num_rows": fireincident_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_hazchem(context: AssetExecutionContext) -> None:
    hazchem_data = pd.read_csv(os.path.join(base_dir, hazchem), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.hazchem AS SELECT * FROM hazchem_data")

    context.add_output_metadata({"num_rows": hazchem_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_hazmat(context: AssetExecutionContext) -> None:
    hazmat_data = pd.read_csv(os.path.join(base_dir, hazmat), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.hazmat AS SELECT * FROM hazmat_data")

    context.add_output_metadata({"num_rows": hazmat_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_hazmatequipinvolved(context: AssetExecutionContext) -> None:
    hazmatequipinvolved_data = pd.read_csv(os.path.join(base_dir, hazmatequipinvolved), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.hazmatequipinvolved AS SELECT * FROM hazmatequipinvolved_data")

    context.add_output_metadata({"num_rows": hazmatequipinvolved_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_hazmobprop(context: AssetExecutionContext) -> None:
    hazmobprop_data = pd.read_csv(os.path.join(base_dir, hazmobprop), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.hazmobprop AS SELECT * FROM hazmobprop_data")

    context.add_output_metadata({"num_rows": hazmobprop_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_incidentaddress(context: AssetExecutionContext) -> None:
    incidentaddress_data = pd.read_csv(os.path.join(base_dir, incidentaddress), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.incidentaddress AS SELECT * FROM incidentaddress_data")

    context.add_output_metadata({"num_rows": incidentaddress_data.shape[0]})
    connection.close()

@asset(compute_kind="python")
def raw_structurefirecauses(context: AssetExecutionContext) -> None:
    structurefirecauses_data = pd.read_csv(os.path.join(base_dir, structurefirecauses), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.structurefirecauses AS SELECT * FROM structurefirecauses_data")

@asset(compute_kind="python")
def raw_wildlands(context: AssetExecutionContext) -> None:
    wildlands_data = pd.read_csv(os.path.join(base_dir, wildlands), delimiter='^', encoding='iso-8859-1', dtype=str)
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")
    connection.execute("CREATE OR REPLACE TABLE nfirs_raw.wildlands AS SELECT * FROM wildlands_data")

    context.add_output_metadata({"num_rows": wildlands_data.shape[0]})
    connection.close()

@dbt_assets(manifest=dbt_manifest_path)
def nfirs_dbt_project_new_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()