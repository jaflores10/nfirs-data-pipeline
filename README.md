# National Fire Incident Reporting System (NFIRS) Data Pipeline and Analysis
## :clipboard: Overview
The primary goal of the NFIRS data pipeline and analysis project implements efficient extract, load, transform (ELT) processes using an open-source modern data stack to provide a comprehensive analysis on fire incidents throughout the U.S. in 2022. This project is designed to help professionals seeking to enhance their data ingestion and transformation workflows, ensuring complete and accurate data for analytical purposes.

The following tools/technologies are used for data ingestion, warehousing, transformation, visualization, and orchestration:
- Data ingestion
  - Python scripts ✒️
- Data warehouse
  - DuckDB 🦆
- Data transformation
  - dbt ♻️
- Data Visualization
  - Tableau 📊
- Data Orchestration
    - Dagster 🎻

NFIRS data may be obtained [here](https://www.fema.gov/about/openfema/data-sets/fema-usfa-nfirs-annual-data).

## ⛓️ Relation Diagram
Below is the relation diagram of NFIRS data. (Image courtesy of NFIRS):
![Relation Diagram](https://github.com/jaflores10/nfirs-data-pipeline/blob/main/nfirs_relation_diagram.JPG)

## 🏘️ Data Warehouse Details
The DuckDB data warehouse contains the below four schemas and relevant tables:

### Schema: nfirs_raw
nfirs_raw contains the raw data obtained from the NFIRS website for all 19 datasets (flat files, '.txt'). Please note, all column data types for every table in nfirs_raw are VARCHAR.

Tables:
- nfirs_arson
- nfirs_basicincident
- nfirs_fdheader
- etc.

### Schema: nfirs_transformed
nfirs_transformed contains all 19 tables with updated data types as specified in the [NFIRS Fire Data Analysis Guidelines and Issues](https://www.usfa.fema.gov/downloads/pdf/nfirs/nfirs_data_analysis_guidelines_issues.pdf). The naming format for this schema is 'nfirs_transformed__{raw table name}.'

Tables:
- nfirs_transformed__arson
- nfirs_transformed__basicincident
- nfirs_transformed__fdheader
- etc.

### Schema: nfirs_processed
nfirs_processed contains four tables which maybe used to further analyze fire incident at a high level, aid given during an incident, casualties during an incident, relevant code descriptions for incidents, and wildfire details. The naming format for this schema is 'nfirs_processed__incidents_.'
Tables:
- nfirs_processed__incidents
- nfirs_processed__incidents_aid_casualty
- nfirs_processed__incidents_codelookup
- nfirs_processed__incidents_wildlands

