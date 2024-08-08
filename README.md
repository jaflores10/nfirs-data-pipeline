# National Fire Incident Reporting System (NFIRS) Data Pipeline and Analysis
## :clipboard: Overview
The primary goal of the NFIRS data pipeline and analysis project is to implement efficient extract, load, transform (ELT) processes using an open-source modern data stack and to provide a comprehensive analysis on fire incidents throughout the U.S. in 2022. This project is designed to help professionals seeking to enhance their data ingestion and transformation workflows, ensuring complete and accurate data for analytical purposes.

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

Inspriration for this project was drawn from my time living in the mountains of northern California. Wildfires are something most east coasters do not experience regularly and moving to California was eye-opening in terms of learning how at-risk the state is with wildfires, how frequently they occur, and how desctructive wildfires are for millions of people in the state each year.

## ⛓️ Relation Diagram
Below is the relation diagram of NFIRS data. The diagram allows an individual to understand how datasets link together. (Image courtesy of NFIRS):
![Relation Diagram](https://github.com/jaflores10/nfirs-data-pipeline/blob/main/nfirs_relation_diagram.JPG)

## 💠 Modules
The modules below are organized to maintain clear separation of ELT processes.

### Data Ingestion
Handles the extraction and loading of NFIRS data linked above.

[Python data extraction scripts](https://github.com/jaflores10/nfirs-data-pipeline/tree/main/python_scripts).

### DuckDB Data Warehouse Details
The DuckDB data warehouse contains the below four schemas and relevant tables (tables in nfirs_transformed, nfirs_processed, and nfirs_analysis are created via dbt):

#### Schema: nfirs_raw
nfirs_raw contains the raw data obtained from the NFIRS website for all 19 datasets (flat files, '.txt' format). Please note, all column data types for every table in nfirs_raw are VARCHAR.

Tables:
- nfirs_arson
- nfirs_basicincident
- nfirs_fdheader
- etc.

#### Schema: nfirs_transformed
nfirs_transformed contains all 19 tables with updated data types as specified in the [NFIRS Fire Data Analysis Guidelines and Issues](https://www.usfa.fema.gov/downloads/pdf/nfirs/nfirs_data_analysis_guidelines_issues.pdf). The naming format for this schema is 'nfirs_transformed__{raw table name}.'

Tables:
- nfirs_transformed__arson
- nfirs_transformed__basicincident
- nfirs_transformed__fdheader
- etc.

#### Schema: nfirs_processed
nfirs_processed contains four tables which maybe used to further analyze fire incidents, aid given during an incident, casualties during an incident, relevant code descriptions for incidents, and wildfire details. The naming format for this schema is 'nfirs_processed__incidents_.'

Tables:
- nfirs_processed__incidents
- nfirs_processed__incidents_aid_casualty
- nfirs_processed__incidents_codelookup
- nfirs_processed__incidents_wildlands

#### Schema: nfirs_analysis
nfirs_analysis contains three tables derived from the tables in nfirs_processed. These tables serve as the data sources for analysis and visualization in Tableau. The naming format for this schema is 'nfirs_analysis__.'

Tables:
- nfirs_analysis__code_count_analysis
- nfirs_analysis__muni_incidents_analysis
- nfirs_analysis__wildlands_ca_analysis

### dbt Data Transformations
The dbt models are used to transform and clean the NFIRS data. The key transformations include changing data types and restructuring data to facilitate analysis and visualization.

#### Key Models
- Transformed models convert columns from VARCHAR to the correct data type as specified in the [NFIRS Fire Data Analysis Guidelines and Issues](https://www.usfa.fema.gov/downloads/pdf/nfirs/nfirs_data_analysis_guidelines_issues.pdf) and after manual review of the data.
- Processed models join tables together to serve as the base data sets for analysis and visualization.
  - nfirs_processed__incidents: Joins fire department description data, basic fire incident data, and detailed fire incident data together
  - nfirs_processed__incidents_aid_casualty: Joins the above incident data with firefighter casualty data and civilian casualty data
  - nfirs_processed__incidents_codelookup: Joins the above incident data with fire incident code data to gather code descriptions for incident types, actions taken, and fire suppression used
  - nfirs_processed__incidents_wildlands: Joins the above incident data with wildland data to obtain wildfire related data
- Analysis models select specified columns and filter the data to analyze and visualize fire incidents
    - nfires_analysis__code_count_analysis: Selects incident types and code descriptions across states, cities, and fire departments to analyze the most common incident types
    - nfirs_analysis_muni_incidents_analysis: Selects incidents across date related (weekday, season, etc.) and address related (state, city, street, etc.) to analyze what locations have the most incidents
    - nfirs_analysis_wildlands_ca_analysis: Selects California related wildland incident data to analyze wildfire incidents in the state

### Tableau Data Visualization
The data is visualized using Tableau. You can view the interactive dashboards through the links below:
- [CA Wildfires](https://public.tableau.com/app/profile/javier.flores5792/viz/NFIRSDataAnalysis-CAWildfires/CAWildfiresSummary).

### Analysis Summary
The analysis provides insights into fire incident patterns, what regions and states in the U.S. had the most incidents in 2022, and detail where most wildfires occurred in California

### Dagster Data Orchestration

