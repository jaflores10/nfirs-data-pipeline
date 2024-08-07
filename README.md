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
Below is the relation diagram of the NFIRS data. (Image courtesy of NFIRS):
![Relation Diagram](https://github.com/jaflores10/nfirs-data-pipeline/NFIRS Relation Diagram.jpg)
