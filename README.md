# National Fire Incident Reporting System (NFIRS) Data Pipeline and Analysis
## 📋 Overview
The primary goal of the NFIRS data pipeline and analysis project is to implement efficient extract, load, transform (ELT) processes using an open-source modern data stack and to provide a comprehensive analysis on fire incidents throughout the U.S. in 2022. This project is designed to help professionals seeking to enhance their data ingestion and transformation workflows, ensuring complete and accurate data for analytical purposes.

The following tools/technologies are used for data ingestion, warehousing, transformation, visualization, and orchestration:
- Data ingestion
  - Python scripts ✒️
- Data warehouse
  - DuckDB 🦆
- Data transformation
  - dbt (data build tool) ♻️
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

**Location**: [python_scripts](https://github.com/jaflores10/nfirs-data-pipeline/tree/main/python_scripts)

### DuckDB Data Warehouse
The DuckDB data warehouse contains the below four schemas and relevant tables (tables in nfirs_transformed, nfirs_processed, and nfirs_analysis are created via dbt).

**Location**: [nfirs_database](https://github.com/jaflores10/nfirs-data-pipeline/tree/main/nfirs_database)

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
The dbt models are used to transform and clean the NFIRS data. Key transformations include changing data types and restructuring data to facilitate analysis and visualization.

**Location**: [nfirs_dbt](https://github.com/jaflores10/nfirs-data-pipeline/tree/main/nfirs_dbt)

**Key Files**
- `dbt_project.yml`: dbt project configuraion
- `schema.yml`: specifies data loaded into nfirs_raw by the python scripts
- `models/`: Contains dbt models for various stages of data transformation such as converting column data types to the correct data type as specified in the [NFIRS Fire Data Analysis Guidelines and Issues](https://www.usfa.fema.gov/downloads/pdf/nfirs/nfirs_data_analysis_guidelines_issues.pdf), joining tables together for further analysis, and selecting specific columns and filters to serve as data sources for Tableau.

### Tableau Data Visualization
The data is visualized using Tableau. You can view the interactive dashboards through the links below:
- [CA Wildfires](https://public.tableau.com/app/profile/javier.flores5792/viz/NFIRSDataAnalysis-CAWildfires/CAWildfiresSummary)

Please note, due to Tableau Public data limitations, dashboards for the code description analysis and the municipal incident analysis are unavailable at the moment.

### Analysis Summary
The analysis provides insights into fire incident patterns, what regions and states in the U.S. had the most incidents in 2022, and details where most wildfires occurred in California and the types of conditions which caused those wildfires. Further details below:

- U.S. Fire Incidents
  - In 2022, the most fire incidents occurred in the Southeast of the U.S. This finding is somewhat surprising given how populated California is; however, aside from California and the Pacific Northwest much of the western U.S. is not densely populated (large states but smaller populations).
- U.S. State and City Fire Incidents Summary
  - Most fire incidents occured in December. This can be associated with people using heating equipment more often and them malfunctioning, cooking inside more often with large gatherings, candles and fireplaces being used more frequently, and Christmas trees.
  - The 611 incident type code is most common. This represents when a fire unit was dispatched an cancelled en route (i.e., someone most likely inadvertently calling 911). 111 (building fire) is the next most frequent incident type. This makes sense given many fire incidents are occuring in December, the majority of which start indoors.
- California Wildfires
  - Unfortunately, the NFIRS data does not contain much quantitative nor qualitative data for wildfires. The majority of causes are marked as underdetermined and data for wildfire attributes such as acres burned, air temperatures, humidity, and fire danger rating were not provided to the NFIRS.
  - There was an uptick in wildfires in March. March typically experiences high levels of precipitation (although 2022 was a low year in preciptation overall), so this was a small outlier.

**Potential Further Analysis**
The analysis provides a high-level summary of fire incidents throughout the U.S. Further analysis may expand to more years, identify seasonal trends with different types of fire incidents, and implement machine learning techniques to model when/where a fire may occur and how to best respond.

### Dagster Data Orchestration
Dagster was integrated to orchestrate and manage the data pipelines for processing NFIRS data.

**Location**: [nfirs_dagster](https://github.com/jaflores10/nfirs-data-pipeline/tree/main/nfirs_dbt/nfirs_dagster/nfirs_dagster)

**Key Files**
- `definitions.py`: This file contains the core configuration for Dagster, defining the repository that includes assets, jobs, and schedules
- `assets.py`: This file is used to define data assets (python scripts, dbt models, etc.).

Below is a DAG demonstrating how the assets are connected and them successfully materializing:

## 🚀 Getting Started
### Prerequisites
Ensure the following accounts and tools are set up befor beginning this project:

#### Accounts
- **GitHub**: For version control and collaboration.

#### Install Tools
- **VS Code or other IDE**: Allows for easy code editing. [Download VS Code](https://code.visualstudio.com/download)
- **Python**: [Download Python](https://www.python.org/downloads/)
- **DuckDB**: [Download DuckDB](https://duckdb.org/docs/installation/?version=stable&environment=cli&platform=win&download_method=package_manager)
- **dbt Core**: [Install dbt Core](https://github.com/dbt-labs/dbt-core)
- **Tableau**: Users may use Tableau public for free. [Download Tableau Public](https://public.tableau.com/app/discover)
- **Dagster**: [Install Dagster](https://github.com/dagster-io/dagster)

### Project Starting Guide
To get started with this project, follow these steps:

1. **Clone the repository**: `git clone https://github.com/jaflores10/nfirs-data-pipeline/tree/main`
2. **Create virtual environment**: `python -m venv env` `source env/bin/activate` `# On Windows: `env\Scripts\activate`
3. **Set up the environment**: Install the required dependencies using `pip install -r requirements.txt`.
4. **Ingest the data**: Run the ingestion scripts to load data into DuckDB.
5. **Run dbt models**: Execute dbt models to transform the data.
6. **Visualize the data**: Access the Tableau dashboards to explore the visualizations and analyze data.

## Contributing
Contributions are welcome! Please submit a pull request or open an issue to discuss changes.

## License
This project is licensed under the MIT License.

## Credits
- **Data Source**: NFIRS
- **Image**: Courtesy of NFIRS
- **Tools and Technologies**: Python, DuckDB, dbt, Tableau, Dagster
