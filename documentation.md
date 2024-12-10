# Currency Exchange Pipeline Documentation

## Overview

This project involves building a data pipeline that fetches currency and exchange rate data, processes it into a Snowflake data warehouse, and prepares analytics-ready models using **dbt Cloud** and **Prefect**.

### Features:

1. Data ingestion from the [CurrencyBeacon API](https://currencybeacon.com/).
2. Transformation of data using dbt Cloud.
3. Deployment of analytics models in Snowflake for insights such as currency trends and average exchange rates.

---

## Repository Structure

### dbt Cloud Repository

- **Repository**: [GitHub - dbt Models](https://github.com/Mani-Biglari/currency)
- Contains all dbt models, configuration files (`dbt_project.yml`), and schema documentation.

### Prefect and Python Code Repository

- **Repository**: [GitHub - Prefect Pipeline](https://github.com/Mani-Biglari/currency_pipeline)
- **Structure**:
  - `fetch_data.py`: Script to fetch data from CurrencyBeacon API.
  - `load_data.py`: Script to stage fetched data in Snowflake.
  - `flow.py`: Defines the Prefect flow to orchestrate the pipeline.
  - `prefect_deployment.py`: Script to deploy Prefect workflows and schedules.

---

## Setup Instructions

### 1. Prerequisites

- **dbt Cloud** account.
- **Snowflake** Database.
- **Prefect** installed locally with Python environment.

---

### 2. Clone the Repositories

#### For dbt Cloud:

1. Connect to the GitHub repository:
   - Repository: `https://github.com/Mani-Biglari/currency`
   - Branch: `main`

#### For Prefect:

1. Clone the Prefect repository:
   ```bash
   git clone https://github.com/Mani-Biglari/currency_pipeline.git
   ```
2. Navigate to the project directory:
   ```bash
   cd currency_pipeline
   ```

---

### 3. Configure dbt Cloud

1. **Connect GitHub Repository**:

   - Repository: `https://github.com/Mani-Biglari/currency`
   - Branch: `main`

2. **Environment Configuration**:

   - Configure your Snowflake credentials in the **dbt Cloud Project Settings**.
   - Set the `database`, `schema`, and `role` appropriately.

3. **Create and Schedule Jobs**:

   - **Run Command**: Include transformations and tests:
     ```bash
     dbt run && dbt test
     ```
   - **Target Environment**: Choose the configured Snowflake target.
   - **Schedule**: Define your schedule (e.g., daily, hourly).

---

### 4. Set Up Prefect

#### Install Prefect:

```bash
pip install prefect
```

#### Deploy the Prefect Flow:

1. Create a deployment script `prefect_deployment.py`:
   ```python
   from prefect import flow

   # Source for the code to deploy (here, a GitHub repo)
   SOURCE_REPO="https://github.com/Mani-Biglari/currency_pipeline.git"

   if __name__ == "__main__":
       flow.from_source(
           source=SOURCE_REPO,
           entrypoint="flow.py:currency_pipeline_flow", # Specific flow to run
       ).deploy(
           name="Currency Pipeline Dev",
           work_pool_name="currency_pipeline_pool", # Work pool target
           cron="* * * * *", # Cron schedule (every minute)
       )
   ```
2. Run the script:
   ```bash
   python prefect_deployment.py
   ```

#### Start a Prefect Server and Worker:

1. Start the Prefect server:
   ```bash
   prefect server start
   ```

2. Start a worker for the work pool:
   ```bash
   prefect worker start --pool "currency_pipeline_pool"
   ```

---

## Assumptions and Decisions

1. **Repository Setup**:

   - dbt models are hosted in `https://github.com/Mani-Biglari/currency`.
   - Python and Prefect workflows are hosted in `https://github.com/Mani-Biglari/currency_pipeline`.

2. **Incremental Loading**:

   - Currency and exchange rate tables use incremental materialization to handle updates and new records efficiently.

3. **Testing**:

   - dbt Cloud validates data using `not_null`, `unique`, and other constraints.

---

## Challenges and Solutions

### 1. **Integration Between Prefect and dbt Cloud**

- Prefect triggers dbt Cloud jobs via API, enabling a seamless workflow from data ingestion to analytics.

### 2. **Schema and Repository Adjustments**

- Schema and repository configurations were aligned to avoid issues with dbt Cloud and Prefect workflows.

### 3. **Data Validation**

- Implemented dbt tests to ensure the integrity and reliability of the data.

---

## Summary of Approach

1. **Prefect Orchestration**:

   - Fetches and stages data in Snowflake.
   - Automatically triggers dbt Cloud jobs for transformation and testing.

2. **dbt Cloud Transformation**:

   - Processes raw data into analytics-ready tables.
   - Generates insights using models like `currencies`, `exchange_rates`, and `avg_exchange_rate_trends_vw`.

3. **Seamless Workflow**:

   - Prefect and dbt Cloud integration ensures a robust pipeline with minimal manual intervention.

