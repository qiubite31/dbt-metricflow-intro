# dbt MetricFlow Introduction

Welcome to the dbt-metricflow-intro project! This repository serves as a hands-on starter project for modern data analytics using **dbt (Data Build Tool)**, **DuckDB**, and dbt's advanced Semantic Layer, **MetricFlow**.

## Overview

This project simulates a basic retail environment tracking customers and their orders to demonstrate how to build a robust semantic layer. It transforms raw simulated data into a clean, physical analytics layer, and then defines a programmatic semantic layer on top of it.

### Core Technologies
- **Backend Analytics Engine**: DuckDB (Local, highly-performant, embedded analytics database)
- **Data Modeling & Physical Layer**: dbt Core 1.9+
- **Metrics definition & Semantic Layer**: MetricFlow

## Detailed Documentation

For a comprehensive guide covering the core concepts, directory structure, physical models, semantic models, metrics, and details on all the automated python scripts in this project, please see the [**PROJECT_DOCS.md**](./PROJECT_DOCS.md).

## Quick Start Guide

1. **Environment Initialization**: Ensure you have installed the required Python packages (dbt-duckdb and duckdb).
2. **Generate Raw Data**: 
   Run python setup_data.py to automatically generate the raw datasets into the data.duckdb file.
3. **Build the dbt project**: 
   Execute dbt build (or python run_dbt_tasks.py) to build all the SQL transformations, staging models, detailed fact tables, and the crucial MetricFlow time spine.
4. **Query the Data**:
   - **Physical Layer**: To test direct SQL output from DuckDB, run python query_data.py.
   - **Semantic Layer (MetricFlow)**: To interact with MetricFlow via python, run python semantic_query.py. This script dynamically asks MetricFlow for dimensions and metrics, compiles aggregated SQL models directly from semantic definitions, and fetches the results from DuckDB.

## Additional Resources

- [dbt Developer Hub](https://docs.getdbt.com/docs/introduction)
- [About MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow)
- [DuckDB Documentation](https://duckdb.org/docs/)
