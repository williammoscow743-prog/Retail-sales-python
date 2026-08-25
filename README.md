# Retail Sales Analysis with Python

Early-stage exploratory work on a retail sales dataset (`retail_sales_dataset`), done in a Databricks notebook using PySpark and Pandas.

## Overview

This repository contains the data-understanding and cleaning stage of a retail sales analysis project. The dataset is loaded via Spark and converted to a Pandas DataFrame for inspection.

## What's in this repo

- `(Clone) Explore workspace.default.retail_sales_dataset ....py` — exported Databricks notebook covering:
  - Loading `retail_sales_dataset` via Spark and converting to Pandas
  - Inspecting structure (`.info()`, `.columns`, `.describe()`, `.dtypes`)
  - Checking and handling missing values (`.isnull().sum()`, `.dropna()`, `.fillna()`)
  - Checking for duplicate rows
  - Parsing the `Date` column to datetime

## Status

This is an in-progress data understanding and cleaning stage — visualizations and business insights haven't been added yet.

## Tools Used

- Databricks
- PySpark
- Pandas, NumPy
- Matplotlib, Seaborn (imported, not yet used)

## Getting Started

This notebook was exported from Databricks and expects a Spark environment with the `retail_sales_dataset` table registered. To run it outside Databricks, you'd need to adapt the data-loading step to read from a local file instead of `spark.table(...)`.

## Author

**William Mathekuana**

GitHub: https://github.com/williammoscow743-prog
