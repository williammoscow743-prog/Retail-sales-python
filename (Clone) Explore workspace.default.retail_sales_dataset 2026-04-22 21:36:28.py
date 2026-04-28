# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `workspace`.`default`.`retail_sales_dataset`;

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

##Loading Dataset from the table using spark 
df = spark.table("retail_sales_dataset")
rs = df.toPandas()


# COMMAND ----------

# MAGIC %md
# MAGIC # Understanding Data

# COMMAND ----------

# Summary of our table/data

rs.info()

# COMMAND ----------

#Listing the table column 
rs.columns

# COMMAND ----------

#Descriptive Statistics of our table/data

rs.describe()


# COMMAND ----------

#Checking for null values in our table/data

rs.isnull().sum()



# COMMAND ----------

#Deleting rows that has missing values 
rs=rs.dropna()

# COMMAND ----------

#Replacing missing values with 0
rs=rs.fillna(0)

# COMMAND ----------

# Checking the data types of our table
rs.dtypes

# COMMAND ----------

#Checking for duplicate values in our table/data

rs.duplicated().sum()

# COMMAND ----------

# Shows the first 5 rows of our table
rs.head()

# COMMAND ----------

##Shows the number of rows and columns in our table
rs.shape

# COMMAND ----------

pd.DatetimeIndex(rs['Date'])

# COMMAND ----------

rs.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC
