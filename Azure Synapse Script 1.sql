CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''

-------------------
--CREATE CREDENTIAL
-------------------

CREATE DATABASE SCOPED CREDENTIAL rogercreds
WITH 
    IDENTITY = 'Managed Identity'


------------------------
--CREATE EXT DATA SOURCE
------------------------
CREATE EXTERNAL DATA SOURCE ds_bronze
WITH (
  LOCATION = 'abfss://bronze@retaildatagit.dfs.core.windows.net',
  CREDENTIAL = rogercreds
);

---------------------
--OPENROWSET FUNCTION
---------------------
SELECT TOP 10 *
FROM OPENROWSET(
    BULK '/retail_sales_dataset.csv',
    DATA_SOURCE = 'ds_bronze',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS rows;





