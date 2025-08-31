------------------------------
--CREATE EXTERNAL FILE FORMATS
------------------------------
CREATE EXTERNAL FILE FORMAT Parquet_format
WITH
(
  FORMAT_TYPE = PARQUET
);

--------------------------------
--CREATE EXTERNAL TABLE dim_date
--------------------------------

CREATE EXTERNAL TABLE dim_date
WITH 
(
  LOCATION = '/gold/simple/dim_date/',
  DATA_SOURCE = ds_bronze,
  FILE_FORMAT = Parquet_format
)
AS 
SELECT DISTINCT
CONVERT(int, FORMAT(TRY_CONVERT(date, [Date], 23), 'yyyyMMdd')) AS date_id,
  TRY_CONVERT(date, [Date], 23)                                   AS [date],
  YEAR(TRY_CONVERT(date, [Date], 23))                              AS [year],
  DATEPART(QUARTER, TRY_CONVERT(date, [Date], 23))                 AS [quarter],
  MONTH(TRY_CONVERT(date, [Date], 23))                             AS [month],
  DATENAME(MONTH, TRY_CONVERT(date, [Date], 23))                   AS month_name,
  DAY(TRY_CONVERT(date, [Date], 23))                               AS [day],
  DATENAME(WEEKDAY, TRY_CONVERT(date, [Date], 23))                 AS [weekday],
  CASE WHEN DATENAME(WEEKDAY, TRY_CONVERT(date, [Date], 23)) IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS is_weekend
FROM OPENROWSET(
    BULK '/retail_sales_dataset.csv',
    DATA_SOURCE = 'ds_bronze',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS s
WHERE TRY_CONVERT(date, [Date], 23) IS NOT NULL;



------------------------------------
--CREATE EXTERNAL TABLE dim_customer
------------------------------------

CREATE EXTERNAL TABLE dbo.dim_customer
WITH (
  LOCATION    = '/gold/simple/dim_customer/',
  DATA_SOURCE = ds_bronze,
  FILE_FORMAT = Parquet_format
)
AS
SELECT DISTINCT
  CAST([Customer ID] AS varchar(40)) AS customer_id,
  CAST([Gender]      AS varchar(10)) AS gender,
  TRY_CAST([Age]     AS int)         AS age,
  CASE
    WHEN TRY_CAST([Age] AS int) IS NULL THEN 'Unknown'
    WHEN TRY_CAST([Age] AS int) <= 17 THEN '0-17'
    WHEN TRY_CAST([Age] AS int) <= 24 THEN '18-24'
    WHEN TRY_CAST([Age] AS int) <= 34 THEN '25-34'
    WHEN TRY_CAST([Age] AS int) <= 44 THEN '35-44'
    WHEN TRY_CAST([Age] AS int) <= 54 THEN '45-54'
    WHEN TRY_CAST([Age] AS int) <= 64 THEN '55-64'
    ELSE '65+'
  END AS age_band
FROM OPENROWSET(
    BULK '/retail_sales_dataset.csv',
    DATA_SOURCE = 'ds_bronze',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS s;

-----------------------------------
--CREATE EXTERNAL TABLE dim_product
-----------------------------------

CREATE EXTERNAL TABLE dbo.dim_product
WITH (
  LOCATION    = '/gold/simple/dim_product/',
  DATA_SOURCE = ds_bronze,
  FILE_FORMAT = Parquet_format
)
AS
SELECT DISTINCT
  CAST([Product Category] AS varchar(50))        AS product_category,
  TRY_CAST([Price per Unit] AS decimal(18,2))    AS price_per_unit
FROM OPENROWSET(
    BULK '/retail_sales_dataset.csv',
    DATA_SOURCE = 'ds_bronze',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS s;

----------------------------------
--CREATE EXTERNAL TABLE fact_sales
----------------------------------

CREATE EXTERNAL TABLE dbo.fact_sales
WITH (
  LOCATION    = '/gold/simple/fact_sales/',
  DATA_SOURCE = ds_bronze,
  FILE_FORMAT = Parquet_format
)
AS
SELECT
  TRY_CAST([Transaction ID] AS int)               AS transaction_id,
  CONVERT(int, FORMAT(TRY_CONVERT(date, [Date], 23), 'yyyyMMdd')) AS date_id,
  CAST([Customer ID] AS varchar(40))              AS customer_id,
  CAST([Product Category] AS varchar(50))         AS product_category,
  TRY_CAST([Price per Unit] AS decimal(18,2))     AS price_per_unit,
  TRY_CAST([Quantity] AS int)                     AS quantity,
  TRY_CAST([Total Amount] AS decimal(18,2))       AS total_amount
FROM OPENROWSET(
    BULK '/retail_sales_dataset.csv',
    DATA_SOURCE = 'ds_bronze',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS s
WHERE TRY_CONVERT(date, [Date], 23) IS NOT NULL;

SELECT TOP 5 * FROM dbo.dim_date;
SELECT TOP 5 * FROM dbo.dim_customer;
SELECT TOP 5 * FROM dbo.dim_product;
SELECT TOP 5 * FROM dbo.fact_sales;