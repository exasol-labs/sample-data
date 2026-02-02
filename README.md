# Exasol Sample Data
This repository contains data generators for sample data to get quickly started with Exasol. Those AI-generated Python scripts can be used by everyone to generate their own data and adapt as needed.

Currently the scripts generate two types of Parquet files:
* a set of online products of an imaginary webshop
* reviews to those products
  
See [definitions](#Datatypes-and-Mappings) for a detailed description of fields and datatypes.

## Sample Data on S3
Exasol also provides two generated sample Parquet files on S3 for direct import via IMPORT command:
* Online Products (1.000.000 rows, 28.6 MB): https://exasol-easy-data-access.s3.eu-central-1.amazonaws.com/sample-data/online_products.parquet
* Product Reviews (1.822.007 rows, 162 MB): https://exasol-easy-data-access.s3.eu-central-1.amazonaws.com/sample-data/product_reviews.parquet

## DDL and IMPORT
To quickly work with above datasets just run the following DDL and IMPORT commands:
```sql
CREATE OR REPLACE TABLE PRODUCTS (
    PRODUCT_ID        DECIMAL(18,0),
    PRODUCT_CATEGORY  VARCHAR(100),
    PRODUCT_NAME      VARCHAR(2000000),
    PRICE_USD         DOUBLE,
    INVENTORY_COUNT   DECIMAL(10,0),
    MARGIN            DOUBLE,
    DISTRIBUTE BY PRODUCT_ID
);

IMPORT INTO PRODUCTS
  FROM PARQUET AT 'https://exasol-easy-data-access.s3.eu-central-1.amazonaws.com/sample-data/'
  FILE 'online_products.parquet';
```
```sql
CREATE OR REPLACE TABLE PRODUCT_REVIEWS (
    REVIEW_ID          DECIMAL(18,0),
    PRODUCT_ID         DECIMAL(18,0),
    PRODUCT_NAME       VARCHAR(2000000),
    PRODUCT_CATEGORY   VARCHAR(100),
    RATING             DECIMAL(2,0),
    REVIEW_TEXT        VARCHAR(100000),
    REVIEWER_NAME      VARCHAR(200),
    REVIEWER_PERSONA   VARCHAR(100),
    REVIEWER_AGE       DECIMAL(3,0),
    REVIEWER_LOCATION  VARCHAR(200),
    REVIEW_DATE       VARCHAR(200),
    DISTRIBUTE BY PRODUCT_ID
);

IMPORT INTO PRODUCT_REVIEWS
  FROM PARQUET AT 'https://exasol-easy-data-access.s3.eu-central-1.amazonaws.com/sample-data/'
  FILE 'product_reviews.parquet';
```


## Datatypes and Mappings
Currently there are two sample Parquet files, see their Parquet types and Exasol mapping below:

Online Products:
|Attribute|Parquet datatype|Exasol datatype|
|---|---|---|
|product_id	|INT64	|DECIMAL(18,0)|
|product_category	|BYTE_ARRAY (STRING)|	VARCHAR(100)|
|product_name	|BYTE_ARRAY (STRING)	|VARCHAR(2000000)|
|price_usd	|DOUBLE	|DOUBLE|
|inventory_count	|INT32	|DECIMAL(10,0)|
|margin|	DOUBLE|	DOUBLE|

Product Reviews:
|Attribute	|Parquet datatype|	Exasol datatype|
|---|---|---|
|review_id	|INT64	|DECIMAL(18,0)|
|product_id|	INT64	|DECIMAL(18,0)|
|product_name|	BYTE_ARRAY (STRING)	|VARCHAR(2000000)|
|product_category	|BYTE_ARRAY (STRING)	|VARCHAR(100)|
|rating	|INT32	|DECIMAL(2,0)|
|review_text|	BYTE_ARRAY (STRING)|	VARCHAR(100000)|
|reviewer_name	|BYTE_ARRAY (STRING)	|VARCHAR(200)|
|reviewer_persona|	BYTE_ARRAY (STRING)|	VARCHAR(100)|
|reviewer_age|	INT32	|DECIMAL(3,0)|
|reviewer_location|	BYTE_ARRAY (STRING)|	VARCHAR(200)|
|review_date	|BYTE_ARRAY (STRING)	|VARCHAR(200)|
